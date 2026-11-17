/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: statistical profiling
 *
 * One streaming pass computes, per column, counts, NULLs, min/max,
 * mean and standard deviation (Welford), string-length statistics, and
 * - while they fit the memory budget - the exact distinct count, the
 * exact quantiles and the exact value frequencies. A structure that
 * outgrows the budget is dropped and replaced by its approximate
 * counterpart: HyperLogLog (p=14, standard error 0.81%) for the
 * distinct count, a 4096-bin histogram over [min, max] (second pass,
 * error at most one bin width) for the quantiles, Misra-Gries (second
 * pass, undercount at most N/(k+1)) for the frequencies. Every result
 * names its algorithm and error bound, so exact and approximate answers
 * are always distinguishable; mode=exact fails with TA_DATA_E2BIG
 * rather than approximate, mode=approx never tries to be exact.
 */

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "ta_data_int.h"

#define HIST_BINS 4096

typedef struct freq_ent {
    uint64_t    h;
    uint64_t    count;
    char       *text;
} freq_ent;

typedef struct pcol {
    size_t          idx;
    int             kind;
    uint64_t        count;
    uint64_t        nulls;
    uint64_t        bad;
    uint64_t        nan;
    bool            have_minmax;
    ta_data_cell    minc;
    ta_data_cell    maxc;
    /* numeric */
    bool            numeric;
    uint64_t        nnum;
    double          mean;
    double          m2;
    double          dmin;
    double          dmax;
    /* sum: exact for integers/decimals, compensated for floats */
    bool            isum_ok;
    int64_t         isum;
    int             iscale;
    double          ksum;
    double          kcomp;
    /* exact quantiles */
    double         *vals;
    size_t          nvals;
    size_t          capvals;
    bool            vals_over;
    uint64_t       *hist;
    /* distinct */
    td_hll         *hll;
    uint64_t       *set;
    size_t          setn;
    size_t          setcap;
    bool            set_over;
    /* frequencies */
    freq_ent       *freq;
    size_t          freqn;
    size_t          freqcap;
    bool            freq_over;
    freq_ent       *mg;
    size_t          mgn;
    /* lengths */
    bool            lens;
    uint64_t        len_min;
    uint64_t        len_max;
    double          len_sum;
} pcol;

typedef struct prof {
    td_mem          mem;
    int             mode;       /* 0 auto, 1 exact, 2 approx */
    size_t          topk;
    size_t          mgk;
    double         *qs;
    size_t          nq;
    pcol           *cols;
    size_t          ncols;
} prof;

static uint64_t
cell_hash(const ta_data_cell *c, td_buf *tmp)
{
    td_buf_reset(tmp);
    td_cell_ser(tmp, c, false);
    return td_hash64(tmp->ptr, tmp->len, 0xd15c);
}

/* ---- exact distinct: an open-addressing set of 64-bit hashes ------- */

static bool
set_add(prof *p, pcol *c, uint64_t h)
{
    size_t i;

    if (h == 0)
        h = 1;
    if (c->setn * 2 >= c->setcap)
    {
        size_t ncap = c->setcap == 0 ? 1024 : c->setcap * 2;
        uint64_t *ns;

        if (!td_mem_take(&p->mem, ncap * sizeof(uint64_t)))
            return false;
        ns = calloc(ncap, sizeof(uint64_t));
        if (ns == NULL)
        {
            td_mem_give(&p->mem, ncap * sizeof(uint64_t));
            return false;
        }
        for (i = 0; i < c->setcap; i++)
        {
            if (c->set[i] != 0)
            {
                size_t j = (size_t)c->set[i] & (ncap - 1);

                while (ns[j] != 0)
                    j = (j + 1) & (ncap - 1);
                ns[j] = c->set[i];
            }
        }
        free(c->set);
        td_mem_give(&p->mem, c->setcap * sizeof(uint64_t));
        c->set = ns;
        c->setcap = ncap;
    }
    i = (size_t)h & (c->setcap - 1);
    while (c->set[i] != 0)
    {
        if (c->set[i] == h)
            return true;
        i = (i + 1) & (c->setcap - 1);
    }
    c->set[i] = h;
    c->setn++;
    return true;
}

static void
set_drop(prof *p, pcol *c)
{
    free(c->set);
    td_mem_give(&p->mem, c->setcap * sizeof(uint64_t));
    c->set = NULL;
    c->setcap = c->setn = 0;
    c->set_over = true;
}

/* ---- exact frequencies -------------------------------------------- */

static void
freq_drop(prof *p, pcol *c)
{
    size_t i;

    for (i = 0; i < c->freqcap; i++)
    {
        if (c->freq[i].text != NULL)
        {
            td_mem_give(&p->mem, strlen(c->freq[i].text) + 1);
            free(c->freq[i].text);
        }
    }
    free(c->freq);
    td_mem_give(&p->mem, c->freqcap * sizeof(freq_ent));
    c->freq = NULL;
    c->freqcap = c->freqn = 0;
    c->freq_over = true;
}

static bool
freq_add(prof *p, pcol *c, uint64_t h, const char *text, size_t len)
{
    size_t i;

    if (h == 0)
        h = 1;
    if (c->freqn * 2 >= c->freqcap)
    {
        size_t ncap = c->freqcap == 0 ? 256 : c->freqcap * 2;
        freq_ent *nf;

        if (!td_mem_take(&p->mem, ncap * sizeof(freq_ent)))
            return false;
        nf = calloc(ncap, sizeof(freq_ent));
        if (nf == NULL)
        {
            td_mem_give(&p->mem, ncap * sizeof(freq_ent));
            return false;
        }
        for (i = 0; i < c->freqcap; i++)
        {
            if (c->freq[i].h != 0)
            {
                size_t j = (size_t)c->freq[i].h & (ncap - 1);

                while (nf[j].h != 0)
                    j = (j + 1) & (ncap - 1);
                nf[j] = c->freq[i];
            }
        }
        free(c->freq);
        td_mem_give(&p->mem, c->freqcap * sizeof(freq_ent));
        c->freq = nf;
        c->freqcap = ncap;
    }
    i = (size_t)h & (c->freqcap - 1);
    while (c->freq[i].h != 0)
    {
        if (c->freq[i].h == h)
        {
            c->freq[i].count++;
            return true;
        }
        i = (i + 1) & (c->freqcap - 1);
    }
    if (!td_mem_take(&p->mem, len + 1))
        return false;
    c->freq[i].h = h;
    c->freq[i].count = 1;
    c->freq[i].text = td_strndup(text, len);
    c->freqn++;
    return true;
}

/* Misra-Gries summary with k counters (second pass). */
static void
mg_add(prof *p, pcol *c, uint64_t h, const char *text, size_t len)
{
    size_t i;
    size_t w;

    for (i = 0; i < c->mgn; i++)
    {
        if (c->mg[i].h == h)
        {
            c->mg[i].count++;
            return;
        }
    }
    if (c->mgn < p->mgk)
    {
        c->mg[c->mgn].h = h;
        c->mg[c->mgn].count = 1;
        c->mg[c->mgn].text = td_strndup(text, len);
        c->mgn++;
        return;
    }
    for (i = 0, w = 0; i < c->mgn; i++)
    {
        if (--c->mg[i].count == 0)
        {
            free(c->mg[i].text);
            continue;
        }
        c->mg[w++] = c->mg[i];
    }
    c->mgn = w;
}

/* ---- the passes --------------------------------------------------- */

static size_t
utf8_len(const char *s, size_t len)
{
    size_t n = 0;
    size_t i;

    for (i = 0; i < len; i++)
    {
        if (((unsigned char)s[i] & 0xC0) != 0x80)
            n++;
    }
    return n;
}

static bool
kind_has_quantiles(int k)
{
    return td_kind_numeric(k) || k == TA_DATA_T_DATE ||
           k == TA_DATA_T_TIMESTAMP;
}

static void
pass1_cell(prof *p, pcol *c, const ta_data_cell *v, td_buf *tmp)
{
    double d;
    uint64_t h;

    if (v->null)
    {
        if (v->bad)
            c->bad++;
        else
            c->nulls++;
        return;
    }
    c->count++;
    if (c->kind != TA_DATA_T_ARRAY && c->kind != TA_DATA_T_OBJECT)
    {
        if (!c->have_minmax)
        {
            td_cell_copy(&c->minc, v);
            td_cell_copy(&c->maxc, v);
            c->have_minmax = true;
        }
        else if (!(v->type == TA_DATA_T_FLOAT && isnan(v->d)))
        {
            if (td_cell_order(v, &c->minc) < 0)
                td_cell_copy(&c->minc, v);
            if (td_cell_order(v, &c->maxc) > 0)
                td_cell_copy(&c->maxc, v);
        }
    }
    if (c->numeric && td_cell_num(v, &d))
    {
        if (isnan(d))
            c->nan++;
        else
        {
            double delta;

            c->nnum++;
            if (c->isum_ok && (v->type == TA_DATA_T_INT ||
                               v->type == TA_DATA_T_DECIMAL ||
                               v->type == TA_DATA_T_BOOL))
            {
                if (__builtin_add_overflow(c->isum, v->i, &c->isum))
                    c->isum_ok = false;
                c->iscale = v->scale;
            }
            else
                c->isum_ok = false;
            {
                /* Neumaier's compensated summation. */
                double t = c->ksum + d;

                if (fabs(c->ksum) >= fabs(d))
                    c->kcomp += (c->ksum - t) + d;
                else
                    c->kcomp += (d - t) + c->ksum;
                c->ksum = t;
            }
            delta = d - c->mean;
            c->mean += delta / (double)c->nnum;
            c->m2 += delta * (d - c->mean);
            if (c->nnum == 1 || d < c->dmin)
                c->dmin = d;
            if (c->nnum == 1 || d > c->dmax)
                c->dmax = d;
            if (kind_has_quantiles(c->kind) && !c->vals_over &&
                p->mode != 2 && p->nq > 0)
            {
                if (c->nvals == c->capvals)
                {
                    size_t ncap = c->capvals == 0 ? 4096 : c->capvals * 2;
                    double *nv;

                    if (!td_mem_take(&p->mem, (ncap - c->capvals) *
                                     sizeof(double)) ||
                        (nv = realloc(c->vals, ncap * sizeof(double))) ==
                        NULL)
                    {
                        free(c->vals);
                        td_mem_give(&p->mem, c->capvals * sizeof(double));
                        c->vals = NULL;
                        c->nvals = c->capvals = 0;
                        c->vals_over = true;
                    }
                    else
                    {
                        c->vals = nv;
                        c->capvals = ncap;
                    }
                }
                if (!c->vals_over)
                    c->vals[c->nvals++] = d;
            }
        }
    }
    if (c->lens && v->s != NULL)
    {
        uint64_t l = v->type == TA_DATA_T_STRING ? utf8_len(v->s, v->len) :
                     v->len;

        if (c->count == 1 || l < c->len_min)
            c->len_min = l;
        if (l > c->len_max)
            c->len_max = l;
        c->len_sum += (double)l;
    }
    h = cell_hash(v, tmp);
    td_hll_add(c->hll, h);
    if (p->mode != 2 && !c->set_over && !set_add(p, c, h))
        set_drop(p, c);
    if (p->topk > 0 && p->mode != 2 && !c->freq_over)
    {
        td_buf t = TD_BUF_INIT;

        td_cell_text(&t, v);
        if (!freq_add(p, c, h, t.ptr == NULL ? "" : t.ptr, t.len))
            freq_drop(p, c);
        td_buf_free(&t);
    }
}

static void
pass2_cell(prof *p, pcol *c, const ta_data_cell *v, td_buf *tmp)
{
    double d;

    if (v->null)
        return;
    if (c->hist != NULL && td_cell_num(v, &d) && !isnan(d))
    {
        double w = (c->dmax - c->dmin) / HIST_BINS;
        size_t b = w <= 0 ? 0 : (size_t)((d - c->dmin) / w);

        if (b >= HIST_BINS)
            b = HIST_BINS - 1;
        c->hist[b]++;
    }
    if (c->mg != NULL)
    {
        td_buf t = TD_BUF_INIT;

        td_cell_text(&t, v);
        mg_add(p, c, cell_hash(v, tmp), t.ptr == NULL ? "" : t.ptr, t.len);
        td_buf_free(&t);
    }
}

static int
run_pass(prof *p, ta_data_ds *ds, const td_kv *opts, int pass,
         td_buf *scan_res, uint64_t *rows, char **err)
{
    td_scan s;
    td_buf tmp = TD_BUF_INIT;
    size_t i;
    int rc = td_scan_open(&s, ds, opts, err);

    if (rc != 0)
        return rc;
    while ((rc = td_scan_next(&s, err)) == 0)
    {
        for (i = 0; i < p->ncols; i++)
        {
            pcol *c = &p->cols[i];

            if (pass == 1)
                pass1_cell(p, c, &s.row.cells[c->idx], &tmp);
            else
                pass2_cell(p, c, &s.row.cells[c->idx], &tmp);
        }
    }
    td_buf_free(&tmp);
    if (rc == 1)
        rc = 0;
    if (rc == 0 && scan_res != NULL)
    {
        td_scan_result(&s, scan_res);
        *rows = s.rows;
    }
    td_scan_close(&s);
    return rc;
}

/* ---- output ------------------------------------------------------- */

static double
exact_quantile(const double *v, size_t n, double q)
{
    double h = q * (double)(n - 1);
    size_t lo = (size_t)floor(h);
    size_t hi = lo + 1 < n ? lo + 1 : lo;

    return v[lo] + (h - (double)lo) * (v[hi] - v[lo]);
}

static double
hist_quantile(const pcol *c, double q, uint64_t n)
{
    double w = (c->dmax - c->dmin) / HIST_BINS;
    double r = q * (double)(n - 1);
    uint64_t cum = 0;
    size_t b;

    if (w <= 0)
        return c->dmin;
    for (b = 0; b < HIST_BINS; b++)
    {
        if ((double)(cum + c->hist[b]) > r)
            return c->dmin + w * ((double)b + (r - (double)cum + 0.5) /
                                  (double)c->hist[b]);
        cum += c->hist[b];
    }
    return c->dmax;
}

static int
dcmp(const void *a, const void *b)
{
    double x = *(const double *)a;
    double y = *(const double *)b;

    return x < y ? -1 : (x > y ? 1 : 0);
}

static int
fcmp(const void *a, const void *b)
{
    const freq_ent *x = a;
    const freq_ent *y = b;

    if (x->count != y->count)
        return x->count > y->count ? -1 : 1;
    return strcmp(x->text, y->text);
}

static void
put_num(td_buf *b, const char *key, double v, int kind)
{
    td_buf_printf(b, "\t%s=", key);
    if (kind == TA_DATA_T_TIMESTAMP)
        td_fmt_timestamp(b, (int64_t)llround(v));
    else if (kind == TA_DATA_T_DATE)
        td_fmt_date(b, (int64_t)llround(v));
    else
        td_fmt_double(b, v);
}

static void
put_cell(td_buf *b, const char *key, const ta_data_cell *v)
{
    td_buf t = TD_BUF_INIT;

    td_cell_text(&t, v);
    td_buf_printf(b, "\t%s=", key);
    td_esc(b, t.ptr == NULL ? "" : t.ptr, t.len);
    td_buf_free(&t);
}

static void
emit_col(prof *p, ta_data_ds *ds, pcol *c, uint64_t rows, td_buf *b)
{
    const td_col *col = &ds->schema.cols[c->idx];
    size_t i;

    td_buf_adds(b, "column=");
    td_esc(b, col->name, strlen(col->name));
    td_buf_adds(b, "\ttype=");
    td_type_format(b, col->type);
    td_buf_printf(b, "\tcount=%llu\tnulls=%llu\tbad=%llu\tnull_pct=",
                  (unsigned long long)c->count, (unsigned long long)c->nulls,
                  (unsigned long long)c->bad);
    td_fmt_double(b, rows == 0 ? 0 : 100.0 * (double)c->nulls / (double)rows);
    if (!c->set_over)
        td_buf_printf(b, "\tdistinct=%zu\tdistinct_algo=exact-hash64"
                      "\tdistinct_err=0", c->setn);
    else
    {
        td_buf_printf(b, "\tdistinct=%.0f\tdistinct_algo=hll-p%d"
                      "\tdistinct_err=", td_hll_count(c->hll), TD_HLL_P);
        td_fmt_double(b, 1.04 / sqrt((double)(1u << TD_HLL_P)));
    }
    if (c->have_minmax)
    {
        put_cell(b, "min", &c->minc);
        put_cell(b, "max", &c->maxc);
    }
    if (c->nan > 0)
        td_buf_printf(b, "\tnan=%llu", (unsigned long long)c->nan);
    if (c->numeric && c->nnum > 0)
    {
        bool time = c->kind == TA_DATA_T_DATE ||
                    c->kind == TA_DATA_T_TIMESTAMP;

        if (!time)
        {
            put_num(b, "mean", c->mean, TA_DATA_T_FLOAT);
            put_num(b, "stddev", c->nnum > 1 ?
                    sqrt(c->m2 / (double)(c->nnum - 1)) : 0,
                    TA_DATA_T_FLOAT);
            put_num(b, "stddev_pop", sqrt(c->m2 / (double)c->nnum),
                    TA_DATA_T_FLOAT);
            if (c->isum_ok && c->kind != TA_DATA_T_FLOAT)
            {
                td_buf_adds(b, "\tsum=");
                td_fmt_decimal(b, c->isum, c->iscale);
            }
            else
                put_num(b, "sum", c->ksum + c->kcomp, TA_DATA_T_FLOAT);
        }
        else
        {
            put_num(b, "range_s", c->kind == TA_DATA_T_DATE ?
                    (c->dmax - c->dmin) * 86400 :
                    (c->dmax - c->dmin) / 1e6, TA_DATA_T_FLOAT);
        }
        if (kind_has_quantiles(c->kind) && p->nq > 0)
        {
            bool exact = !c->vals_over && p->mode != 2;

            if (exact)
                qsort(c->vals, c->nvals, sizeof(double), dcmp);
            td_buf_adds(b, "\tquantiles=");
            for (i = 0; i < p->nq; i++)
            {
                double v = exact ? exact_quantile(c->vals, c->nvals, p->qs[i]) :
                           hist_quantile(c, p->qs[i], c->nnum);

                if (i > 0)
                    td_buf_addc(b, '|');
                td_fmt_double(b, p->qs[i]);
                td_buf_addc(b, ':');
                if (time)
                {
                    td_buf t = TD_BUF_INIT;

                    if (c->kind == TA_DATA_T_DATE)
                        td_fmt_date(&t, (int64_t)llround(v));
                    else
                        td_fmt_timestamp(&t, (int64_t)llround(v));
                    td_esc(b, t.ptr, t.len);
                    td_buf_free(&t);
                }
                else
                    td_fmt_double(b, v);
            }
            if (exact)
                td_buf_adds(b, "\tq_algo=exact-linear\tq_err=0");
            else
            {
                td_buf_printf(b, "\tq_algo=histogram-%d\tq_err=", HIST_BINS);
                td_fmt_double(b, (c->dmax - c->dmin) / HIST_BINS);
            }
        }
    }
    if (c->lens && c->count > 0)
    {
        td_buf_printf(b, "\tlen_min=%llu\tlen_max=%llu",
                      (unsigned long long)c->len_min,
                      (unsigned long long)c->len_max);
        put_num(b, "len_mean", c->len_sum / (double)c->count,
                TA_DATA_T_FLOAT);
    }
    if (p->topk > 0)
    {
        freq_ent *v = c->freq_over ? c->mg : c->freq;
        size_t n = c->freq_over ? c->mgn : c->freqcap;
        freq_ent *sorted = calloc(n + 1, sizeof(*sorted));
        size_t k = 0;

        for (i = 0; sorted != NULL && i < n; i++)
        {
            if (v[i].text != NULL && v[i].count > 0)
                sorted[k++] = v[i];
        }
        if (sorted != NULL)
            qsort(sorted, k, sizeof(*sorted), fcmp);
        td_buf_adds(b, "\ttop=");
        for (i = 0; i < k && i < p->topk; i++)
        {
            if (i > 0)
                td_buf_addc(b, '|');
            td_esc(b, sorted[i].text, strlen(sorted[i].text));
            td_buf_printf(b, ":%llu", (unsigned long long)sorted[i].count);
        }
        free(sorted);
        if (!c->freq_over)
            td_buf_adds(b, "\ttop_algo=exact\ttop_err=0");
        else
            td_buf_printf(b, "\ttop_algo=misra-gries-%zu\ttop_err=%llu",
                          p->mgk,
                          (unsigned long long)(c->count / (p->mgk + 1)));
    }
    td_buf_addc(b, '\n');
}

static void
pcol_free(prof *p, pcol *c)
{
    size_t i;

    ta_data_cell_clear(&c->minc);
    ta_data_cell_clear(&c->maxc);
    free(c->vals);
    free(c->hist);
    free(c->hll);
    free(c->set);
    if (!c->freq_over && c->freq != NULL)
        freq_drop(p, c);
    for (i = 0; i < c->mgn; i++)
        free(c->mg[i].text);
    free(c->mg);
}

int
ta_data_profile(ta_data_ds *ds, const char *params, char **result,
                char **err)
{
    prof p;
    td_kv opts;
    td_buf scanres = TD_BUF_INIT;
    uint64_t rows = 0;
    td_buf out = TD_BUF_INIT;
    size_t *idx = NULL;
    char **qv = NULL;
    size_t nqv;
    const char *mode;
    bool need2 = false;
    size_t i;
    int rc;

    memset(&p, 0, sizeof(p));
    rc = td_opts(ds, params, &opts);
    if (rc != 0)
        return rc;
    p.mem.limit = td_opts_mem(&opts);
    mode = td_kv_get(&opts, "mode", "auto");
    p.mode = strcasecmp(mode, "exact") == 0 ? 1 :
             strcasecmp(mode, "approx") == 0 ? 2 : 0;
    p.topk = (size_t)td_kv_int(&opts, "topk", 5);
    p.mgk = p.topk * 10 < 64 ? 64 : p.topk * 10;
    nqv = td_split(td_kv_get(&opts, "quantiles", "0.25,0.5,0.75"), ',', &qv);
    p.qs = calloc(nqv + 1, sizeof(double));
    for (i = 0; p.qs != NULL && i < nqv; i++)
    {
        double q = strtod(qv[i], NULL);

        if (q < 0 || q > 1)
        {
            rc = td_fail(err, TA_DATA_EINVAL, "quantile %s is outside [0,1]",
                         qv[i]);
            break;
        }
        p.qs[p.nq++] = q;
    }
    td_split_free(qv, nqv);
    if (rc == 0)
        rc = td_cols_resolve(ds, td_kv_get(&opts, "columns", ""), &idx,
                             &p.ncols, err);
    if (rc != 0)
        goto out;
    p.cols = calloc(p.ncols + 1, sizeof(*p.cols));
    if (p.cols == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < p.ncols; i++)
    {
        pcol *c = &p.cols[i];

        c->idx = idx[i];
        c->kind = ds->schema.cols[idx[i]].type->kind;
        c->numeric = td_kind_numeric(c->kind) || c->kind == TA_DATA_T_BOOL ||
                     c->kind == TA_DATA_T_DATE ||
                     c->kind == TA_DATA_T_TIMESTAMP;
        c->lens = c->kind == TA_DATA_T_STRING || c->kind == TA_DATA_T_BINARY;
        c->isum_ok = true;
        c->minc.null = c->maxc.null = 1;
        c->hll = calloc(1, sizeof(td_hll));
        if (c->hll == NULL || !td_mem_take(&p.mem, sizeof(td_hll)))
        {
            rc = td_fail(err, TA_DATA_E2BIG, "mem_limit is too small for "
                         "even an approximate profile");
            goto out;
        }
        if (p.mode == 2)
        {
            c->set_over = true;
            c->vals_over = true;
            c->freq_over = true;
        }
    }

    rc = run_pass(&p, ds, &opts, 1, &scanres, &rows, err);
    if (rc != 0)
        goto out;

    for (i = 0; i < p.ncols; i++)
    {
        pcol *c = &p.cols[i];

        if (p.mode == 1 && (c->set_over || c->vals_over ||
                            (c->freq_over && p.topk > 0)))
        {
            rc = td_fail(err, TA_DATA_E2BIG,
                         "exact profile of column '%s' needs more than "
                         "mem_limit=%llu bytes",
                         ds->schema.cols[c->idx].name,
                         (unsigned long long)p.mem.limit);
            goto out;
        }
        if (c->vals_over && c->nnum > 0 && kind_has_quantiles(c->kind) &&
            p.nq > 0)
        {
            c->hist = calloc(HIST_BINS, sizeof(uint64_t));
            need2 = true;
        }
        if (c->freq_over && p.topk > 0)
        {
            c->mg = calloc(p.mgk, sizeof(freq_ent));
            need2 = true;
        }
    }
    if (need2)
    {
        rc = run_pass(&p, ds, &opts, 2, NULL, NULL, err);
        if (rc != 0)
            goto out;
    }

    td_buf_add(&out, scanres.ptr, scanres.len);
    td_buf_printf(&out, "columns=%zu\npasses=%d\nmem_peak=%llu\n"
                  "mem_limit=%llu\n", p.ncols, need2 ? 2 : 1,
                  (unsigned long long)p.mem.peak,
                  (unsigned long long)p.mem.limit);
    for (i = 0; i < p.ncols; i++)
        emit_col(&p, ds, &p.cols[i], rows, &out);
    *result = td_buf_take(&out);

out:
    for (i = 0; p.cols != NULL && i < p.ncols; i++)
        pcol_free(&p, &p.cols[i]);
    free(p.cols);
    free(p.qs);
    free(idx);
    td_buf_free(&scanres);
    td_buf_free(&out);
    td_kv_free(&opts);
    return rc;
}
