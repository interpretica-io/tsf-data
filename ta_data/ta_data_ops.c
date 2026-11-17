/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: count, sample, digest, generate,
 *        convert, aggregate, reconcile
 */

#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <jansson.h>

#ifdef TA_DATA_HAVE_SQLITE
#include <sqlite3.h>
#endif

#include "ta_data_int.h"

/* ---- count -------------------------------------------------------- */

int
ta_data_count(ta_data_ds *ds, const char *params, char **result, char **err)
{
    td_kv opts;
    td_scan s;
    td_buf res = TD_BUF_INIT;
    uint64_t *nulls;
    uint64_t null_cells = 0;
    size_t i;
    int rc;

    rc = td_opts(ds, params, &opts);
    if (rc != 0)
        return rc;
    nulls = calloc(ds->schema.n + 1, sizeof(uint64_t));
    rc = nulls == NULL ? TA_DATA_ENOMEM : td_scan_open(&s, ds, &opts, err);
    if (rc != 0)
    {
        free(nulls);
        td_kv_free(&opts);
        return rc;
    }
    while ((rc = td_scan_next(&s, err)) == 0)
    {
        for (i = 0; i < s.row.n; i++)
        {
            if (s.row.cells[i].null && !s.row.cells[i].bad)
            {
                nulls[i]++;
                null_cells++;
            }
        }
    }
    if (rc == 1)
    {
        rc = 0;
        td_scan_result(&s, &res);
        td_buf_printf(&res, "null_cells=%llu\ncolumns=%zu\nbytes=%llu\n",
                      (unsigned long long)null_cells, ds->schema.n,
                      (unsigned long long)ds->file_bytes);
        td_buf_adds(&res, "col_nulls=");
        for (i = 0; i < ds->schema.n; i++)
        {
            if (i > 0)
                td_buf_addc(&res, '|');
            td_esc(&res, ds->schema.cols[i].name,
                   strlen(ds->schema.cols[i].name));
            td_buf_printf(&res, ":%llu", (unsigned long long)nulls[i]);
        }
        td_buf_adds(&res, "\ncol_bad=");
        for (i = 0; i < ds->schema.n; i++)
        {
            if (i > 0)
                td_buf_addc(&res, '|');
            td_esc(&res, ds->schema.cols[i].name,
                   strlen(ds->schema.cols[i].name));
            td_buf_printf(&res, ":%llu", (unsigned long long)s.col_bad[i]);
        }
        td_buf_addc(&res, '\n');
        *result = td_buf_take(&res);
    }
    td_scan_close(&s);
    free(nulls);
    td_kv_free(&opts);
    return rc;
}

/* ---- sample ------------------------------------------------------- */

static void
row_jsonl(td_buf *b, const ta_data_ds *ds, const td_row *r, const size_t *idx,
          size_t n)
{
    size_t i;

    td_buf_printf(b, "{\"_row\":%llu", (unsigned long long)r->rowno);
    for (i = 0; i < n; i++)
    {
        ta_data_cell name;

        memset(&name, 0, sizeof(name));
        name.type = TA_DATA_T_STRING;
        name.s = ds->schema.cols[idx[i]].name;
        name.len = strlen(name.s);
        td_buf_addc(b, ',');
        td_cell_json(b, &name);
        td_buf_addc(b, ':');
        td_cell_json(b, &r->cells[idx[i]]);
    }
    td_buf_adds(b, "}\n");
}

int
ta_data_sample(ta_data_ds *ds, const char *params, char **result,
               char **rows, char **err)
{
    td_kv opts;
    td_scan s;
    td_buf res = TD_BUF_INIT;
    td_buf out = TD_BUF_INIT;
    size_t *idx = NULL;
    size_t n = 0;
    uint64_t offset;
    uint64_t limit;
    bool random;
    td_rng rng;
    char **keep = NULL;
    uint64_t kept = 0;
    uint64_t seen = 0;
    uint64_t i;
    int rc;

    rc = td_opts(ds, params, &opts);
    if (rc != 0)
        return rc;
    offset = (uint64_t)td_kv_int(&opts, "offset", 0);
    limit = (uint64_t)td_kv_int(&opts, "limit", 10);
    if (limit > 10000)
        limit = 10000;      /* a sample is bounded, always */
    random = strcasecmp(td_kv_get(&opts, "mode", "head"), "random") == 0;
    td_rng_seed(&rng, (uint64_t)td_kv_int(&opts, "seed", 1));
    rc = td_cols_resolve(ds, td_kv_get(&opts, "columns", ""), &idx, &n, err);
    if (rc == 0)
    {
        keep = calloc(limit + 1, sizeof(char *));
        rc = keep == NULL ? TA_DATA_ENOMEM : td_scan_open(&s, ds, &opts, err);
    }
    if (rc != 0)
    {
        free(idx);
        free(keep);
        td_kv_free(&opts);
        return rc;
    }
    while ((rc = td_scan_next(&s, err)) == 0)
    {
        td_buf line = TD_BUF_INIT;

        if (s.rows <= offset)
            continue;
        seen++;
        if (!random)
        {
            if (kept >= limit)
            {
                if (!td_kv_bool(&opts, "count_all", false))
                {
                    rc = 1;
                    break;
                }
                continue;
            }
            row_jsonl(&line, ds, &s.row, idx, n);
            keep[kept++] = td_buf_take(&line);
            continue;
        }
        /* Reservoir sampling (Algorithm R): uniform, bounded memory. */
        if (kept < limit)
        {
            row_jsonl(&line, ds, &s.row, idx, n);
            keep[kept++] = td_buf_take(&line);
        }
        else
        {
            uint64_t j = (uint64_t)td_rng_range(&rng, 0, (int64_t)seen - 1);

            if (j < limit)
            {
                row_jsonl(&line, ds, &s.row, idx, n);
                free(keep[j]);
                keep[j] = td_buf_take(&line);
            }
        }
    }
    if (rc == 1)
    {
        rc = 0;
        td_scan_result(&s, &res);
        td_buf_printf(&res, "returned=%llu\nlimit=%llu\n",
                      (unsigned long long)kept, (unsigned long long)limit);
        for (i = 0; i < kept; i++)
            td_buf_adds(&out, keep[i]);
        *result = td_buf_take(&res);
        *rows = td_buf_take(&out);
    }
    for (i = 0; i < kept; i++)
        free(keep[i]);
    free(keep);
    free(idx);
    td_scan_close(&s);
    td_kv_free(&opts);
    return rc;
}

/* ---- digest ------------------------------------------------------- */

typedef struct digest {
    uint64_t ordered;
    uint64_t multiset;
    uint64_t rows;
} digest;

static void
digest_row(digest *d, const td_row *r, const size_t *idx, size_t n,
           td_buf *tmp)
{
    uint64_t h;
    size_t i;

    td_buf_reset(tmp);
    for (i = 0; i < n; i++)
        td_cell_ser(tmp, &r->cells[idx == NULL ? i : idx[i]], false);
    h = td_hash64(tmp->ptr == NULL ? "" : tmp->ptr, tmp->len, 0xd16e57);
    d->ordered = td_hash64(&h, sizeof(h), d->ordered);
    d->multiset += h;
    d->rows++;
}

static void
digest_put(td_buf *b, const digest *d)
{
    td_buf_printf(b, "digest_rows=%llu\ndigest=%016llx\n"
                  "digest_unordered=%016llx\n", (unsigned long long)d->rows,
                  (unsigned long long)d->ordered,
                  (unsigned long long)d->multiset);
}

int
ta_data_digest(ta_data_ds *ds, const char *params, char **result, char **err)
{
    td_kv opts;
    td_scan s;
    td_buf res = TD_BUF_INIT;
    td_buf tmp = TD_BUF_INIT;
    digest d = { 0, 0, 0 };
    size_t *idx = NULL;
    size_t n = 0;
    int rc;

    rc = td_opts(ds, params, &opts);
    if (rc == 0)
        rc = td_cols_resolve(ds, td_kv_get(&opts, "columns", ""), &idx, &n,
                             err);
    if (rc == 0)
        rc = td_scan_open(&s, ds, &opts, err);
    if (rc != 0)
    {
        free(idx);
        td_kv_free(&opts);
        return rc;
    }
    while ((rc = td_scan_next(&s, err)) == 0)
        digest_row(&d, &s.row, idx, n, &tmp);
    if (rc == 1)
    {
        rc = 0;
        td_scan_result(&s, &res);
        digest_put(&res, &d);
        *result = td_buf_take(&res);
    }
    td_buf_free(&tmp);
    td_scan_close(&s);
    free(idx);
    td_kv_free(&opts);
    return rc;
}

/* ---- synthetic generation ----------------------------------------- */

typedef enum gen_kind {
    G_SEQ = 1,
    G_UNIFORM,
    G_NORMAL,
    G_BOOL,
    G_STRING,
    G_UUID,
    G_TIME,
    G_ENUM,
    G_CORR,
    G_CONST,
} gen_kind;

typedef struct gen_col {
    char       *name;
    td_type    *type;
    gen_kind    kind;
    td_kv       args;
    td_rng      rng;
    double      null_p;
    double      boundary_p;
    /* parsed arguments */
    double      lo;
    double      hi;
    int64_t     ilo;
    int64_t     ihi;
    int64_t     start;
    int64_t     step;
    int64_t     jitter;
    double      p;
    char      **values;
    size_t      nvalues;
    double     *weights;
    double      wsum;
    int         of;         /* G_CORR source column */
    double      k;
    double      b;
    double      noise;
    char       *pattern;
    size_t      len_min;
    size_t      len_max;
    ta_data_cell konst;
} gen_col;

static const struct {
    const char *name;
    gen_kind kind;
} gen_names[] = {
    { "seq", G_SEQ }, { "uniform", G_UNIFORM }, { "range", G_UNIFORM },
    { "normal", G_NORMAL }, { "bool", G_BOOL }, { "string", G_STRING },
    { "pattern", G_STRING }, { "uuid", G_UUID }, { "time", G_TIME },
    { "timestamp", G_TIME }, { "date", G_TIME }, { "enum", G_ENUM },
    { "corr", G_CORR }, { "copy", G_CORR }, { "const", G_CONST },
};

static int
parse_when(const char *s, const td_type *t, int64_t *out)
{
    if (s == NULL)
        return -1;
    if (t->kind == TA_DATA_T_DATE)
        return td_parse_date(s, strlen(s), out);
    return td_parse_timestamp(s, strlen(s), out);
}

static int
gen_col_parse(const char *line, gen_col *g, const td_schema *sch,
              uint64_t seed, char **err)
{
    char *f[4] = { NULL, NULL, NULL, NULL };
    size_t nf = 0;
    const char *p = line;
    int depth = 0;
    size_t i;
    int rc = 0;

    memset(g, 0, sizeof(*g));
    g->konst.null = 1;
    g->of = -1;
    /* name:type:generator[:args] - the type may hold ':' inside {} and
     * the arguments (timestamps) may hold ':' anywhere. */
    while (nf < 3)
    {
        const char *q = p;

        while (*q != '\0' && !(*q == ':' && depth == 0))
        {
            if (*q == '{' || *q == '<' || *q == '(')
                depth++;
            else if ((*q == '}' || *q == '>' || *q == ')') && depth > 0)
                depth--;
            q++;
        }
        f[nf++] = td_strndup(p, (size_t)(q - p));
        if (*q == '\0')
            break;
        p = q + 1;
    }
    if (nf == 3 && p > line && p[-1] == ':')
        f[nf++] = td_strdup(p);
    if (nf < 3)
    {
        for (i = 0; i < nf; i++)
            free(f[i]);
        return td_fail(err, TA_DATA_EINVAL, "generator '%s' is not "
                       "name:type:generator[:args]", line);
    }
    g->name = td_strdup(f[0]);
    rc = td_type_parse(f[1], &g->type, err);
    for (i = 0; rc == 0 && i < sizeof(gen_names) / sizeof(gen_names[0]); i++)
    {
        if (strcasecmp(f[2], gen_names[i].name) == 0)
            g->kind = gen_names[i].kind;
    }
    if (rc == 0 && g->kind == 0)
        rc = td_fail(err, TA_DATA_EINVAL, "unknown generator '%s'", f[2]);
    /* Arguments: ',' separated k=v. */
    if (rc == 0 && nf == 4)
    {
        char **kv;
        size_t nkv = td_split(f[3], ',', &kv);
        size_t k;

        for (k = 0; k < nkv; k++)
        {
            char *eq = strchr(kv[k], '=');

            if (eq != NULL)
            {
                *eq = '\0';
                td_kv_set(&g->args, kv[k], eq + 1);
            }
        }
        td_split_free(kv, nkv);
    }
    for (i = 0; i < nf; i++)
        free(f[i]);
    if (rc != 0)
        return rc;

    /* Each column has its own stream, so adding a column leaves the
     * others' values unchanged. */
    td_rng_seed(&g->rng, seed ^ td_hash64(g->name, strlen(g->name), 0x6e6));
    g->null_p = td_kv_dbl(&g->args, "null", 0);
    g->boundary_p = td_kv_dbl(&g->args, "boundary", 0);
    g->lo = td_kv_dbl(&g->args, "min", 0);
    g->hi = td_kv_dbl(&g->args, "max", 100);
    g->ilo = td_kv_int(&g->args, "min", 0);
    g->ihi = td_kv_int(&g->args, "max", 100);
    g->start = td_kv_int(&g->args, "start", 1);
    g->step = td_kv_int(&g->args, "step", 1);
    g->jitter = td_kv_int(&g->args, "jitter", 0);
    g->p = td_kv_dbl(&g->args, "p", 0.5);
    g->k = td_kv_dbl(&g->args, "k", 1);
    g->b = td_kv_dbl(&g->args, "b", 0);
    g->noise = td_kv_dbl(&g->args, "noise", 0);
    g->len_min = (size_t)td_kv_int(&g->args, "len_min", 8);
    g->len_max = (size_t)td_kv_int(&g->args, "len_max", g->len_min);
    if (g->kind == G_NORMAL)
    {
        g->lo = td_kv_dbl(&g->args, "mean", 0);
        g->hi = td_kv_dbl(&g->args, "stddev", 1);
    }
    if (g->kind == G_TIME || (g->kind == G_SEQ &&
                              (g->type->kind == TA_DATA_T_TIMESTAMP ||
                               g->type->kind == TA_DATA_T_DATE)))
    {
        const char *st = td_kv_get(&g->args, "start", "2026-01-01T00:00:00Z");

        if (parse_when(st, g->type, &g->start) != 0)
            return td_fail(err, TA_DATA_EINVAL, "column '%s': start '%s' is "
                           "not a %s", g->name, st,
                           td_kind_name(g->type->kind));
        g->step = td_kv_int(&g->args, "step",
                            g->type->kind == TA_DATA_T_DATE ? 1 : 1000000);
        g->kind = G_TIME;
    }
    if (g->kind == G_ENUM)
    {
        char **w = NULL;
        size_t nw;

        g->nvalues = td_split(td_kv_get(&g->args, "values", ""), '|',
                              &g->values);
        if (g->nvalues == 0)
            return td_fail(err, TA_DATA_EINVAL, "column '%s': enum needs "
                           "values=a|b|c", g->name);
        nw = td_split(td_kv_get(&g->args, "weights", ""), '|', &w);
        g->weights = calloc(g->nvalues, sizeof(double));
        if (g->weights == NULL)
        {
            td_split_free(w, nw);
            return TA_DATA_ENOMEM;
        }
        for (i = 0; i < g->nvalues; i++)
        {
            g->weights[i] = i < nw ? strtod(w[i], NULL) : 1.0;
            g->wsum += g->weights[i];
        }
        td_split_free(w, nw);
    }
    if (g->kind == G_CORR)
    {
        const char *of = td_kv_get(&g->args, "of", "");

        g->of = td_schema_find(sch, of);
        if (g->of < 0)
            return td_fail(err, TA_DATA_EINVAL, "column '%s': of='%s' must "
                           "name an earlier column", g->name, of);
    }
    if (g->kind == G_STRING)
        g->pattern = td_strdup(td_kv_get(&g->args, "pattern", ""));
    if (g->kind == G_CONST)
    {
        const char *v = td_kv_get(&g->args, "value", "");

        if (td_parse_text(v, strlen(v), g->type, &g->konst) != 0)
            return td_fail(err, TA_DATA_EINVAL, "column '%s': value '%s' is "
                           "not a %s", g->name, v,
                           td_kind_name(g->type->kind));
    }
    return 0;
}

static void
gen_col_free(gen_col *g)
{
    free(g->name);
    td_type_free(g->type);
    td_kv_free(&g->args);
    td_split_free(g->values, g->nvalues);
    free(g->weights);
    free(g->pattern);
    ta_data_cell_clear(&g->konst);
}

/* One character from a bracket class like "A-Z0-9_". */
static char
class_char(td_rng *r, const char *cls, size_t n)
{
    char pool[256];
    size_t np = 0;
    size_t i;

    for (i = 0; i < n && np < sizeof(pool); i++)
    {
        if (i + 2 < n && cls[i + 1] == '-')
        {
            int c;

            for (c = (unsigned char)cls[i];
                 c <= (unsigned char)cls[i + 2] && np < sizeof(pool); c++)
                pool[np++] = (char)c;
            i += 2;
        }
        else
            pool[np++] = cls[i];
    }
    return np == 0 ? '?' : pool[td_rng_range(r, 0, (int64_t)np - 1)];
}

/*
 * A small pattern language: literals, \d \w \l \u, [classes], and the
 * quantifiers {n} {n,m} ? + * (+ and * stop at 8).
 */
static void
gen_pattern(td_rng *r, const char *p, td_buf *out)
{
    while (*p != '\0')
    {
        char cls[128];
        size_t ncls = 0;
        bool literal = false;
        char lit = 0;
        int lo = 1;
        int hi = 1;
        int n;
        int k;

        if (*p == '\\' && p[1] != '\0')
        {
            switch (p[1])
            {
                case 'd': strcpy(cls, "0-9"); break;
                case 'w': strcpy(cls, "a-zA-Z0-9_"); break;
                case 'l': strcpy(cls, "a-z"); break;
                case 'u': strcpy(cls, "A-Z"); break;
                default: literal = true; lit = p[1]; break;
            }
            ncls = strlen(cls);
            p += 2;
        }
        else if (*p == '[')
        {
            const char *e = strchr(p + 1, ']');

            if (e == NULL)
            {
                literal = true;
                lit = *p++;
            }
            else
            {
                ncls = (size_t)(e - p - 1) < sizeof(cls) - 1 ?
                       (size_t)(e - p - 1) : sizeof(cls) - 1;
                memcpy(cls, p + 1, ncls);
                p = e + 1;
            }
        }
        else
        {
            literal = true;
            lit = *p++;
        }
        if (*p == '{')
        {
            if (sscanf(p, "{%d,%d}", &lo, &hi) == 2 ||
                (sscanf(p, "{%d}", &lo) == 1 && (hi = lo, true)))
                p = strchr(p, '}') + 1;
        }
        else if (*p == '?')
        {
            lo = 0;
            hi = 1;
            p++;
        }
        else if (*p == '+' || *p == '*')
        {
            lo = *p == '+' ? 1 : 0;
            hi = 8;
            p++;
        }
        n = (int)td_rng_range(r, lo, hi);
        for (k = 0; k < n; k++)
            td_buf_addc(out, literal ? lit : class_char(r, cls, ncls));
    }
}

static void
boundary_value(gen_col *g, ta_data_cell *c)
{
    static const int64_t ints[] = { INT64_MIN, INT64_MAX, 0, -1, 1 };
    static const double flts[] = { 0.0, -1e308, 1e308, 5e-324, -0.0 };
    static const char *const strs[] = {
        "", " ", "a,\"b\"\nc", "\xce\xa9\xe2\x89\x88\xc3\xa7\xe2\x88\x9a",
        "NULL",
    };
    static const char *const dates[] = {
        "1970-01-01", "0001-01-01", "9999-12-31", "2000-02-29", "1969-12-31",
    };
    static const char *const tss[] = {
        "1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999Z",
        "9999-12-31T23:59:59Z", "2000-02-29T12:00:00Z",
        "2038-01-19T03:14:08Z",
    };
    int64_t pick = td_rng_range(&g->rng, 0, 4);
    td_buf long_s = TD_BUF_INIT;

    c->null = 0;
    c->type = g->type->kind;
    switch (g->type->kind)
    {
        case TA_DATA_T_INT:
            c->i = ints[pick];
            break;
        case TA_DATA_T_FLOAT:
            c->d = flts[pick];
            break;
        case TA_DATA_T_DECIMAL:
        {
            int64_t m = 1;
            int i;

            for (i = 0; i < g->type->precision && i < 18; i++)
                m *= 10;
            c->scale = g->type->scale;
            c->i = pick == 0 ? m - 1 : pick == 1 ? -(m - 1) :
                   pick == 2 ? 0 : pick == 3 ? 1 : -1;
            break;
        }
        case TA_DATA_T_BOOL:
            c->i = pick % 2;
            break;
        case TA_DATA_T_DATE:
            td_parse_date(dates[pick], strlen(dates[pick]), &c->i);
            break;
        case TA_DATA_T_TIMESTAMP:
            td_parse_timestamp(tss[pick], strlen(tss[pick]), &c->i);
            break;
        default:
            if (pick == 4)
            {
                int i;

                for (i = 0; i < 1000; i++)
                    td_buf_addc(&long_s, (char)('a' + i % 26));
                td_cell_set_str(c, g->type->kind, long_s.ptr, long_s.len);
            }
            else
                td_cell_set_str(c, g->type->kind == TA_DATA_T_BINARY ?
                                TA_DATA_T_BINARY : TA_DATA_T_STRING,
                                strs[pick], strlen(strs[pick]));
            if (g->type->kind == TA_DATA_T_ARRAY)
                td_cell_set_str(c, TA_DATA_T_ARRAY, "[]", 2);
            else if (g->type->kind == TA_DATA_T_OBJECT)
                td_cell_set_str(c, TA_DATA_T_OBJECT, "{}", 2);
            break;
    }
    td_buf_free(&long_s);
}

static int
gen_value(gen_col *g, uint64_t row, const td_row *cur, ta_data_cell *c)
{
    td_buf t = TD_BUF_INIT;
    double d = 0;
    int kind = g->type->kind;

    ta_data_cell_clear(c);
    /* Fixed draws per row whatever the outcome, so that the stream stays
     * aligned: null, boundary, value. */
    {
        double u_null = td_rng_double(&g->rng);
        double u_bound = td_rng_double(&g->rng);

        if (u_null < g->null_p)
            return 0;
        if (u_bound < g->boundary_p)
        {
            boundary_value(g, c);
            return 0;
        }
    }
    c->null = 0;
    c->type = kind;
    switch (g->kind)
    {
        case G_SEQ:
            d = (double)(g->start + (int64_t)row * g->step);
            c->i = g->start + (int64_t)row * g->step;
            break;
        case G_UNIFORM:
            if (kind == TA_DATA_T_INT)
                c->i = td_rng_range(&g->rng, g->ilo, g->ihi);
            else
                d = g->lo + (g->hi - g->lo) * td_rng_double(&g->rng);
            break;
        case G_NORMAL:
            d = g->lo + g->hi * td_rng_normal(&g->rng);
            break;
        case G_BOOL:
            c->type = TA_DATA_T_BOOL;
            c->i = td_rng_double(&g->rng) < g->p;
            kind = TA_DATA_T_BOOL;
            break;
        case G_STRING:
            if (g->pattern != NULL && *g->pattern != '\0')
                gen_pattern(&g->rng, g->pattern, &t);
            else
            {
                size_t n = (size_t)td_rng_range(&g->rng, (int64_t)g->len_min,
                                                (int64_t)g->len_max);
                size_t i;

                for (i = 0; i < n; i++)
                    td_buf_addc(&t, (char)('a' + td_rng_range(&g->rng, 0,
                                                               25)));
            }
            break;
        case G_UUID:
        {
            uint64_t a = td_rng_next(&g->rng);
            uint64_t b = td_rng_next(&g->rng);

            a = (a & ~0xf000ULL) | 0x4000ULL;           /* version 4 */
            b = (b & 0x3fffffffffffffffULL) | 0x8000000000000000ULL;
            td_buf_printf(&t, "%08llx-%04llx-%04llx-%04llx-%012llx",
                          (unsigned long long)(a >> 32),
                          (unsigned long long)((a >> 16) & 0xffff),
                          (unsigned long long)(a & 0xffff),
                          (unsigned long long)(b >> 48),
                          (unsigned long long)(b & 0xffffffffffffULL));
            break;
        }
        case G_TIME:
            c->i = g->start + (int64_t)row * g->step +
                   (g->jitter > 0 ? td_rng_range(&g->rng, -g->jitter,
                                                 g->jitter) : 0);
            break;
        case G_ENUM:
        {
            double u = td_rng_double(&g->rng) * g->wsum;
            size_t i;

            for (i = 0; i + 1 < g->nvalues && u >= g->weights[i]; i++)
                u -= g->weights[i];
            td_buf_adds(&t, g->values[i]);
            break;
        }
        case G_CORR:
        {
            const ta_data_cell *src = &cur->cells[g->of];
            double x;

            if (src->null)
            {
                c->null = 1;
                return 0;
            }
            if (td_cell_num(src, &x))
            {
                d = g->k * x + g->b +
                    (g->noise > 0 ? g->noise * td_rng_normal(&g->rng) : 0);
                if (kind == TA_DATA_T_INT || kind == TA_DATA_T_DATE ||
                    kind == TA_DATA_T_TIMESTAMP)
                    c->i = (int64_t)llround(d);
            }
            else
                td_cell_text(&t, src);
            break;
        }
        case G_CONST:
            td_cell_copy(c, &g->konst);
            return 0;
        default:
            break;
    }

    /* Bring the drawn value to the column type. */
    switch (kind)
    {
        case TA_DATA_T_FLOAT:
            if (g->kind == G_SEQ)
                d = (double)c->i;
            if (g->kind == G_UNIFORM && kind == TA_DATA_T_INT)
                d = (double)c->i;
            c->d = d;
            break;
        case TA_DATA_T_INT:
            if (g->kind == G_NORMAL || (g->kind == G_UNIFORM && false))
                c->i = (int64_t)llround(d);
            break;
        case TA_DATA_T_DECIMAL:
        {
            int64_t m = 1;
            int i;

            for (i = 0; i < g->type->scale; i++)
                m *= 10;
            if (g->kind == G_SEQ)
                d = (double)c->i;
            c->scale = g->type->scale;
            c->i = (int64_t)llround(d * (double)m);
            break;
        }
        case TA_DATA_T_STRING:
        case TA_DATA_T_BINARY:
        {
            int rc;

            if (t.ptr == NULL && (g->kind == G_SEQ || g->kind == G_UNIFORM ||
                                  g->kind == G_NORMAL || g->kind == G_TIME))
            {
                ta_data_cell tmp = *c;

                if (g->kind == G_UNIFORM || g->kind == G_NORMAL)
                {
                    tmp.type = TA_DATA_T_FLOAT;
                    tmp.d = d;
                }
                else
                    tmp.type = TA_DATA_T_INT;
                tmp.s = NULL;
                td_cell_text(&t, &tmp);
            }
            rc = td_cell_set_str(c, kind, t.ptr == NULL ? "" : t.ptr, t.len);
            td_buf_free(&t);
            return rc;
        }
        case TA_DATA_T_ARRAY:
        case TA_DATA_T_OBJECT:
        case TA_DATA_T_DATE:
        case TA_DATA_T_TIMESTAMP:
        case TA_DATA_T_BOOL:
            if (t.ptr != NULL)
            {
                int rc = td_parse_text(t.ptr, t.len, g->type, c);

                td_buf_free(&t);
                if (rc != 0)
                    c->null = 1;
                return 0;
            }
            break;
        default:
            break;
    }
    td_buf_free(&t);
    return 0;
}

int
ta_data_generate(const char *path, const char *format, const char *params,
                 const char *spec, char **result, char **err)
{
    td_kv opts = TD_KV_INIT;
    td_schema sch;
    gen_col *cols = NULL;
    size_t ncols = 0;
    char **lines = NULL;
    size_t nlines;
    td_format fmt;
    td_writer *w = NULL;
    td_row row;
    td_rng rowrng;
    uint64_t rows;
    uint64_t seed;
    double dups;
    double malformed;
    td_row *ring = NULL;
    size_t ring_n = 0;
    const size_t ring_cap = 1024;
    uint64_t n_dup = 0;
    uint64_t n_bad = 0;
    uint64_t bytes = 0;
    digest dg = { 0, 0, 0 };
    td_buf tmp = TD_BUF_INIT;
    td_buf res = TD_BUF_INIT;
    uint64_t r;
    size_t i;
    int rc;

    memset(&sch, 0, sizeof(sch));
    memset(&row, 0, sizeof(row));
    rc = td_kv_parse(&opts, params);
    if (rc != 0)
        return rc;
    if (td_format_parse(format, path, &fmt) != 0)
    {
        td_kv_free(&opts);
        return td_fail(err, TA_DATA_EINVAL, "unknown output format for '%s'",
                       path);
    }
    rows = (uint64_t)td_kv_int(&opts, "rows", 100);
    seed = (uint64_t)td_kv_int(&opts, "seed", 1);
    dups = td_kv_dbl(&opts, "dups", 0);
    malformed = td_kv_dbl(&opts, "malformed", 0);
    td_rng_seed(&rowrng, seed ^ 0x726f7773ULL);

    nlines = td_split(spec == NULL ? "" : spec, '\n', &lines);
    cols = calloc(nlines + 1, sizeof(*cols));
    if (cols == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < nlines && rc == 0; i++)
    {
        if (lines[i][0] == '\0' || lines[i][0] == '#')
            continue;
        rc = gen_col_parse(lines[i], &cols[ncols], &sch, seed, err);
        ncols++;
        if (rc == 0)
            rc = td_schema_add(&sch, cols[ncols - 1].name,
                               td_type_dup(cols[ncols - 1].type),
                               cols[ncols - 1].null_p > 0 ||
                               cols[ncols - 1].boundary_p > 0 ||
                               !td_kv_bool(&cols[ncols - 1].args, "required",
                                           false));
    }
    if (rc == 0 && ncols == 0)
        rc = td_fail(err, TA_DATA_EINVAL, "empty generator spec");
    if (rc == 0 && malformed > 0 && fmt == TD_FMT_PARQUET)
        rc = td_fail(err, TA_DATA_EINVAL, "Parquet is typed: it cannot "
                     "carry malformed records");
    if (rc != 0)
        goto out;
    sch.explicit_ = true;
    ring = calloc(ring_cap, sizeof(*ring));
    rc = ring == NULL ? TA_DATA_ENOMEM : td_row_init(&row, ncols);
    if (rc == 0)
        rc = td_writer_open(path, fmt, &sch, &opts, &w, err);
    for (r = 0; rc == 0 && r < rows; r++)
    {
        double u_dup = td_rng_double(&rowrng);
        double u_bad = td_rng_double(&rowrng);
        const td_row *out = &row;

        /* The column streams advance every row, emitted or not, so a
         * duplicate or malformed row does not shift later values. */
        for (i = 0; i < ncols && rc == 0; i++)
            rc = gen_value(&cols[i], r, &row, &row.cells[i]);
        if (rc != 0)
            break;
        if (ring_n > 0 && u_dup < dups)
        {
            out = &ring[(size_t)td_rng_range(&rowrng, 0,
                                             (int64_t)ring_n - 1)];
            n_dup++;
        }
        else
        {
            td_row *slot;

            if (ring_n < ring_cap)
            {
                slot = &ring[ring_n++];
                td_row_init(slot, ncols);
            }
            else
                slot = &ring[(size_t)(r % ring_cap)];
            for (i = 0; i < ncols; i++)
                td_cell_copy(&slot->cells[i], &row.cells[i]);
        }
        if (u_bad < malformed)
        {
            rc = td_writer_malformed(w, out, &rowrng, err);
            n_bad++;
            continue;
        }
        digest_row(&dg, out, NULL, ncols, &tmp);
        rc = td_writer_row(w, out, err);
    }
    if (w != NULL)
    {
        int rc2 = td_writer_close(w, &bytes, rc == 0 ? err : NULL);

        if (rc == 0)
            rc = rc2;
    }
    if (rc != 0)
        goto out;
    td_buf_printf(&res, "rows=%llu\nwritten=%llu\nduplicates=%llu\n"
                  "malformed=%llu\nbytes=%llu\nformat=%s\nseed=%llu\n",
                  (unsigned long long)rows,
                  (unsigned long long)(rows - n_bad),
                  (unsigned long long)n_dup, (unsigned long long)n_bad,
                  (unsigned long long)bytes, td_format_name(fmt),
                  (unsigned long long)seed);
    digest_put(&res, &dg);
    td_buf_adds(&res, "schema=");
    for (i = 0; i < sch.n; i++)
    {
        td_buf t = TD_BUF_INIT;

        td_buf_printf(&t, "%s%s:", i > 0 ? "," : "", sch.cols[i].name);
        td_type_format(&t, sch.cols[i].type);
        if (!sch.cols[i].nullable)
            td_buf_addc(&t, '!');
        td_esc(&res, t.ptr, t.len);
        td_buf_free(&t);
    }
    td_buf_addc(&res, '\n');
    *result = td_buf_take(&res);

out:
    for (i = 0; ring != NULL && i < ring_n; i++)
        td_row_free(&ring[i]);
    free(ring);
    td_row_free(&row);
    for (i = 0; cols != NULL && i < ncols; i++)
        gen_col_free(&cols[i]);
    free(cols);
    td_split_free(lines, nlines);
    td_schema_free(&sch);
    td_buf_free(&tmp);
    td_buf_free(&res);
    td_kv_free(&opts);
    return rc;
}

/* ---- convert ------------------------------------------------------ */

int
ta_data_convert(ta_data_ds *ds, const char *path, const char *format,
                const char *params, char **result, char **err)
{
    td_kv opts;
    td_format fmt;
    td_writer *w = NULL;
    td_scan s;
    td_buf res = TD_BUF_INIT;
    td_buf tmp = TD_BUF_INIT;
    digest dg = { 0, 0, 0 };
    uint64_t bytes = 0;
    int rc;

    rc = td_opts(ds, params, &opts);
    if (rc != 0)
        return rc;
    if (td_format_parse(format, path, &fmt) != 0)
    {
        td_kv_free(&opts);
        return td_fail(err, TA_DATA_EINVAL, "unknown output format for '%s'",
                       path);
    }
    rc = td_scan_open(&s, ds, &opts, err);
    if (rc != 0)
    {
        td_kv_free(&opts);
        return rc;
    }
    rc = td_writer_open(path, fmt, &ds->schema, &opts, &w, err);
    while (rc == 0 && (rc = td_scan_next(&s, err)) == 0)
    {
        digest_row(&dg, &s.row, NULL, s.row.n, &tmp);
        rc = td_writer_row(w, &s.row, err);
    }
    if (rc == 1)
        rc = 0;
    if (w != NULL)
    {
        int rc2 = td_writer_close(w, &bytes, rc == 0 ? err : NULL);

        if (rc == 0)
            rc = rc2;
    }
    if (rc == 0)
    {
        td_scan_result(&s, &res);
        td_buf_printf(&res, "bytes=%llu\nformat=%s\n",
                      (unsigned long long)bytes, td_format_name(fmt));
        digest_put(&res, &dg);
        *result = td_buf_take(&res);
    }
    td_buf_free(&tmp);
    td_scan_close(&s);
    td_kv_free(&opts);
    return rc;
}

/* ---- aggregate ---------------------------------------------------- */

typedef enum agg_fn {
    A_COUNT = 1,
    A_SUM,
    A_MIN,
    A_MAX,
    A_AVG,
} agg_fn;

typedef struct agg {
    agg_fn  fn;
    int     col;        /* -1 for count(*) */
    char   *name;
} agg;

typedef struct actx {
    ta_data_ds     *ds;
    size_t         *gidx;
    size_t          ng;
    agg            *aggs;
    size_t          na;
    td_schema       out;
    td_row         *rows;
    size_t          nrows;
    size_t          cap;
    td_mem         *mem;
    char          **keys;   /* serialized group keys, for sorting */
    size_t         *klens;
} actx;

static int
agg_cb(void *ctx, const char *key, size_t klen, const td_rec *recs, size_t n,
       char **err)
{
    actx *a = ctx;
    td_row *r;
    const char *p = key;
    size_t i;
    size_t k;

    if (a->nrows == a->cap)
    {
        size_t cap = a->cap == 0 ? 256 : a->cap * 2;
        td_row *nr = realloc(a->rows, cap * sizeof(*nr));
        char **nk;
        size_t *nl;

        if (nr == NULL)
            return TA_DATA_ENOMEM;
        a->rows = nr;
        nk = realloc(a->keys, cap * sizeof(*nk));
        if (nk == NULL)
            return TA_DATA_ENOMEM;
        a->keys = nk;
        nl = realloc(a->klens, cap * sizeof(*nl));
        if (nl == NULL)
            return TA_DATA_ENOMEM;
        a->klens = nl;
        a->cap = cap;
    }
    if (!td_mem_take(a->mem, klen + (a->ng + a->na) * sizeof(ta_data_cell) +
                     64))
        return td_fail(err, TA_DATA_E2BIG, "aggregate output does not fit "
                       "mem_limit");
    r = &a->rows[a->nrows];
    td_row_init(r, a->ng + a->na);
    a->keys[a->nrows] = td_strndup(key, klen);
    a->klens[a->nrows] = klen;
    a->nrows++;
    for (i = 0; i < a->ng; i++)
        td_cell_deser(&p, key + klen, &r->cells[i]);
    for (k = 0; k < a->na; k++)
    {
        ta_data_cell *o = &r->cells[a->ng + k];
        const agg *g = &a->aggs[k];
        uint64_t cnt = 0;
        double sum = 0;
        int64_t isum = 0;
        bool exact_int = true;
        int scale = 0;
        ta_data_cell best;

        memset(&best, 0, sizeof(best));
        best.null = 1;
        for (i = 0; i < n; i++)
        {
            const char *q = recs[i].payload;
            const char *e = q + recs[i].plen;
            ta_data_cell v;
            size_t j;
            double d;

            memset(&v, 0, sizeof(v));
            /* The payload holds every aggregate's input, in order. */
            for (j = 0; j <= k; j++)
                td_cell_deser(&q, e, &v);
            if (g->fn == A_COUNT && g->col < 0)
            {
                cnt++;
                ta_data_cell_clear(&v);
                continue;
            }
            if (v.null)
            {
                ta_data_cell_clear(&v);
                continue;
            }
            cnt++;
            if (g->fn == A_MIN || g->fn == A_MAX)
            {
                if (best.null ||
                    (g->fn == A_MIN ? td_cell_order(&v, &best) < 0 :
                                      td_cell_order(&v, &best) > 0))
                    td_cell_copy(&best, &v);
            }
            else if (td_cell_num(&v, &d))
            {
                sum += d;
                if (v.type == TA_DATA_T_INT || v.type == TA_DATA_T_DECIMAL)
                {
                    if (__builtin_add_overflow(isum, v.i, &isum))
                        exact_int = false;
                    scale = v.scale;
                }
                else
                    exact_int = false;
            }
            ta_data_cell_clear(&v);
        }
        ta_data_cell_clear(o);
        o->null = 0;
        switch (g->fn)
        {
            case A_COUNT:
                o->type = TA_DATA_T_INT;
                o->i = (int64_t)cnt;
                break;
            case A_SUM:
                if (cnt == 0)
                {
                    o->null = 1;
                    break;
                }
                o->type = a->out.cols[a->ng + k].type->kind;
                if (o->type == TA_DATA_T_FLOAT || !exact_int)
                {
                    o->type = TA_DATA_T_FLOAT;
                    o->d = sum;
                }
                else
                {
                    o->i = isum;
                    o->scale = scale;
                }
                break;
            case A_AVG:
                if (cnt == 0)
                {
                    o->null = 1;
                    break;
                }
                o->type = TA_DATA_T_FLOAT;
                o->d = sum / (double)cnt;
                break;
            default:
                *o = best;
                memset(&best, 0, sizeof(best));
                best.null = 1;
                break;
        }
        ta_data_cell_clear(&best);
    }
    return 0;
}

static actx *sort_ctx;

static int
agg_row_cmp(const void *x, const void *y)
{
    size_t i = *(const size_t *)x;
    size_t j = *(const size_t *)y;
    size_t k;

    for (k = 0; k < sort_ctx->ng; k++)
    {
        const ta_data_cell *a = &sort_ctx->rows[i].cells[k];
        const ta_data_cell *b = &sort_ctx->rows[j].cells[k];
        int c;

        if (a->null || b->null)
        {
            if (a->null != b->null)
                return a->null ? -1 : 1;
            continue;
        }
        c = td_cell_order(a, b);
        if (c != 0)
            return c;
    }
    return 0;
}

int
ta_data_aggregate(ta_data_ds *ds, const char *path, const char *format,
                  const char *params, char **result, char **err)
{
    td_kv opts;
    actx a;
    td_mem mem;
    td_group *g = NULL;
    td_scan s;
    bool have_scan = false;
    td_writer *w = NULL;
    td_format fmt;
    char **spec = NULL;
    size_t nspec = 0;
    td_buf key = TD_BUF_INIT;
    td_buf pay = TD_BUF_INIT;
    td_buf res = TD_BUF_INIT;
    size_t *order = NULL;
    uint64_t bytes = 0;
    size_t i;
    int rc;

    memset(&a, 0, sizeof(a));
    memset(&mem, 0, sizeof(mem));
    rc = td_opts(ds, params, &opts);
    if (rc != 0)
        return rc;
    mem.limit = td_opts_mem(&opts);
    a.ds = ds;
    a.mem = &mem;
    if (td_format_parse(format, path, &fmt) != 0)
    {
        rc = td_fail(err, TA_DATA_EINVAL, "unknown output format for '%s'",
                     path);
        goto out;
    }
    rc = td_cols_resolve(ds, td_kv_get(&opts, "group_by", ""), &a.gidx, &a.ng,
                         err);
    if (rc == 0 && td_kv_get(&opts, "group_by", "")[0] == '\0')
        a.ng = 0;
    if (rc != 0)
        goto out;
    nspec = td_split(td_kv_get(&opts, "aggs", "count:*"), ',', &spec);
    a.aggs = calloc(nspec + 1, sizeof(*a.aggs));
    if (a.aggs == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < a.ng; i++)
    {
        const td_col *c = &ds->schema.cols[a.gidx[i]];

        td_schema_add(&a.out, c->name, td_type_dup(c->type), c->nullable);
    }
    for (i = 0; i < nspec; i++)
    {
        char *colon = strchr(spec[i], ':');
        const char *fn = spec[i];
        const char *cn = colon != NULL ? colon + 1 : "*";
        agg *ag = &a.aggs[a.na];
        td_buf name = TD_BUF_INIT;
        td_type *t;

        if (colon != NULL)
            *colon = '\0';
        ag->fn = strcasecmp(fn, "count") == 0 ? A_COUNT :
                 strcasecmp(fn, "sum") == 0 ? A_SUM :
                 strcasecmp(fn, "min") == 0 ? A_MIN :
                 strcasecmp(fn, "max") == 0 ? A_MAX :
                 strcasecmp(fn, "avg") == 0 ? A_AVG : 0;
        if (ag->fn == 0)
        {
            rc = td_fail(err, TA_DATA_EINVAL, "unknown aggregate '%s'", fn);
            goto out;
        }
        ag->col = strcmp(cn, "*") == 0 ? -1 : td_schema_find(&ds->schema, cn);
        if (ag->col < 0 && !(ag->fn == A_COUNT && strcmp(cn, "*") == 0))
        {
            rc = td_fail(err, TA_DATA_ENOENT, "no column '%s'", cn);
            goto out;
        }
        if (ag->col < 0)
            td_buf_adds(&name, "count");
        else
            td_buf_printf(&name, "%s_%s", fn, cn);
        ag->name = td_buf_take(&name);
        switch (ag->fn)
        {
            case A_COUNT:
                t = td_type_simple(TA_DATA_T_INT);
                break;
            case A_AVG:
                t = td_type_simple(TA_DATA_T_FLOAT);
                break;
            default:
                t = td_type_dup(ds->schema.cols[ag->col].type);
                break;
        }
        td_schema_add(&a.out, ag->name, t, ag->fn != A_COUNT);
        a.na++;
    }
    a.out.explicit_ = true;

    rc = td_group_open(&g, &mem, td_kv_get(&opts, "tmpdir", NULL),
                       ds->file_bytes, err);
    if (rc == 0)
        rc = td_scan_open(&s, ds, &opts, err);
    have_scan = rc == 0;
    while (rc == 0 && (rc = td_scan_next(&s, err)) == 0)
    {
        td_buf_reset(&key);
        td_buf_reset(&pay);
        for (i = 0; i < a.ng; i++)
            td_cell_ser(&key, &s.row.cells[a.gidx[i]], false);
        for (i = 0; i < a.na; i++)
        {
            ta_data_cell nul;

            memset(&nul, 0, sizeof(nul));
            nul.null = 1;
            td_cell_ser(&pay, a.aggs[i].col < 0 ? &nul :
                        &s.row.cells[a.aggs[i].col], false);
        }
        rc = td_group_add(g, 0, s.row.rowno, key.ptr == NULL ? "" : key.ptr,
                          key.len, pay.ptr, pay.len, err);
    }
    if (rc == 1)
        rc = 0;
    if (rc == 0)
        rc = td_group_run(g, agg_cb, &a, err);
    if (rc != 0)
        goto out;

    /* Deterministic output: groups sorted by their key values. */
    order = calloc(a.nrows + 1, sizeof(size_t));
    if (order == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < a.nrows; i++)
        order[i] = i;
    sort_ctx = &a;
    qsort(order, a.nrows, sizeof(size_t), agg_row_cmp);
    sort_ctx = NULL;
    rc = td_writer_open(path, fmt, &a.out, &opts, &w, err);
    for (i = 0; rc == 0 && i < a.nrows; i++)
        rc = td_writer_row(w, &a.rows[order[i]], err);
    if (w != NULL)
    {
        int rc2 = td_writer_close(w, &bytes, rc == 0 ? err : NULL);

        if (rc == 0)
            rc = rc2;
    }
    if (rc == 0)
    {
        td_scan_result(&s, &res);
        td_buf_printf(&res, "groups=%zu\nbytes=%llu\npartitions=%u\n"
                      "mem_peak=%llu\nschema=", a.nrows,
                      (unsigned long long)bytes, td_group_partitions(g),
                      (unsigned long long)mem.peak);
        for (i = 0; i < a.out.n; i++)
        {
            td_buf t = TD_BUF_INIT;

            td_buf_printf(&t, "%s%s:", i > 0 ? "," : "", a.out.cols[i].name);
            td_type_format(&t, a.out.cols[i].type);
            td_esc(&res, t.ptr, t.len);
            td_buf_free(&t);
        }
        td_buf_addc(&res, '\n');
        *result = td_buf_take(&res);
    }

out:
    if (have_scan)
        td_scan_close(&s);
    td_group_close(g);
    for (i = 0; i < a.nrows; i++)
    {
        td_row_free(&a.rows[i]);
        free(a.keys[i]);
    }
    free(a.rows);
    free(a.keys);
    free(a.klens);
    for (i = 0; i < a.na; i++)
        free(a.aggs[i].name);
    free(a.aggs);
    free(a.gidx);
    free(order);
    td_schema_free(&a.out);
    td_split_free(spec, nspec);
    td_buf_free(&key);
    td_buf_free(&pay);
    td_buf_free(&res);
    td_kv_free(&opts);
    return rc;
}

/* ---- reconcile ---------------------------------------------------- */

typedef struct rctx {
    td_report  *rep;
    size_t      nk;
    uint64_t    lost;       /* source keys absent from the target */
    uint64_t    phantom;    /* target keys with no source */
    uint64_t    dup_src;
    uint64_t    dup_dst;
    uint64_t    both;
} rctx;

static int
rec_cb(void *ctx, const char *key, size_t klen, const td_rec *recs, size_t n,
       char **err)
{
    rctx *r = ctx;
    uint64_t ns = 0;
    uint64_t nd = 0;
    uint64_t first_s = 0;
    uint64_t first_d = 0;
    size_t i;

    (void)err;
    for (i = 0; i < n; i++)
    {
        if (recs[i].side == 0)
        {
            if (ns++ == 0)
                first_s = recs[i].rowno;
        }
        else if (nd++ == 0)
            first_d = recs[i].rowno;
    }
    if (ns > 1)
        r->dup_src += ns - 1;
    if (nd > 1)
        r->dup_dst += nd - 1;
    if (ns > 0 && nd > 0)
    {
        r->both++;
        return 0;
    }
    {
        td_buf l = TD_BUF_INIT;
        td_buf kt = TD_BUF_INIT;
        const char *p = key;
        ta_data_cell v;
        size_t k;

        memset(&v, 0, sizeof(v));
        for (k = 0; k < r->nk; k++)
        {
            if (td_cell_deser(&p, key + klen, &v) != 0)
                break;
            if (k > 0)
                td_buf_addc(&kt, ',');
            if (v.null)
                td_buf_adds(&kt, "NULL");
            else
                td_cell_text(&kt, &v);
        }
        ta_data_cell_clear(&v);
        td_buf_printf(&l, "%s\t%llu\t%llu\t", ns > 0 ? "lost" : "phantom",
                      (unsigned long long)first_s,
                      (unsigned long long)first_d);
        td_esc(&l, kt.ptr == NULL ? "" : kt.ptr, kt.len);
        td_report_add(r->rep, ns > 0 ? 1 : 2, first_s, first_d, l.ptr);
        td_buf_free(&l);
        td_buf_free(&kt);
    }
    if (ns > 0)
        r->lost++;
    else
        r->phantom++;
    return 0;
}

int
ta_data_reconcile(ta_data_ds *src, ta_data_ds *dst, const char *params,
                  char **result, char **report, char **err)
{
    td_kv opts = TD_KV_INIT;
    td_mem mem;
    td_report rep;
    td_group *g = NULL;
    rctx r;
    char **keys = NULL;
    char **sums = NULL;
    char **map = NULL;
    size_t nkeys;
    size_t nsums;
    size_t nmap;
    double *sum[2] = { NULL, NULL };
    uint64_t rows[2] = { 0, 0 };
    td_buf res = TD_BUF_INIT;
    int side;
    size_t i;
    int rc;

    memset(&mem, 0, sizeof(mem));
    memset(&r, 0, sizeof(r));
    rc = td_kv_parse(&opts, params);
    if (rc != 0)
        return rc;
    mem.limit = td_opts_mem(&opts);
    td_report_init(&rep, (size_t)td_kv_int(&opts, "max_report", 50));
    r.rep = &rep;
    nkeys = td_split(td_kv_get(&opts, "keys", ""), ',', &keys);
    nsums = td_split(td_kv_get(&opts, "sums", ""), ',', &sums);
    nmap = td_split(td_kv_get(&opts, "map", ""), ',', &map);
    r.nk = nkeys;
    sum[0] = calloc(nsums + 1, sizeof(double));
    sum[1] = calloc(nsums + 1, sizeof(double));
    if (sum[0] == NULL || sum[1] == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    if (nkeys > 0)
        rc = td_group_open(&g, &mem, td_kv_get(&opts, "tmpdir", NULL),
                           src->file_bytes + dst->file_bytes, err);
    for (side = 0; side < 2 && rc == 0; side++)
    {
        ta_data_ds *ds = side == 0 ? src : dst;
        td_kv so;
        td_scan s;
        int *kidx = calloc(nkeys + 1, sizeof(int));
        int *sidx = calloc(nsums + 1, sizeof(int));
        td_buf key = TD_BUF_INIT;

        rc = kidx == NULL || sidx == NULL ? TA_DATA_ENOMEM : 0;
        /* Target names come through map=src:dst. */
        for (i = 0; rc == 0 && i < nkeys + nsums; i++)
        {
            const char *n = i < nkeys ? keys[i] : sums[i - nkeys];
            size_t m;
            int k;

            if (side == 1)
            {
                for (m = 0; m < nmap; m++)
                {
                    const char *c = strchr(map[m], ':');

                    if (c != NULL && (size_t)(c - map[m]) == strlen(n) &&
                        strncmp(map[m], n, strlen(n)) == 0)
                        n = c + 1;
                }
            }
            k = td_schema_find(&ds->schema, n);
            if (k < 0)
                rc = td_fail(err, TA_DATA_ENOENT, "no column '%s' in the %s",
                             n, side == 0 ? "source" : "target");
            else if (i < nkeys)
                kidx[i] = k;
            else
                sidx[i - nkeys] = k;
        }
        if (rc == 0)
            rc = td_opts(ds, NULL, &so);
        if (rc == 0)
        {
            for (i = 0; i < opts.n; i++)
                td_kv_set(&so, opts.key[i], opts.val[i]);
            rc = td_scan_open(&s, ds, &so, err);
            td_kv_free(&so);
            while (rc == 0 && (rc = td_scan_next(&s, err)) == 0)
            {
                for (i = 0; i < nsums; i++)
                {
                    double d;

                    if (td_cell_num(&s.row.cells[sidx[i]], &d))
                        sum[side][i] += d;
                }
                if (nkeys == 0)
                    continue;
                td_buf_reset(&key);
                for (i = 0; i < nkeys; i++)
                {
                    const ta_data_cell *v = &s.row.cells[kidx[i]];

                    td_cell_ser(&key, v, td_kind_numeric(v->type) &&
                                v->type != TA_DATA_T_INT);
                }
                rc = td_group_add(g, side, s.row.rowno, key.ptr, key.len, "",
                                  0, err);
            }
            if (rc == 1)
                rc = 0;
            rows[side] = s.rows;
            td_scan_close(&s);
        }
        td_buf_free(&key);
        free(kidx);
        free(sidx);
    }
    if (rc == 0 && g != NULL)
        rc = td_group_run(g, rec_cb, &r, err);
    if (rc != 0)
        goto out;
    td_buf_printf(&res, "rows_src=%llu\nrows_dst=%llu\nrow_delta=%lld\n",
                  (unsigned long long)rows[0], (unsigned long long)rows[1],
                  (long long)rows[1] - (long long)rows[0]);
    if (nkeys > 0)
        td_buf_printf(&res, "keys_matched=%llu\nkeys_lost=%llu\n"
                      "keys_phantom=%llu\ndup_src=%llu\ndup_dst=%llu\n",
                      (unsigned long long)r.both, (unsigned long long)r.lost,
                      (unsigned long long)r.phantom,
                      (unsigned long long)r.dup_src,
                      (unsigned long long)r.dup_dst);
    for (i = 0; i < nsums; i++)
    {
        td_buf_printf(&res, "sum_%s_src=", sums[i]);
        td_fmt_double(&res, sum[0][i]);
        td_buf_printf(&res, "\nsum_%s_dst=", sums[i]);
        td_fmt_double(&res, sum[1][i]);
        td_buf_printf(&res, "\nsum_%s_delta=", sums[i]);
        td_fmt_double(&res, sum[1][i] - sum[0][i]);
        td_buf_addc(&res, '\n');
    }
    td_buf_printf(&res, "partitions=%u\nmem_peak=%llu\nreport_total=%llu\n",
                  g == NULL ? 0 : td_group_partitions(g),
                  (unsigned long long)mem.peak,
                  (unsigned long long)rep.total);
    *result = td_buf_take(&res);
    *report = td_report_take(&rep);
out:
    td_group_close(g);
    td_report_free(&rep);
    td_split_free(keys, nkeys);
    td_split_free(sums, nsums);
    td_split_free(map, nmap);
    free(sum[0]);
    free(sum[1]);
    td_buf_free(&res);
    td_kv_free(&opts);
    return rc;
}

/* ---- features ----------------------------------------------------- */

int
ta_data_features(unsigned int *features, char **versions)
{
    td_buf b = TD_BUF_INIT;
    unsigned int f = 0;

    td_buf_printf(&b, "jansson=%s", JANSSON_VERSION);
#ifdef TA_DATA_HAVE_SQLITE
    f |= TA_DATA_FEAT_SQLITE;
    td_buf_printf(&b, " sqlite=%s", sqlite3_libversion());
#endif
    if (ta_data_pq_supported())
    {
        f |= TA_DATA_FEAT_PARQUET;
        td_buf_printf(&b, " arrow=%s", ta_data_pq_version());
    }
    *features = f;
    *versions = td_buf_take(&b);
    return 0;
}
