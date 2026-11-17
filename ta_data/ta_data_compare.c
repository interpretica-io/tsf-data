/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: dataset comparison
 *
 * Six modes over two datasets A (expected) and B (actual):
 *
 * - exact: row i of A against row i of B, streaming, O(1) memory;
 * - unordered: the two as multisets of whole rows;
 * - key: rows matched by key columns, then compared column by column;
 * - schema: column names, types and nullability only;
 * - distribution: per column, the two-sample Kolmogorov-Smirnov
 *   statistic (numeric, from shared 1024-bin histograms) or the total
 *   variation distance of the value frequencies (other kinds);
 *
 * plus the options: ignored columns, a column mapping A->B, absolute and
 * relative float tolerance, timestamp tolerance, NULL semantics, string
 * trimming / case folding, duplicate treatment. "missing" counts rows of
 * A with no counterpart in B, "extra" rows of B with none in A,
 * "mismatched" rows found in both whose values differ.
 *
 * unordered and key go through the on-disk partitioned group-by, so
 * neither input has to fit in memory. The report is bounded (max_report)
 * and sorted by (kind, row of A, row of B), and its digest covers every
 * difference found, so two runs over the same inputs report the same
 * thing whatever the partitioning.
 */

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "ta_data_int.h"

enum {
    K_COL_MISSING = 1,
    K_COL_EXTRA,
    K_TYPE_DIFF,
    K_MISSING,
    K_EXTRA,
    K_MISMATCH,
    K_DUP_A,
    K_DUP_B,
};

static const char *
kind_name(int k)
{
    switch (k)
    {
        case K_COL_MISSING: return "column_missing";
        case K_COL_EXTRA: return "column_extra";
        case K_TYPE_DIFF: return "type_diff";
        case K_MISSING: return "missing";
        case K_EXTRA: return "extra";
        case K_MISMATCH: return "mismatch";
        case K_DUP_A: return "duplicate_a";
        case K_DUP_B: return "duplicate_b";
        default: return "?";
    }
}

typedef struct cmp {
    ta_data_ds     *a;
    ta_data_ds     *b;
    td_kv           opts;
    td_mem          mem;
    td_tol          tol;
    bool            multiset_dups;
    /* paired value columns (not keys): A index, B index */
    size_t         *pa;
    size_t         *pb;
    bool           *as_float;
    size_t          np;
    /* key columns */
    size_t         *ka;
    size_t         *kb;
    bool           *kfloat;
    size_t          nk;
    uint64_t        rows_a;
    uint64_t        rows_b;
    uint64_t        matched;
    uint64_t        missing;
    uint64_t        extra;
    uint64_t        mismatched;
    uint64_t        dup_a;
    uint64_t        dup_b;
    uint64_t        col_missing;
    uint64_t        col_extra;
    uint64_t        type_diffs;
    uint64_t       *col_mis;    /* per paired column */
    int             round_digits;
    td_report       rep;
    unsigned        partitions;
} cmp;

static void
add_report(cmp *c, int kind, uint64_t ra, uint64_t rb, const char *key,
           const char *column, const char *va, const char *vb)
{
    td_buf l = TD_BUF_INIT;

    td_buf_printf(&l, "%s\t%llu\t%llu\t", kind_name(kind),
                  (unsigned long long)ra, (unsigned long long)rb);
    if (key != NULL)
        td_esc(&l, key, strlen(key));
    td_buf_addc(&l, '\t');
    if (column != NULL)
        td_esc(&l, column, strlen(column));
    td_buf_addc(&l, '\t');
    if (va != NULL)
        td_esc(&l, va, strlen(va));
    td_buf_addc(&l, '\t');
    if (vb != NULL)
        td_esc(&l, vb, strlen(vb));
    td_report_add(&c->rep, kind, ra, rb, l.ptr);
    td_buf_free(&l);
}

static void
cell_str(td_buf *b, const ta_data_cell *v)
{
    td_buf_reset(b);
    if (v->null)
        td_buf_adds(b, v->bad ? "<bad>" : "NULL");
    else
        td_cell_text(b, v);
    if (b->ptr == NULL)
        td_buf_adds(b, "");
}

/* ---- column pairing ----------------------------------------------- */

static const char *
mapped_name(char **map, size_t nmap, const char *a)
{
    size_t i;

    for (i = 0; i < nmap; i++)
    {
        const char *colon = strchr(map[i], ':');

        if (colon != NULL && (size_t)(colon - map[i]) == strlen(a) &&
            strncmp(map[i], a, strlen(a)) == 0)
            return colon + 1;
    }
    return a;
}

static bool
in_list(char **v, size_t n, const char *s)
{
    size_t i;

    for (i = 0; i < n; i++)
    {
        if (strcmp(v[i], s) == 0)
            return true;
    }
    return false;
}

static int
pair_columns(cmp *c, bool report_schema, char **err)
{
    char **ign;
    char **map;
    char **keys;
    char **only;
    size_t nign = td_split(td_kv_get(&c->opts, "ignore", ""), ',', &ign);
    size_t nmap = td_split(td_kv_get(&c->opts, "map", ""), ',', &map);
    size_t nkeys = td_split(td_kv_get(&c->opts, "keys", ""), ',', &keys);
    size_t nonly = td_split(td_kv_get(&c->opts, "columns", ""), ',', &only);
    const td_schema *sa = &c->a->schema;
    const td_schema *sb = &c->b->schema;
    bool *b_used = calloc(sb->n + 1, sizeof(bool));
    size_t i;
    int rc = 0;

    c->pa = calloc(sa->n + 1, sizeof(size_t));
    c->pb = calloc(sa->n + 1, sizeof(size_t));
    c->as_float = calloc(sa->n + 1, sizeof(bool));
    c->ka = calloc(nkeys + 1, sizeof(size_t));
    c->kb = calloc(nkeys + 1, sizeof(size_t));
    c->kfloat = calloc(nkeys + 1, sizeof(bool));
    c->col_mis = calloc(sa->n + 1, sizeof(uint64_t));
    if (b_used == NULL || c->pa == NULL || c->pb == NULL ||
        c->as_float == NULL || c->ka == NULL || c->kb == NULL ||
        c->kfloat == NULL || c->col_mis == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < nkeys; i++)
    {
        int x = td_schema_find(sa, keys[i]);
        int y = td_schema_find(sb, mapped_name(map, nmap, keys[i]));

        if (x < 0 || y < 0)
        {
            rc = td_fail(err, TA_DATA_ENOENT, "key column '%s' is not in %s",
                         keys[i], x < 0 ? "A" : "B");
            goto out;
        }
        c->ka[c->nk] = (size_t)x;
        c->kb[c->nk] = (size_t)y;
        c->kfloat[c->nk] = sa->cols[x].type->kind != sb->cols[y].type->kind;
        c->nk++;
        b_used[y] = true;
    }
    for (i = 0; i < sa->n; i++)
    {
        const td_col *ca = &sa->cols[i];
        const char *bn;
        int y;

        if (in_list(ign, nign, ca->name) || in_list(keys, nkeys, ca->name))
            continue;
        if (nonly > 0 && !in_list(only, nonly, ca->name))
            continue;
        bn = mapped_name(map, nmap, ca->name);
        y = td_schema_find(sb, bn);
        if (y < 0)
        {
            c->col_missing++;
            if (report_schema)
                add_report(c, K_COL_MISSING, 0, 0, NULL, ca->name, NULL,
                           NULL);
            continue;
        }
        b_used[y] = true;
        if (ca->type->kind != sb->cols[y].type->kind ||
            ca->nullable != sb->cols[y].nullable)
        {
            td_buf ta = TD_BUF_INIT;
            td_buf tb = TD_BUF_INIT;

            td_type_format(&ta, ca->type);
            td_buf_adds(&ta, ca->nullable ? "" : "!");
            td_type_format(&tb, sb->cols[y].type);
            td_buf_adds(&tb, sb->cols[y].nullable ? "" : "!");
            if (strcmp(ta.ptr, tb.ptr) != 0)
            {
                c->type_diffs++;
                if (report_schema)
                    add_report(c, K_TYPE_DIFF, 0, 0, NULL, ca->name, ta.ptr,
                               tb.ptr);
            }
            td_buf_free(&ta);
            td_buf_free(&tb);
        }
        c->pa[c->np] = i;
        c->pb[c->np] = (size_t)y;
        c->as_float[c->np] = ca->type->kind != sb->cols[y].type->kind &&
                             td_kind_numeric(ca->type->kind) &&
                             td_kind_numeric(sb->cols[y].type->kind);
        c->np++;
    }
    for (i = 0; i < sb->n; i++)
    {
        const char *name = sb->cols[i].name;
        size_t k;
        bool ignored = false;

        if (b_used[i])
            continue;
        /* A B column reached by an ignored/filtered A column is not extra. */
        for (k = 0; k < sa->n && !ignored; k++)
        {
            if (strcmp(mapped_name(map, nmap, sa->cols[k].name), name) == 0)
                ignored = true;
        }
        if (ignored || in_list(ign, nign, name))
            continue;
        if (nonly > 0)
            continue;
        c->col_extra++;
        if (report_schema)
            add_report(c, K_COL_EXTRA, 0, 0, NULL, name, NULL, NULL);
    }
out:
    free(b_used);
    td_split_free(ign, nign);
    td_split_free(map, nmap);
    td_split_free(keys, nkeys);
    td_split_free(only, nonly);
    return rc;
}

/* ---- value comparison of one row pair ----------------------------- */

/* Returns true when the paired cells all agree; reports each that does
 * not. va/vb are the paired values in pairing order. */
static bool
compare_values(cmp *c, const ta_data_cell *va, const ta_data_cell *vb,
               uint64_t ra, uint64_t rb, const char *key)
{
    td_buf x = TD_BUF_INIT;
    td_buf y = TD_BUF_INIT;
    bool same = true;
    size_t i;

    for (i = 0; i < c->np; i++)
    {
        if (td_cell_equal(&va[i], &vb[i], &c->tol))
            continue;
        same = false;
        c->col_mis[i]++;
        cell_str(&x, &va[i]);
        cell_str(&y, &vb[i]);
        add_report(c, K_MISMATCH, ra, rb, key,
                   c->a->schema.cols[c->pa[i]].name, x.ptr, y.ptr);
    }
    td_buf_free(&x);
    td_buf_free(&y);
    return same;
}

/* ---- exact (positional) ------------------------------------------- */

static void
row_text(td_buf *b, const td_row *r, const size_t *idx, size_t n)
{
    td_buf t = TD_BUF_INIT;
    size_t i;

    td_buf_reset(b);
    for (i = 0; i < n; i++)
    {
        if (i > 0)
            td_buf_addc(b, ',');
        cell_str(&t, &r->cells[idx[i]]);
        td_buf_add(b, t.ptr, t.len);
    }
    if (b->ptr == NULL)
        td_buf_adds(b, "");
    td_buf_free(&t);
}

static int
cmp_exact(cmp *c, td_scan *sa, td_scan *sb, char **err)
{
    ta_data_cell *va = calloc(c->np + 1, sizeof(*va));
    ta_data_cell *vb = calloc(c->np + 1, sizeof(*vb));
    td_buf t = TD_BUF_INIT;
    int ra = 0;
    int rb = 0;
    int rc = 0;
    size_t i;

    if (va == NULL || vb == NULL)
    {
        free(va);
        free(vb);
        return TA_DATA_ENOMEM;
    }
    for (;;)
    {
        if (ra == 0)
            ra = td_scan_next(sa, err);
        if (rb == 0)
            rb = td_scan_next(sb, err);
        if ((ra != 0 && ra != 1) || (rb != 0 && rb != 1))
        {
            rc = ra > 1 ? ra : rb;
            break;
        }
        if (ra == 1 && rb == 1)
            break;
        if (ra == 0 && rb == 0)
        {
            for (i = 0; i < c->np; i++)
            {
                va[i] = sa->row.cells[c->pa[i]];
                vb[i] = sb->row.cells[c->pb[i]];
            }
            if (compare_values(c, va, vb, sa->row.rowno, sb->row.rowno, NULL))
                c->matched++;
            else
                c->mismatched++;
        }
        else if (ra == 0)
        {
            c->missing++;
            row_text(&t, &sa->row, c->pa, c->np);
            add_report(c, K_MISSING, sa->row.rowno, 0, NULL, NULL, t.ptr,
                       NULL);
        }
        else
        {
            c->extra++;
            row_text(&t, &sb->row, c->pb, c->np);
            add_report(c, K_EXTRA, 0, sb->row.rowno, NULL, NULL, NULL,
                       t.ptr);
        }
    }
    td_buf_free(&t);
    free(va);
    free(vb);
    return rc;
}

/* ---- partitioned: unordered and key ------------------------------- */

typedef struct pctx {
    cmp    *c;
    bool    keyed;
} pctx;

static int
decode_cells(const char *p, size_t len, ta_data_cell *out, size_t n)
{
    const char *end = p + len;
    size_t i;

    for (i = 0; i < n; i++)
    {
        if (td_cell_deser(&p, end, &out[i]) != 0)
            return TA_DATA_ECORRUPT;
    }
    return 0;
}

static void
clear_cells(ta_data_cell *v, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++)
        ta_data_cell_clear(&v[i]);
}

static void
key_text(cmp *c, const char *key, size_t klen, size_t ncells, td_buf *out)
{
    ta_data_cell *v = calloc(ncells + 1, sizeof(*v));
    td_buf t = TD_BUF_INIT;
    size_t i;

    (void)c;
    td_buf_reset(out);
    if (v != NULL && decode_cells(key, klen, v, ncells) == 0)
    {
        for (i = 0; i < ncells; i++)
        {
            if (i > 0)
                td_buf_addc(out, ',');
            cell_str(&t, &v[i]);
            td_buf_add(out, t.ptr, t.len);
        }
    }
    if (v != NULL)
    {
        clear_cells(v, ncells);
        free(v);
    }
    if (out->ptr == NULL)
        td_buf_adds(out, "");
    td_buf_free(&t);
}

static int
group_cb(void *ctx, const char *key, size_t klen, const td_rec *recs,
         size_t n, char **err)
{
    pctx *px = ctx;
    cmp *c = px->c;
    size_t na = 0;
    size_t nb = 0;
    size_t i;
    td_buf kt = TD_BUF_INIT;
    const td_rec **ra = malloc((n + 1) * sizeof(*ra));
    const td_rec **rb = malloc((n + 1) * sizeof(*rb));
    int rc = 0;

    if (ra == NULL || rb == NULL)
    {
        free(ra);
        free(rb);
        return TA_DATA_ENOMEM;
    }
    for (i = 0; i < n; i++)
    {
        if (recs[i].side == 0)
            ra[na++] = &recs[i];
        else
            rb[nb++] = &recs[i];
    }
    key_text(c, key, klen, px->keyed ? c->nk : c->np, &kt);

    if (!px->keyed)
    {
        size_t m = na < nb ? na : nb;

        c->matched += m;
        for (i = m; i < na; i++)
        {
            c->missing++;
            add_report(c, K_MISSING, ra[i]->rowno, 0, kt.ptr, NULL, NULL,
                       NULL);
        }
        for (i = m; i < nb; i++)
        {
            c->extra++;
            add_report(c, K_EXTRA, 0, rb[i]->rowno, kt.ptr, NULL, NULL, NULL);
        }
        goto out;
    }

    for (i = 1; i < na; i++)
    {
        c->dup_a++;
        add_report(c, K_DUP_A, ra[i]->rowno, 0, kt.ptr, NULL, NULL, NULL);
    }
    for (i = 1; i < nb; i++)
    {
        c->dup_b++;
        add_report(c, K_DUP_B, 0, rb[i]->rowno, kt.ptr, NULL, NULL, NULL);
    }
    {
        size_t pairs = na < nb ? na : nb;
        ta_data_cell *va = calloc(c->np + 1, sizeof(*va));
        ta_data_cell *vb = calloc(c->np + 1, sizeof(*vb));

        if (!c->multiset_dups && pairs > 1)
            pairs = 1;
        for (i = 0; va != NULL && vb != NULL && i < pairs && rc == 0; i++)
        {
            rc = decode_cells(ra[i]->payload, ra[i]->plen, va, c->np);
            if (rc == 0)
                rc = decode_cells(rb[i]->payload, rb[i]->plen, vb, c->np);
            if (rc == 0)
            {
                if (compare_values(c, va, vb, ra[i]->rowno, rb[i]->rowno,
                                   kt.ptr))
                    c->matched++;
                else
                    c->mismatched++;
            }
            clear_cells(va, c->np);
            clear_cells(vb, c->np);
        }
        if (va == NULL || vb == NULL)
            rc = TA_DATA_ENOMEM;
        free(va);
        free(vb);
        if (rc != 0)
        {
            rc = td_fail(err, rc, "spill record decode failed");
            goto out;
        }
        /* Unpaired: with first-only duplicate treatment only the keys
         * absent on one side count, the duplicates are reported above. */
        if (na == 0)
        {
            c->extra++;
            add_report(c, K_EXTRA, 0, rb[0]->rowno, kt.ptr, NULL, NULL, NULL);
        }
        else if (nb == 0)
        {
            c->missing++;
            add_report(c, K_MISSING, ra[0]->rowno, 0, kt.ptr, NULL, NULL,
                       NULL);
        }
        else if (c->multiset_dups)
        {
            for (i = pairs; i < na; i++)
            {
                c->missing++;
                add_report(c, K_MISSING, ra[i]->rowno, 0, kt.ptr, NULL, NULL,
                           NULL);
            }
            for (i = pairs; i < nb; i++)
            {
                c->extra++;
                add_report(c, K_EXTRA, 0, rb[i]->rowno, kt.ptr, NULL, NULL,
                           NULL);
            }
        }
        if (na == 0 && c->multiset_dups)
        {
            for (i = 1; i < nb; i++)
                c->extra++;
        }
        if (nb == 0 && c->multiset_dups)
        {
            for (i = 1; i < na; i++)
                c->missing++;
        }
    }
out:
    td_buf_free(&kt);
    free(ra);
    free(rb);
    return rc;
}

static void
round_cell(ta_data_cell *v, int digits)
{
    double d;

    if (digits <= 0 || v->null || !td_kind_numeric(v->type))
        return;
    td_cell_num(v, &d);
    if (d != 0 && isfinite(d))
    {
        double m = pow(10, digits - 1 - (int)floor(log10(fabs(d))));

        d = round(d * m) / m;
    }
    v->type = TA_DATA_T_FLOAT;
    v->d = d;
}

static int
feed(cmp *c, td_group *g, td_scan *s, int side, bool keyed, char **err)
{
    td_buf key = TD_BUF_INIT;
    td_buf pay = TD_BUF_INIT;
    const size_t *kidx = side == 0 ? c->ka : c->kb;
    const size_t *pidx = side == 0 ? c->pa : c->pb;
    size_t i;
    int rc;

    while ((rc = td_scan_next(s, err)) == 0)
    {
        td_buf_reset(&key);
        td_buf_reset(&pay);
        if (keyed)
        {
            for (i = 0; i < c->nk; i++)
                td_cell_ser(&key, &s->row.cells[kidx[i]], c->kfloat[i]);
            for (i = 0; i < c->np; i++)
                td_cell_ser(&pay, &s->row.cells[pidx[i]], false);
        }
        else
        {
            for (i = 0; i < c->np; i++)
            {
                ta_data_cell *v = &s->row.cells[pidx[i]];

                round_cell(v, c->round_digits);
                td_cell_ser(&key, v, c->as_float[i] || c->round_digits > 0);
            }
        }
        rc = td_group_add(g, side, s->row.rowno, key.ptr == NULL ? "" :
                          key.ptr, key.len, pay.ptr == NULL ? "" : pay.ptr,
                          pay.len, err);
        if (rc != 0)
            break;
    }
    td_buf_free(&key);
    td_buf_free(&pay);
    return rc == 1 ? 0 : rc;
}

static int
cmp_partitioned(cmp *c, td_scan *sa, td_scan *sb, bool keyed, char **err)
{
    td_group *g = NULL;
    pctx px = { c, keyed };
    int rc;

    if (keyed && c->nk == 0)
        return td_fail(err, TA_DATA_EINVAL, "mode=key needs keys=");
    rc = td_group_open(&g, &c->mem, td_kv_get(&c->opts, "tmpdir", NULL),
                       c->a->file_bytes + c->b->file_bytes, err);
    if (rc == 0)
        rc = feed(c, g, sa, 0, keyed, err);
    if (rc == 0)
        rc = feed(c, g, sb, 1, keyed, err);
    if (rc == 0)
        rc = td_group_run(g, group_cb, &px, err);
    if (g != NULL)
        c->partitions = td_group_partitions(g);
    td_group_close(g);
    return rc;
}

/* ---- distribution ------------------------------------------------- */

#define DIST_BINS 1024

typedef struct dist_col {
    bool        numeric;
    double      lo;
    double      hi;
    bool        seen;
    uint64_t   *ha;
    uint64_t   *hb;
    uint64_t    na;
    uint64_t    nb;
    double      tvd;
} dist_col;

typedef struct dctx {
    cmp        *c;
    dist_col   *dc;
} dctx;

static int
dist_group_cb(void *ctx, const char *key, size_t klen, const td_rec *recs,
              size_t n, char **err)
{
    dctx *d = ctx;
    size_t col;
    uint64_t ca = 0;
    uint64_t cb = 0;
    size_t i;

    (void)klen;
    (void)err;
    memcpy(&col, key, sizeof(col));
    for (i = 0; i < n; i++)
    {
        if (recs[i].side == 0)
            ca++;
        else
            cb++;
    }
    d->dc[col].tvd += 0.5 * fabs((d->dc[col].na == 0 ? 0 :
                                  (double)ca / (double)d->dc[col].na) -
                                 (d->dc[col].nb == 0 ? 0 :
                                  (double)cb / (double)d->dc[col].nb));
    return 0;
}

static int
dist_scan(cmp *c, ta_data_ds *ds, int side, dist_col *dc, int pass,
          td_group *g, td_buf *scanres, char **err)
{
    td_scan s;
    td_buf key = TD_BUF_INIT;
    const size_t *pidx = side == 0 ? c->pa : c->pb;
    size_t i;
    int rc = td_scan_open(&s, ds, &c->opts, err);

    if (rc != 0)
        return rc;
    while ((rc = td_scan_next(&s, err)) == 0)
    {
        for (i = 0; i < c->np; i++)
        {
            const ta_data_cell *v = &s.row.cells[pidx[i]];
            dist_col *d = &dc[i];
            double x;

            if (v->null)
                continue;
            if (d->numeric && td_cell_num(v, &x) && !isnan(x))
            {
                if (pass == 1)
                {
                    if (!d->seen || x < d->lo)
                        d->lo = x;
                    if (!d->seen || x > d->hi)
                        d->hi = x;
                    d->seen = true;
                }
                else
                {
                    double w = (d->hi - d->lo) / DIST_BINS;
                    size_t bin = w <= 0 ? 0 : (size_t)((x - d->lo) / w);

                    if (bin >= DIST_BINS)
                        bin = DIST_BINS - 1;
                    (side == 0 ? d->ha : d->hb)[bin]++;
                }
                if (pass == 1)
                {
                    if (side == 0)
                        d->na++;
                    else
                        d->nb++;
                }
            }
            else if (!d->numeric && pass == 1)
            {
                td_buf_reset(&key);
                td_buf_add(&key, &i, sizeof(i));
                td_cell_ser(&key, v, false);
                if (side == 0)
                    d->na++;
                else
                    d->nb++;
                rc = td_group_add(g, side, s.row.rowno, key.ptr, key.len, "",
                                  0, err);
                if (rc != 0)
                    break;
            }
        }
        if (rc != 0)
            break;
    }
    if (rc == 1)
        rc = 0;
    if (rc == 0 && scanres != NULL)
    {
        if (side == 0)
            c->rows_a = s.rows;
        else
            c->rows_b = s.rows;
        (void)scanres;
    }
    td_buf_free(&key);
    td_scan_close(&s);
    return rc;
}

static int
cmp_distribution(cmp *c, td_buf *res, char **err)
{
    dist_col *dc = calloc(c->np + 1, sizeof(*dc));
    td_group *g = NULL;
    dctx d = { c, dc };
    double worst = 0;
    const char *worst_col = "";
    size_t i;
    int rc;

    if (dc == NULL)
        return TA_DATA_ENOMEM;
    for (i = 0; i < c->np; i++)
    {
        int ka = c->a->schema.cols[c->pa[i]].type->kind;
        int kb = c->b->schema.cols[c->pb[i]].type->kind;

        dc[i].numeric = (td_kind_numeric(ka) || ka == TA_DATA_T_DATE ||
                         ka == TA_DATA_T_TIMESTAMP) &&
                        (td_kind_numeric(kb) || kb == TA_DATA_T_DATE ||
                         kb == TA_DATA_T_TIMESTAMP);
    }
    rc = td_group_open(&g, &c->mem, td_kv_get(&c->opts, "tmpdir", NULL),
                       c->a->file_bytes + c->b->file_bytes, err);
    if (rc == 0)
        rc = dist_scan(c, c->a, 0, dc, 1, g, res, err);
    if (rc == 0)
        rc = dist_scan(c, c->b, 1, dc, 1, g, res, err);
    if (rc == 0)
        rc = td_group_run(g, dist_group_cb, &d, err);
    for (i = 0; rc == 0 && i < c->np; i++)
    {
        if (dc[i].numeric)
        {
            dc[i].ha = calloc(DIST_BINS, sizeof(uint64_t));
            dc[i].hb = calloc(DIST_BINS, sizeof(uint64_t));
            if (dc[i].ha == NULL || dc[i].hb == NULL)
                rc = TA_DATA_ENOMEM;
        }
    }
    if (rc == 0)
        rc = dist_scan(c, c->a, 0, dc, 2, NULL, NULL, err);
    if (rc == 0)
        rc = dist_scan(c, c->b, 1, dc, 2, NULL, NULL, err);
    for (i = 0; rc == 0 && i < c->np; i++)
    {
        const char *name = c->a->schema.cols[c->pa[i]].name;
        td_buf l = TD_BUF_INIT;
        double stat = 0;
        double err_bound = 0;

        if (dc[i].numeric)
        {
            uint64_t cum_a = 0;
            uint64_t cum_b = 0;
            size_t k;

            for (k = 0; k < DIST_BINS; k++)
            {
                double fa;
                double fb;

                cum_a += dc[i].ha[k];
                cum_b += dc[i].hb[k];
                fa = dc[i].na == 0 ? 0 : (double)cum_a / (double)dc[i].na;
                fb = dc[i].nb == 0 ? 0 : (double)cum_b / (double)dc[i].nb;
                if (fabs(fa - fb) > stat)
                    stat = fabs(fa - fb);
                /* Within a bin the two CDFs may cross: at most the
                 * larger share either side puts into one bin. */
                if (dc[i].na > 0 &&
                    (double)dc[i].ha[k] / (double)dc[i].na > err_bound)
                    err_bound = (double)dc[i].ha[k] / (double)dc[i].na;
                if (dc[i].nb > 0 &&
                    (double)dc[i].hb[k] / (double)dc[i].nb > err_bound)
                    err_bound = (double)dc[i].hb[k] / (double)dc[i].nb;
            }
            td_esc(&l, name, strlen(name));
            td_buf_adds(&l, "\tks\t");
            td_fmt_double(&l, stat);
            td_buf_printf(&l, "\thistogram-%d\t", DIST_BINS);
            td_fmt_double(&l, err_bound);
        }
        else
        {
            stat = dc[i].tvd;
            td_esc(&l, name, strlen(name));
            td_buf_adds(&l, "\ttvd\t");
            td_fmt_double(&l, stat);
            td_buf_adds(&l, "\texact\t0");
        }
        td_buf_printf(&l, "\t%llu\t%llu", (unsigned long long)dc[i].na,
                      (unsigned long long)dc[i].nb);
        td_report_add(&c->rep, 0, i, 0, l.ptr);
        td_buf_free(&l);
        if (stat > worst)
        {
            worst = stat;
            worst_col = name;
        }
    }
    if (rc == 0)
    {
        td_buf_adds(res, "max_stat=");
        td_fmt_double(res, worst);
        td_buf_adds(res, "\nmax_stat_column=");
        td_esc(res, worst_col, strlen(worst_col));
        td_buf_addc(res, '\n');
    }
    if (g != NULL)
        c->partitions = td_group_partitions(g);
    td_group_close(g);
    for (i = 0; i < c->np; i++)
    {
        free(dc[i].ha);
        free(dc[i].hb);
    }
    free(dc);
    return rc;
}

/* ---- entry -------------------------------------------------------- */

int
ta_data_compare(ta_data_ds *a, ta_data_ds *b, const char *params,
                char **result, char **report, char **err)
{
    cmp c;
    td_scan sa;
    td_scan sb;
    td_buf res = TD_BUF_INIT;
    const char *mode;
    bool have_scans = false;
    size_t i;
    int rc;

    memset(&c, 0, sizeof(c));
    memset(&sa, 0, sizeof(sa));
    memset(&sb, 0, sizeof(sb));
    c.a = a;
    c.b = b;
    rc = td_kv_parse(&c.opts, params);
    if (rc != 0)
        return rc;
    c.mem.limit = td_opts_mem(&c.opts);
    c.tol.abs_tol = td_kv_dbl(&c.opts, "abs_tol", 0);
    c.tol.rel_tol = td_kv_dbl(&c.opts, "rel_tol", 0);
    c.tol.ts_tol_us = td_kv_int(&c.opts, "ts_tol_us", 0);
    c.tol.null_eq = td_kv_bool(&c.opts, "null_eq", true);
    c.tol.trim = td_kv_bool(&c.opts, "trim", false);
    c.tol.nocase = td_kv_bool(&c.opts, "nocase", false);
    c.multiset_dups = strcasecmp(td_kv_get(&c.opts, "dups", "first"),
                                 "multiset") == 0;
    c.round_digits = (int)td_kv_int(&c.opts, "round", 0);
    td_report_init(&c.rep, (size_t)td_kv_int(&c.opts, "max_report", 100));
    mode = td_kv_get(&c.opts, "mode", td_kv_get(&c.opts, "keys", NULL) !=
                     NULL ? "key" : "exact");

    rc = pair_columns(&c, true, err);
    if (rc != 0)
        goto out;

    if (strcasecmp(mode, "schema") == 0)
    {
        td_buf_printf(&res, "mode=schema\n");
    }
    else if (strcasecmp(mode, "distribution") == 0)
    {
        td_buf_printf(&res, "mode=distribution\n");
        rc = cmp_distribution(&c, &res, err);
    }
    else
    {
        td_kv oa;
        td_kv ob;

        /* Each side scans with its own dataset options plus the call's. */
        rc = td_opts(a, params, &oa);
        if (rc == 0)
            rc = td_opts(b, params, &ob);
        if (rc == 0)
            rc = td_scan_open(&sa, a, &oa, err);
        if (rc == 0)
            rc = td_scan_open(&sb, b, &ob, err);
        td_kv_free(&oa);
        td_kv_free(&ob);
        if (rc != 0)
            goto out;
        have_scans = true;
        if (strcasecmp(mode, "exact") == 0)
            rc = cmp_exact(&c, &sa, &sb, err);
        else if (strcasecmp(mode, "unordered") == 0)
            rc = cmp_partitioned(&c, &sa, &sb, false, err);
        else if (strcasecmp(mode, "key") == 0)
            rc = cmp_partitioned(&c, &sa, &sb, true, err);
        else
            rc = td_fail(err, TA_DATA_EINVAL, "unknown compare mode '%s'",
                         mode);
        c.rows_a = sa.rows;
        c.rows_b = sb.rows;
        td_buf_printf(&res, "mode=%s\n", mode);
    }
    if (rc != 0)
        goto out;

    td_buf_printf(&res, "rows_a=%llu\nrows_b=%llu\nmatched=%llu\n"
                  "missing=%llu\nextra=%llu\nmismatched=%llu\n"
                  "dup_a=%llu\ndup_b=%llu\ncolumns_compared=%zu\n"
                  "missing_columns=%llu\nextra_columns=%llu\n"
                  "type_diffs=%llu\npartitions=%u\nmem_peak=%llu\n"
                  "mem_limit=%llu\n",
                  (unsigned long long)c.rows_a, (unsigned long long)c.rows_b,
                  (unsigned long long)c.matched, (unsigned long long)c.missing,
                  (unsigned long long)c.extra,
                  (unsigned long long)c.mismatched,
                  (unsigned long long)c.dup_a, (unsigned long long)c.dup_b,
                  c.np, (unsigned long long)c.col_missing,
                  (unsigned long long)c.col_extra,
                  (unsigned long long)c.type_diffs, c.partitions,
                  (unsigned long long)c.mem.peak,
                  (unsigned long long)c.mem.limit);
    if (have_scans)
        td_buf_printf(&res, "malformed_a=%llu\nmalformed_b=%llu\n"
                      "mismatched_a=%llu\nmismatched_b=%llu\n",
                      (unsigned long long)sa.malformed,
                      (unsigned long long)sb.malformed,
                      (unsigned long long)sa.mismatched,
                      (unsigned long long)sb.mismatched);
    td_buf_adds(&res, "col_mismatch=");
    for (i = 0; i < c.np; i++)
    {
        const char *n = a->schema.cols[c.pa[i]].name;

        if (i > 0)
            td_buf_addc(&res, '|');
        td_esc(&res, n, strlen(n));
        td_buf_printf(&res, ":%llu", (unsigned long long)c.col_mis[i]);
    }
    td_buf_printf(&res, "\nreport_total=%llu\nreport_digest=%016llx\n"
                  "equal=%d\n", (unsigned long long)c.rep.total,
                  (unsigned long long)c.rep.digest,
                  strcasecmp(mode, "distribution") == 0 ? -1 :
                  strcasecmp(mode, "schema") == 0 ?
                  (c.col_missing == 0 && c.col_extra == 0 &&
                   c.type_diffs == 0) :
                  (c.missing == 0 && c.extra == 0 && c.mismatched == 0 &&
                   c.col_missing == 0 && c.col_extra == 0 &&
                   c.dup_a == 0 && c.dup_b == 0));
    *result = td_buf_take(&res);
    *report = td_report_take(&c.rep);

out:
    if (have_scans)
    {
        td_scan_close(&sa);
        td_scan_close(&sb);
    }
    td_buf_free(&res);
    td_report_free(&c.rep);
    td_kv_free(&c.opts);
    free(c.pa);
    free(c.pb);
    free(c.as_float);
    free(c.ka);
    free(c.kb);
    free(c.kfloat);
    free(c.col_mis);
    return rc;
}
