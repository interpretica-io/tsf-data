/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: data quality checks
 *
 * The checks measure; they do not judge. Each returns what it counted
 * (rows checked, violations and the details of the kind) and a bounded,
 * sorted sample of the offending records. Thresholds - how many
 * violations are acceptable - are applied on the engine by tapi_data,
 * which turns the numbers into a pass/fail and a TE verdict.
 *
 * Kinds: schema, nulls, unique, range, pattern, freshness, distribution,
 * reference, pii. See ta_data_core.h and the README for the options.
 */

#include <ctype.h>
#include <math.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <sys/time.h>

#include "ta_data_int.h"

typedef struct chk {
    ta_data_ds     *ds;
    ta_data_ds     *ref;
    td_kv           opts;
    td_mem          mem;
    td_report       rep;
    td_buf          res;
    uint64_t        checked;
    uint64_t        violations;
} chk;

static void
sample(chk *c, int kind, uint64_t row, const char *column, const char *value,
       const char *reason)
{
    td_buf l = TD_BUF_INIT;

    td_buf_printf(&l, "%llu\t", (unsigned long long)row);
    if (column != NULL)
        td_esc(&l, column, strlen(column));
    td_buf_addc(&l, '\t');
    if (value != NULL)
        td_esc(&l, value, strlen(value) > 200 ? 200 : strlen(value));
    td_buf_addc(&l, '\t');
    if (reason != NULL)
        td_esc(&l, reason, strlen(reason));
    td_report_add(&c->rep, kind, row, 0, l.ptr);
    td_buf_free(&l);
}

static void
val_text(td_buf *b, const ta_data_cell *v)
{
    td_buf_reset(b);
    if (v->null)
        td_buf_adds(b, v->bad ? "<bad>" : "NULL");
    else
        td_cell_text(b, v);
    if (b->ptr == NULL)
        td_buf_adds(b, "");
}

static int
one_column(chk *c, const char *key, size_t *col, char **err)
{
    const char *name = td_kv_get(&c->opts, key, NULL);
    int k;

    if (name == NULL || *name == '\0')
        return td_fail(err, TA_DATA_EINVAL, "the check needs %s=", key);
    k = td_schema_find(&c->ds->schema, name);
    if (k < 0)
        return td_fail(err, TA_DATA_ENOENT, "no column '%s' in '%s'", name,
                       c->ds->path);
    *col = (size_t)k;
    return 0;
}

/* ---- schema ------------------------------------------------------- */

enum { SEV_INFO = 0, SEV_WARN = 1, SEV_ERROR = 2 };
static const char *const sev_name[] = { "info", "warning", "error" };

typedef struct schk {
    chk        *c;
    uint64_t    findings[3];
} schk;

static void
finding(schk *s, int sev, const char *code, const char *column,
        const char *detail)
{
    td_buf l = TD_BUF_INIT;

    s->findings[sev]++;
    td_buf_printf(&l, "%s\t%s\t", sev_name[sev], code);
    if (column != NULL)
        td_esc(&l, column, strlen(column));
    td_buf_addc(&l, '\t');
    if (detail != NULL)
        td_esc(&l, detail, strlen(detail));
    /* Structural findings sort before data samples (kind 0 < 1). */
    td_report_add(&s->c->rep, 0, s->findings[0] + s->findings[1] +
                  s->findings[2], 0, l.ptr);
    td_buf_free(&l);
}

/* Is a value of kind @p from always representable as @p to? */
static bool
widens(const td_type *from, const td_type *to)
{
    if (from->kind == to->kind)
    {
        if (from->kind == TA_DATA_T_DECIMAL)
            return to->scale >= from->scale &&
                   to->precision - to->scale >= from->precision - from->scale;
        return true;
    }
    switch (to->kind)
    {
        case TA_DATA_T_FLOAT:
            return from->kind == TA_DATA_T_INT ||
                   from->kind == TA_DATA_T_DECIMAL;
        case TA_DATA_T_DECIMAL:
            return from->kind == TA_DATA_T_INT &&
                   to->precision - to->scale >= 18;
        case TA_DATA_T_TIMESTAMP:
            return from->kind == TA_DATA_T_DATE;
        case TA_DATA_T_INT:
            return from->kind == TA_DATA_T_BOOL;
        default:
            return false;
    }
}

/* The same type up to nested nullability markers ('!')? */
static bool
type_shape_equal(const td_type *a, const td_type *b)
{
    td_buf x = TD_BUF_INIT;
    td_buf y = TD_BUF_INIT;
    size_t i;
    size_t k;
    bool eq;

    td_type_format(&x, a);
    td_type_format(&y, b);
    for (i = k = 0; i < x.len; i++)
        if (x.ptr[i] != '!')
            x.ptr[k++] = x.ptr[i];
    x.len = k;
    for (i = k = 0; i < y.len; i++)
        if (y.ptr[i] != '!')
            y.ptr[k++] = y.ptr[i];
    y.len = k;
    eq = x.len == y.len && memcmp(x.ptr, y.ptr, x.len) == 0;
    td_buf_free(&x);
    td_buf_free(&y);
    return eq;
}

static bool
src_has(const ta_data_ds *ds, const char *name)
{
    size_t i;

    for (i = 0; i < ds->nsrc; i++)
    {
        if (strcmp(ds->src_names[i], name) == 0)
            return true;
    }
    return td_schema_find(&ds->schema, name) >= 0;
}

static int
check_schema(chk *c, char **err)
{
    schk s;
    td_schema exp;
    const char *text = td_kv_get(&c->opts, "schema", NULL);
    bool compatible = strcasecmp(td_kv_get(&c->opts, "mode", "strict"),
                                 "compatible") == 0;
    bool allow_extra = td_kv_bool(&c->opts, "allow_extra", compatible);
    bool data = td_kv_bool(&c->opts, "data", true);
    /* A CSV, or JSON whose schema was inferred, has no declared types:
     * only the data can say whether it fits. */
    bool typed = c->ds->format == TD_FMT_PARQUET ||
                 c->ds->format == TD_FMT_SQLITE || c->ds->schema.explicit_;
    td_buf tmp = TD_BUF_INIT;
    td_buf view_schema = TD_BUF_INIT;
    uint64_t rows_bad = 0;
    uint64_t coerced = 0;
    size_t i;
    int rc;

    memset(&s, 0, sizeof(s));
    s.c = c;
    if (text == NULL)
        return td_fail(err, TA_DATA_EINVAL, "check schema needs schema=");
    rc = td_schema_parse(text, &exp, err);
    if (rc != 0)
        return rc;

    /* Structure: expected columns against the dataset's. */
    for (i = 0; i < exp.n; i++)
    {
        const td_col *e = &exp.cols[i];
        int k = td_schema_find(&c->ds->schema, e->name);
        /* The data pass checks values against the expected type, except
         * where the declared type already differs: that is reported
         * once, here, and the values are read in their own type. */
        const td_type *data_type = e->type;

        if (k >= 0 && typed && !type_shape_equal(c->ds->schema.cols[k].type,
                                                 e->type))
            data_type = c->ds->schema.cols[k].type;
        td_buf_printf(&view_schema, "%s%s:", i > 0 ? "\n" : "",
                      e->name);
        td_type_format(&view_schema, data_type);
        if (!e->nullable)
            td_buf_addc(&view_schema, '!');

        if (k < 0 && !src_has(c->ds, e->name))
        {
            if (compatible && e->nullable)
                finding(&s, SEV_WARN, "missing_optional_column", e->name,
                        "nullable column absent; reads as NULL");
            else
                finding(&s, SEV_ERROR, "missing_column", e->name,
                        e->nullable ? "column absent" :
                        "required column absent");
            continue;
        }
        if (k < 0 || !typed)
            continue;
        {
            const td_col *a = &c->ds->schema.cols[k];

            td_buf_reset(&tmp);
            td_buf_adds(&tmp, "expected ");
            td_type_format(&tmp, e->type);
            td_buf_adds(&tmp, ", found ");
            td_type_format(&tmp, a->type);
            if (type_shape_equal(a->type, e->type))
                ;   /* nested nullability: the data pass decides */
            else if (widens(e->type, a->type) && compatible)
                finding(&s, SEV_INFO, "widened", e->name, tmp.ptr);
            else if (widens(e->type, a->type))
                finding(&s, SEV_ERROR, "type_changed", e->name, tmp.ptr);
            else if (widens(a->type, e->type))
                finding(&s, compatible ? SEV_INFO : SEV_ERROR,
                        compatible ? "narrower_type" : "type_changed",
                        e->name, tmp.ptr);
            else
                finding(&s, SEV_ERROR, "type_mismatch", e->name, tmp.ptr);
            if (!e->nullable && a->nullable)
                finding(&s, compatible ? SEV_WARN : SEV_ERROR,
                        "nullability_relaxed", e->name,
                        "expected required, declared nullable");
        }
    }
    for (i = 0; i < c->ds->nsrc; i++)
    {
        const char *n = c->ds->src_names[i];

        if (td_schema_find(&exp, n) >= 0)
            continue;
        {
            int k = td_schema_find(&c->ds->schema, n);
            /* An inferred schema declares no nullability: what a sample
             * happened not to contain says nothing about the contract. */
            bool nullable = k < 0 || !typed || c->ds->schema.cols[k].nullable;

            if (allow_extra && (nullable || !compatible))
                finding(&s, SEV_INFO, "added_column", n, "not in the "
                        "expected schema");
            else
                finding(&s, SEV_ERROR, "unexpected_column", n,
                        compatible ? "added column is required" :
                        "not in the expected schema");
        }
    }

    /* Data: every value against the expected type and nullability. */
    if (data)
    {
        ta_data_ds *view = NULL;
        td_kv vo;
        td_scan sc;
        char *verr = NULL;
        uint64_t *nulls = calloc(exp.n + 1, sizeof(uint64_t));

        rc = td_opts(c->ds, NULL, &vo);
        if (rc == 0)
            rc = td_kv_parse(&vo, "on_error=count");
        if (rc == 0 && td_kv_bool(&c->opts, "coerce", false))
            rc = td_kv_parse(&vo, "coerce=1");
        if (rc == 0)
        {
            td_buf params = TD_BUF_INIT;

            for (i = 0; i < vo.n; i++)
                td_buf_printf(&params, "%s=%s\n", vo.key[i], vo.val[i]);
            rc = ta_data_ds_open(c->ds->path, td_format_name(c->ds->format),
                                 params.ptr, view_schema.ptr, &view, &verr);
            td_buf_free(&params);
        }
        if (rc == 0)
            rc = td_scan_open(&sc, view, &vo, &verr);
        if (rc == 0)
        {
            while ((rc = td_scan_next(&sc, &verr)) == 0)
            {
                bool bad = false;

                for (i = 0; i < exp.n; i++)
                {
                    const ta_data_cell *v = &sc.row.cells[i];

                    if (v->null && !v->bad && !exp.cols[i].nullable &&
                        src_has(c->ds, exp.cols[i].name))
                    {
                        nulls[i]++;
                        bad = true;
                        sample(c, 1, sc.row.rowno, exp.cols[i].name, "NULL",
                               "NULL in a required column");
                    }
                    else if (v->bad)
                    {
                        bad = true;
                        td_buf_reset(&tmp);
                        td_buf_adds(&tmp, "value is not ");
                        td_type_format(&tmp, exp.cols[i].type);
                        sample(c, 1, sc.row.rowno, exp.cols[i].name, "<bad>",
                               tmp.ptr);
                    }
                }
                if (bad)
                    rows_bad++;
            }
            if (rc == 1)
                rc = 0;
            c->checked = sc.rows;
            coerced = sc.coerced;
            for (i = 0; rc == 0 && i < exp.n; i++)
            {
                if (sc.col_bad[i] > 0)
                {
                    td_buf_reset(&tmp);
                    td_buf_printf(&tmp, "%llu values do not fit ",
                                  (unsigned long long)sc.col_bad[i]);
                    td_type_format(&tmp, exp.cols[i].type);
                    finding(&s, SEV_ERROR, "bad_values", exp.cols[i].name,
                            tmp.ptr);
                }
                if (nulls[i] > 0)
                {
                    td_buf_reset(&tmp);
                    td_buf_printf(&tmp, "%llu NULLs in a required column",
                                  (unsigned long long)nulls[i]);
                    finding(&s, SEV_ERROR, "null_in_required", exp.cols[i].name,
                            tmp.ptr);
                }
            }
            if (rc == 0 && sc.malformed > 0)
            {
                td_buf_reset(&tmp);
                td_buf_printf(&tmp, "%llu malformed records",
                              (unsigned long long)sc.malformed);
                finding(&s, SEV_ERROR, "malformed_records", NULL, tmp.ptr);
            }
            if (rc == 0 && sc.rd != NULL && sc.rd->unexpected > 0 &&
                !allow_extra)
            {
                /* JSON: keys outside the expected schema, seen in data. */
                char **names;
                size_t nn = td_split(sc.rd->unexpected_names, ',', &names);
                size_t k;

                for (k = 0; k < nn; k++)
                {
                    if (!src_has(c->ds, names[k]))
                        finding(&s, SEV_ERROR, "unexpected_column", names[k],
                                "seen in records, not in the expected "
                                "schema");
                }
                td_split_free(names, nn);
            }
            if (coerced > 0)
            {
                td_buf_reset(&tmp);
                td_buf_printf(&tmp, "%llu values were coerced (coerce=1)",
                              (unsigned long long)coerced);
                finding(&s, SEV_WARN, "coerced", NULL, tmp.ptr);
            }
            td_scan_close(&sc);
        }
        if (rc != 0)
            rc = td_fail(err, rc, "%s", verr == NULL ? "schema scan failed" :
                         verr);
        free(verr);
        free(nulls);
        td_kv_free(&vo);
        ta_data_ds_free(view);
    }
    td_buf_free(&tmp);
    td_buf_free(&view_schema);
    td_schema_free(&exp);
    if (rc != 0)
        return rc;
    c->violations = s.findings[SEV_ERROR];
    td_buf_printf(&c->res, "errors=%llu\nwarnings=%llu\ninfos=%llu\n"
                  "rows_bad=%llu\ncoerced=%llu\nmode=%s\n",
                  (unsigned long long)s.findings[SEV_ERROR],
                  (unsigned long long)s.findings[SEV_WARN],
                  (unsigned long long)s.findings[SEV_INFO],
                  (unsigned long long)rows_bad, (unsigned long long)coerced,
                  compatible ? "compatible" : "strict");
    return 0;
}

/* ---- per-row checks: nulls, range, pattern ------------------------ */

typedef struct builtin_pat {
    const char *name;
    const char *re;
} builtin_pat;

static const builtin_pat builtin_patterns[] = {
    { "email", "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$" },
    { "uuid", "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
              "[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" },
    { "ipv4", "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}"
              "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$" },
    { "date", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
    { "digits", "^[0-9]+$" },
    { "alnum", "^[A-Za-z0-9]+$" },
};

static int
check_rows(chk *c, const char *kind, char **err)
{
    td_scan sc;
    td_buf v = TD_BUF_INIT;
    size_t col;
    ta_data_cell lo;
    ta_data_cell hi;
    bool have_lo = false;
    bool have_hi = false;
    regex_t re;
    bool have_re = false;
    bool invert = td_kv_bool(&c->opts, "invert", false);
    uint64_t nulls = 0;
    uint64_t bad = 0;
    uint64_t below = 0;
    uint64_t above = 0;
    int rc;

    memset(&lo, 0, sizeof(lo));
    memset(&hi, 0, sizeof(hi));
    lo.null = hi.null = 1;
    rc = one_column(c, "column", &col, err);
    if (rc != 0)
        return rc;
    if (strcmp(kind, "range") == 0)
    {
        const td_type *t = c->ds->schema.cols[col].type;
        const char *smin = td_kv_get(&c->opts, "min", NULL);
        const char *smax = td_kv_get(&c->opts, "max", NULL);

        if (smin != NULL && *smin != '\0')
        {
            if (td_parse_text(smin, strlen(smin), t, &lo) != 0)
                return td_fail(err, TA_DATA_EINVAL, "min '%s' is not a "
                               "value of column '%s'", smin,
                               c->ds->schema.cols[col].name);
            have_lo = true;
        }
        if (smax != NULL && *smax != '\0')
        {
            if (td_parse_text(smax, strlen(smax), t, &hi) != 0)
            {
                ta_data_cell_clear(&lo);
                return td_fail(err, TA_DATA_EINVAL, "max '%s' is not a "
                               "value of column '%s'", smax,
                               c->ds->schema.cols[col].name);
            }
            have_hi = true;
        }
        if (!have_lo && !have_hi)
            return td_fail(err, TA_DATA_EINVAL, "check range needs min= "
                           "and/or max=");
    }
    else if (strcmp(kind, "pattern") == 0)
    {
        const char *name = td_kv_get(&c->opts, "pattern", NULL);
        const char *rx = td_kv_get(&c->opts, "regex", NULL);
        td_buf anch = TD_BUF_INIT;
        size_t i;
        int rr;

        if (rx == NULL && name != NULL)
        {
            for (i = 0; i < sizeof(builtin_patterns) /
                            sizeof(builtin_patterns[0]); i++)
            {
                if (strcasecmp(builtin_patterns[i].name, name) == 0)
                    rx = builtin_patterns[i].re;
            }
            if (rx == NULL)
                return td_fail(err, TA_DATA_EINVAL, "unknown pattern '%s'",
                               name);
        }
        if (rx == NULL)
            return td_fail(err, TA_DATA_EINVAL, "check pattern needs regex= "
                           "or pattern=");
        /* A full match unless the expression anchors itself. */
        if (rx[0] != '^')
            td_buf_addc(&anch, '^');
        td_buf_adds(&anch, rx);
        if (rx[strlen(rx) - 1] != '$')
            td_buf_addc(&anch, '$');
        rr = regcomp(&re, anch.ptr, REG_EXTENDED | REG_NOSUB);
        td_buf_free(&anch);
        if (rr != 0)
            return td_fail(err, TA_DATA_EINVAL, "bad regular expression "
                           "'%s'", rx);
        have_re = true;
    }

    rc = td_scan_open(&sc, c->ds, &c->opts, err);
    if (rc != 0)
        goto out;
    while ((rc = td_scan_next(&sc, err)) == 0)
    {
        const ta_data_cell *x = &sc.row.cells[col];
        const char *why = NULL;

        if (x->bad)
        {
            bad++;
            why = "value does not fit the column type";
        }
        else if (x->null)
        {
            nulls++;
            if (strcmp(kind, "nulls") == 0)
                why = "NULL";
        }
        else if (strcmp(kind, "range") == 0)
        {
            if (have_lo && td_cell_order(x, &lo) < 0)
            {
                below++;
                why = "below min";
            }
            else if (have_hi && td_cell_order(x, &hi) > 0)
            {
                above++;
                why = "above max";
            }
        }
        else if (have_re)
        {
            bool m;

            td_buf_reset(&v);
            td_cell_text(&v, x);
            m = regexec(&re, v.ptr == NULL ? "" : v.ptr, 0, NULL, 0) == 0;
            if (m == invert)
                why = invert ? "matches the forbidden pattern" :
                               "does not match";
        }
        if (why != NULL)
        {
            c->violations++;
            val_text(&v, x);
            sample(c, 1, sc.row.rowno, c->ds->schema.cols[col].name, v.ptr,
                   why);
        }
    }
    if (rc == 1)
        rc = 0;
    c->checked = sc.rows;
    td_scan_close(&sc);
    if (rc == 0)
    {
        td_buf_printf(&c->res, "nulls=%llu\nbad=%llu\n",
                      (unsigned long long)nulls, (unsigned long long)bad);
        if (strcmp(kind, "range") == 0)
            td_buf_printf(&c->res, "below=%llu\nabove=%llu\n",
                          (unsigned long long)below,
                          (unsigned long long)above);
        if (strcmp(kind, "nulls") == 0)
        {
            td_buf_adds(&c->res, "null_fraction=");
            td_fmt_double(&c->res, c->checked == 0 ? 0 :
                          (double)nulls / (double)c->checked);
            td_buf_addc(&c->res, '\n');
        }
    }
out:
    ta_data_cell_clear(&lo);
    ta_data_cell_clear(&hi);
    if (have_re)
        regfree(&re);
    td_buf_free(&v);
    return rc;
}

/* ---- keyed checks: unique, reference, duplicate ids --------------- */

typedef struct kctx {
    chk        *c;
    const char *what;
    size_t      nkey;
    uint64_t    distinct;
    uint64_t    dup_rows;
    uint64_t    dup_keys;
    uint64_t    orphans;
    uint64_t    orphan_values;
    int64_t     kmin;
    int64_t     kmax;
    bool        kseen;
} kctx;

static void
key_str(const char *key, size_t klen, size_t n, td_buf *out)
{
    const char *p = key;
    const char *end = key + klen;
    ta_data_cell v;
    td_buf t = TD_BUF_INIT;
    size_t i;

    memset(&v, 0, sizeof(v));
    td_buf_reset(out);
    for (i = 0; i < n; i++)
    {
        if (td_cell_deser(&p, end, &v) != 0)
            break;
        if (i > 0)
            td_buf_addc(out, ',');
        val_text(&t, &v);
        td_buf_add(out, t.ptr, t.len);
    }
    ta_data_cell_clear(&v);
    td_buf_free(&t);
    if (out->ptr == NULL)
        td_buf_adds(out, "");
}

static int
unique_cb(void *ctx, const char *key, size_t klen, const td_rec *recs,
          size_t n, char **err)
{
    kctx *k = ctx;
    td_buf t = TD_BUF_INIT;
    size_t i;

    (void)err;
    k->distinct++;
    if (n > 1)
    {
        k->dup_keys++;
        key_str(key, klen, k->nkey, &t);
        for (i = 1; i < n; i++)
        {
            td_buf r = TD_BUF_INIT;

            td_buf_printf(&r, "duplicate of record %llu",
                          (unsigned long long)recs[0].rowno);
            k->dup_rows++;
            sample(k->c, 1, recs[i].rowno, k->what, t.ptr, r.ptr);
            td_buf_free(&r);
        }
    }
    /* Integer keys: track the range for sequence-gap accounting. */
    if (k->nkey == 1 && klen == 9 && (unsigned char)key[0] == TA_DATA_T_INT)
    {
        const char *p = key;
        ta_data_cell v;

        memset(&v, 0, sizeof(v));
        if (td_cell_deser(&p, key + klen, &v) == 0)
        {
            if (!k->kseen || v.i < k->kmin)
                k->kmin = v.i;
            if (!k->kseen || v.i > k->kmax)
                k->kmax = v.i;
            k->kseen = true;
        }
    }
    td_buf_free(&t);
    return 0;
}

static int
reference_cb(void *ctx, const char *key, size_t klen, const td_rec *recs,
             size_t n, char **err)
{
    kctx *k = ctx;
    bool in_ref = false;
    size_t i;
    uint64_t here = 0;

    (void)err;
    for (i = 0; i < n; i++)
    {
        if (recs[i].side == 1)
            in_ref = true;
        else
            here++;
    }
    if (!in_ref && here > 0)
    {
        td_buf t = TD_BUF_INIT;

        k->orphan_values++;
        k->orphans += here;
        key_str(key, klen, k->nkey, &t);
        for (i = 0; i < n; i++)
        {
            if (recs[i].side == 0)
                sample(k->c, 1, recs[i].rowno, k->what, t.ptr,
                       "no matching record in the reference");
        }
        td_buf_free(&t);
    }
    return 0;
}

/* Feed a dataset's key columns into a group; NULL keys are skipped. */
static int
feed_keys(chk *c, td_group *g, ta_data_ds *ds, const td_kv *opts,
          const size_t *idx, size_t n, int side, uint64_t *rows,
          uint64_t *null_keys, char **err)
{
    td_scan sc;
    td_buf key = TD_BUF_INIT;
    bool fold = td_kv_bool(&c->opts, "numeric_fold", true);
    size_t i;
    int rc = td_scan_open(&sc, ds, opts, err);

    if (rc != 0)
        return rc;
    while ((rc = td_scan_next(&sc, err)) == 0)
    {
        bool has_null = false;

        td_buf_reset(&key);
        for (i = 0; i < n; i++)
        {
            const ta_data_cell *v = &sc.row.cells[idx[i]];

            has_null = has_null || v->null;
            td_cell_ser(&key, v, fold && td_kind_numeric(v->type) &&
                        v->type != TA_DATA_T_INT);
        }
        if (has_null)
        {
            (*null_keys)++;
            continue;
        }
        rc = td_group_add(g, side, sc.row.rowno, key.ptr, key.len, "", 0,
                          err);
        if (rc != 0)
            break;
    }
    if (rc == 1)
        rc = 0;
    *rows = sc.rows;
    td_buf_free(&key);
    td_scan_close(&sc);
    return rc;
}

static int
check_keyed(chk *c, const char *kind, char **err)
{
    bool is_ref = strcmp(kind, "reference") == 0;
    const char *list = td_kv_get(&c->opts, is_ref ? "column" : "columns",
                                 td_kv_get(&c->opts, "column", ""));
    size_t *idx = NULL;
    size_t n = 0;
    size_t *ridx = NULL;
    size_t rn = 0;
    td_group *g = NULL;
    kctx k;
    uint64_t null_keys = 0;
    uint64_t ref_rows = 0;
    uint64_t ref_null = 0;
    int rc;

    memset(&k, 0, sizeof(k));
    k.c = c;
    k.what = list;
    if (list == NULL || *list == '\0')
        return td_fail(err, TA_DATA_EINVAL, "check %s needs %s=", kind,
                       is_ref ? "column" : "columns");
    rc = td_cols_resolve(c->ds, list, &idx, &n, err);
    if (rc != 0)
        return rc;
    k.nkey = n;
    if (is_ref)
    {
        if (c->ref == NULL)
        {
            free(idx);
            return td_fail(err, TA_DATA_EINVAL, "check reference needs a "
                           "reference dataset");
        }
        rc = td_cols_resolve(c->ref, td_kv_get(&c->opts, "ref_column", list),
                             &ridx, &rn, err);
        if (rc == 0 && rn != n)
            rc = td_fail(err, TA_DATA_EINVAL, "%zu key columns against %zu "
                         "reference columns", n, rn);
    }
    if (rc == 0)
        rc = td_group_open(&g, &c->mem, td_kv_get(&c->opts, "tmpdir", NULL),
                           c->ds->file_bytes +
                           (c->ref != NULL ? c->ref->file_bytes : 0), err);
    if (rc == 0)
        rc = feed_keys(c, g, c->ds, &c->opts, idx, n, 0, &c->checked,
                       &null_keys, err);
    if (rc == 0 && is_ref)
    {
        td_kv ro;

        rc = td_opts(c->ref, NULL, &ro);
        if (rc == 0)
            rc = feed_keys(c, g, c->ref, &ro, ridx, rn, 1, &ref_rows,
                           &ref_null, err);
        td_kv_free(&ro);
    }
    if (rc == 0)
        rc = td_group_run(g, is_ref ? reference_cb : unique_cb, &k, err);
    if (rc == 0)
    {
        if (is_ref)
        {
            c->violations = k.orphans;
            td_buf_printf(&c->res, "orphans=%llu\norphan_values=%llu\n"
                          "null_keys=%llu\nref_rows=%llu\n",
                          (unsigned long long)k.orphans,
                          (unsigned long long)k.orphan_values,
                          (unsigned long long)null_keys,
                          (unsigned long long)ref_rows);
        }
        else
        {
            c->violations = k.dup_rows;
            td_buf_printf(&c->res, "distinct=%llu\nduplicates=%llu\n"
                          "dup_keys=%llu\nnull_keys=%llu\n",
                          (unsigned long long)k.distinct,
                          (unsigned long long)k.dup_rows,
                          (unsigned long long)k.dup_keys,
                          (unsigned long long)null_keys);
            if (k.kseen)
            {
                uint64_t span = (uint64_t)(k.kmax - k.kmin) + 1;

                td_buf_printf(&c->res, "key_min=%lld\nkey_max=%lld\n"
                              "key_missing=%llu\n", (long long)k.kmin,
                              (long long)k.kmax,
                              (unsigned long long)(span > k.distinct ?
                                                   span - k.distinct : 0));
            }
        }
        td_buf_printf(&c->res, "partitions=%u\nmem_peak=%llu\n",
                      td_group_partitions(g),
                      (unsigned long long)c->mem.peak);
    }
    td_group_close(g);
    free(idx);
    free(ridx);
    return rc;
}

/* ---- freshness ---------------------------------------------------- */

/* A unique check over one column of the same dataset, sharing the
 * report and the budget; *missing gets the integer key-range gaps. */
static int
sub_unique(chk *c, const char *column, uint64_t *dups, uint64_t *missing,
           char **err)
{
    chk sub = *c;
    size_t i;
    int rc;

    memset(&sub.res, 0, sizeof(sub.res));
    memset(&sub.opts, 0, sizeof(sub.opts));
    for (i = 0; i < c->opts.n; i++)
        td_kv_set(&sub.opts, c->opts.key[i], c->opts.val[i]);
    td_kv_set(&sub.opts, "columns", column);
    sub.violations = 0;
    sub.checked = 0;
    rc = check_keyed(&sub, "unique", err);
    if (rc == 0)
    {
        td_kv got = TD_KV_INIT;

        *dups = sub.violations;
        td_kv_parse(&got, sub.res.ptr);
        if (missing != NULL)
            *missing = (uint64_t)td_kv_int(&got, "key_missing", 0);
        td_kv_free(&got);
    }
    /* The samples went into the shared report. */
    c->rep = sub.rep;
    if (sub.mem.peak > c->mem.peak)
        c->mem.peak = sub.mem.peak;
    td_kv_free(&sub.opts);
    td_buf_free(&sub.res);
    return rc;
}

static int64_t
now_us(const td_kv *opts)
{
    const char *now = td_kv_get(opts, "now", NULL);
    int64_t v;
    struct timeval tv;

    if (now != NULL && td_parse_timestamp(now, strlen(now), &v) == 0)
        return v;
    v = td_kv_int(opts, "now_us", 0);
    if (v != 0)
        return v;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static int
ts_of(const ta_data_cell *v, int64_t *us)
{
    if (v->null)
        return -1;
    if (v->type == TA_DATA_T_TIMESTAMP)
        *us = v->i;
    else if (v->type == TA_DATA_T_DATE)
        *us = v->i * 86400LL * 1000000LL;
    else if (v->type == TA_DATA_T_INT)
        *us = v->i;     /* epoch microseconds */
    else
        return -1;
    return 0;
}

static void
put_ts(td_buf *b, const char *key, int64_t us)
{
    td_buf_printf(b, "%s=", key);
    td_fmt_timestamp(b, us);
    td_buf_addc(b, '\n');
}

static int
check_freshness(chk *c, char **err)
{
    size_t ev;
    int ing = -1;
    int idc = -1;
    int sqc = -1;
    int64_t skew = td_kv_int(&c->opts, "skew_us", 0);
    int64_t max_age = td_kv_int(&c->opts, "max_age_us", -1);
    int64_t max_delay = td_kv_int(&c->opts, "max_delay_us", -1);
    int64_t interval = td_kv_int(&c->opts, "interval_us", 0);
    int64_t now = now_us(&c->opts);
    td_scan sc;
    int64_t latest = INT64_MIN;
    int64_t earliest = INT64_MAX;
    int64_t prev = INT64_MIN;
    int64_t worst_delay = INT64_MIN;
    int64_t prev_seq = 0;
    bool have_seq = false;
    uint64_t delays_bad = 0;
    uint64_t negative = 0;
    uint64_t ooo = 0;
    uint64_t seq_missing = 0;
    uint64_t seq_back = 0;
    uint64_t gaps = 0;
    uint64_t gap_total = 0;
    uint64_t no_ts = 0;
    uint64_t events = 0;
    td_buf t = TD_BUF_INIT;
    td_buf u = TD_BUF_INIT;
    int rc;

    rc = one_column(c, "event", &ev, err);
    if (rc != 0)
        return rc;
#define OPT_COL(_key, _var) \
    do {                                                                \
        const char *n_ = td_kv_get(&c->opts, _key, NULL);               \
        if (n_ != NULL && *n_ != '\0')                                  \
        {                                                               \
            (_var) = td_schema_find(&c->ds->schema, n_);                \
            if ((_var) < 0)                                             \
                return td_fail(err, TA_DATA_ENOENT, "no column '%s'", n_); \
        }                                                               \
    } while (0)
    OPT_COL("ingest", ing);
    OPT_COL("id", idc);
    OPT_COL("seq", sqc);
#undef OPT_COL

    rc = td_scan_open(&sc, c->ds, &c->opts, err);
    if (rc != 0)
        return rc;
    while ((rc = td_scan_next(&sc, err)) == 0)
    {
        int64_t e;
        int64_t in;

        if (ts_of(&sc.row.cells[ev], &e) != 0)
        {
            no_ts++;
            continue;
        }
        events++;
        if (e > latest)
            latest = e;
        if (e < earliest)
            earliest = e;
        if (prev != INT64_MIN && e + skew < prev)
        {
            ooo++;
            td_buf_reset(&t);
            td_fmt_timestamp(&t, e);
            sample(c, 3, sc.row.rowno, c->ds->schema.cols[ev].name, t.ptr,
                   "event time goes back");
        }
        if (e > prev || prev == INT64_MIN)
            prev = e;
        if (ing >= 0 && ts_of(&sc.row.cells[ing], &in) == 0)
        {
            int64_t d = in - e;

            if (d > worst_delay)
                worst_delay = d;
            if (d < -skew)
            {
                negative++;
                sample(c, 4, sc.row.rowno, c->ds->schema.cols[ing].name,
                       NULL, "ingested before the event happened "
                       "(clock skew beyond skew_us)");
            }
            else if (max_delay >= 0 && d > max_delay + skew)
            {
                delays_bad++;
                td_buf_reset(&u);
                td_buf_printf(&u, "ingestion delay %lld us > %lld",
                              (long long)d, (long long)max_delay);
                sample(c, 5, sc.row.rowno, c->ds->schema.cols[ing].name, NULL,
                       u.ptr);
            }
        }
        if (sqc >= 0 && !sc.row.cells[sqc].null &&
            sc.row.cells[sqc].type == TA_DATA_T_INT)
        {
            int64_t s = sc.row.cells[sqc].i;

            if (have_seq && s <= prev_seq)
                seq_back++;
            if (!have_seq || s > prev_seq)
                prev_seq = s;
            have_seq = true;
        }
    }
    if (rc == 1)
        rc = 0;
    c->checked = sc.rows;
    td_scan_close(&sc);
    if (rc != 0)
        goto out;

    if (interval > 0 && events > 0)
    {
        /* Gaps over a bitmap of interval-wide buckets from the earliest
         * event: independent of the order the events arrive in. */
        uint64_t nb = (uint64_t)((latest - earliest) / interval) + 1;
        uint8_t *bits;
        uint64_t i;
        uint64_t run = 0;

        if (!td_mem_take(&c->mem, nb / 8 + 1) ||
            (bits = calloc(nb / 8 + 1, 1)) == NULL)
        {
            rc = td_fail(err, TA_DATA_E2BIG, "gap detection needs %llu "
                         "bytes", (unsigned long long)(nb / 8 + 1));
            goto out;
        }
        rc = td_scan_open(&sc, c->ds, &c->opts, err);
        while (rc == 0 && (rc = td_scan_next(&sc, err)) == 0)
        {
            int64_t e;

            if (ts_of(&sc.row.cells[ev], &e) == 0)
            {
                uint64_t b = (uint64_t)((e - earliest) / interval);

                bits[b / 8] |= (uint8_t)(1u << (b % 8));
            }
        }
        if (rc == 1)
            rc = 0;
        td_scan_close(&sc);
        for (i = 0; rc == 0 && i <= nb; i++)
        {
            bool set = i < nb && (bits[i / 8] & (1u << (i % 8)));

            if (!set && i < nb)
            {
                run++;
                continue;
            }
            if (run > 0)
            {
                int64_t from = earliest + (int64_t)(i - run) * interval;

                gaps++;
                gap_total += run * (uint64_t)interval;
                td_buf_reset(&t);
                td_fmt_timestamp(&t, from);
                td_buf_reset(&u);
                td_buf_printf(&u, "no events for %llu interval(s) from ",
                              (unsigned long long)run);
                td_fmt_timestamp(&u, from);
                sample(c, 2, 0, c->ds->schema.cols[ev].name, t.ptr, u.ptr);
            }
            run = 0;
        }
        free(bits);
        td_mem_give(&c->mem, nb / 8 + 1);
        if (rc != 0)
            goto out;
    }

    td_buf_printf(&c->res, "events=%llu\nno_event_time=%llu\n",
                  (unsigned long long)events, (unsigned long long)no_ts);
    if (events > 0)
    {
        int64_t age = now - latest;

        put_ts(&c->res, "latest_event", latest);
        put_ts(&c->res, "earliest_event", earliest);
        put_ts(&c->res, "now", now);
        td_buf_printf(&c->res, "age_us=%lld\nstale=%d\n", (long long)age,
                      max_age >= 0 && age > max_age + skew);
        if (max_age >= 0 && age > max_age + skew)
            c->violations++;
    }
    if (ing >= 0)
        td_buf_printf(&c->res, "max_delay_us=%lld\ndelay_violations=%llu\n"
                      "negative_delays=%llu\n",
                      (long long)(worst_delay == INT64_MIN ? 0 : worst_delay),
                      (unsigned long long)delays_bad,
                      (unsigned long long)negative);
    td_buf_printf(&c->res, "out_of_order=%llu\ngaps=%llu\ngap_us_total=%llu\n",
                  (unsigned long long)ooo, (unsigned long long)gaps,
                  (unsigned long long)gap_total);
    c->violations += delays_bad + negative + gaps;

    if (rc == 0 && idc >= 0)
    {
        uint64_t dups = 0;

        rc = sub_unique(c, c->ds->schema.cols[idc].name, &dups, NULL, err);
        if (rc == 0)
        {
            td_buf_printf(&c->res, "dup_ids=%llu\n", (unsigned long long)dups);
            c->violations += dups;
        }
    }
    if (rc == 0 && sqc >= 0)
    {
        /* Missing sequence numbers whatever the arrival order: the span
         * of the sequence minus its distinct values. */
        uint64_t dups = 0;

        rc = sub_unique(c, c->ds->schema.cols[sqc].name, &dups,
                        &seq_missing, err);
        if (rc == 0)
        {
            td_buf_printf(&c->res, "seq_missing=%llu\nseq_duplicates=%llu\n"
                          "seq_regressions=%llu\n",
                          (unsigned long long)seq_missing,
                          (unsigned long long)dups,
                          (unsigned long long)seq_back);
            c->violations += seq_missing;
        }
    }
out:
    td_buf_free(&t);
    td_buf_free(&u);
    return rc;
}

/* ---- distribution ------------------------------------------------- */

static int
check_distribution(chk *c, char **err)
{
    size_t col;
    const char *freq = td_kv_get(&c->opts, "freq", NULL);
    td_scan sc;
    char **ent = NULL;
    size_t nent = 0;
    char **names = NULL;
    double *expect = NULL;
    uint64_t *seen = NULL;
    uint64_t other = 0;
    uint64_t n = 0;
    double mean = 0;
    double m2 = 0;
    td_buf v = TD_BUF_INIT;
    size_t i;
    int rc;

    if (c->ref != NULL)
    {
        /* Against a reference dataset: KS or TVD, via compare. */
        char *res = NULL;
        char *rep = NULL;
        td_buf p = TD_BUF_INIT;
        const char *cn = td_kv_get(&c->opts, "column", "");

        td_buf_printf(&p, "mode=distribution\ncolumns=%s\n", cn);
        if (td_kv_get(&c->opts, "ref_column", NULL) != NULL)
            td_buf_printf(&p, "map=%s:%s\n", cn,
                          td_kv_get(&c->opts, "ref_column", NULL));
        rc = ta_data_compare(c->ref, c->ds, p.ptr, &res, &rep, err);
        td_buf_free(&p);
        if (rc == 0)
        {
            td_buf_adds(&c->res, res);
            td_report_add(&c->rep, 0, 0, 0, rep);
        }
        free(res);
        free(rep);
        return rc;
    }
    rc = one_column(c, "column", &col, err);
    if (rc != 0)
        return rc;
    if (freq != NULL)
    {
        nent = td_split(freq, '|', &ent);
        names = calloc(nent + 1, sizeof(char *));
        expect = calloc(nent + 1, sizeof(double));
        seen = calloc(nent + 1, sizeof(uint64_t));
        if (names == NULL || expect == NULL || seen == NULL)
        {
            rc = TA_DATA_ENOMEM;
            goto out;
        }
        for (i = 0; i < nent; i++)
        {
            char *colon = strrchr(ent[i], ':');

            if (colon == NULL)
            {
                rc = td_fail(err, TA_DATA_EINVAL, "freq entry '%s' is not "
                             "value:share", ent[i]);
                goto out;
            }
            names[i] = td_strndup(ent[i], (size_t)(colon - ent[i]));
            expect[i] = strtod(colon + 1, NULL);
        }
    }
    rc = td_scan_open(&sc, c->ds, &c->opts, err);
    if (rc != 0)
        goto out;
    while ((rc = td_scan_next(&sc, err)) == 0)
    {
        const ta_data_cell *x = &sc.row.cells[col];
        double d;

        if (x->null)
            continue;
        n++;
        if (td_cell_num(x, &d))
        {
            double delta = d - mean;

            mean += delta / (double)n;
            m2 += delta * (d - mean);
        }
        if (nent > 0)
        {
            td_buf_reset(&v);
            td_cell_text(&v, x);
            for (i = 0; i < nent; i++)
            {
                if (strcmp(names[i], v.ptr == NULL ? "" : v.ptr) == 0)
                {
                    seen[i]++;
                    break;
                }
            }
            if (i == nent)
                other++;
        }
    }
    if (rc == 1)
        rc = 0;
    c->checked = sc.rows;
    td_scan_close(&sc);
    if (rc != 0)
        goto out;
    td_buf_printf(&c->res, "values=%llu\n", (unsigned long long)n);
    td_buf_adds(&c->res, "mean=");
    td_fmt_double(&c->res, mean);
    td_buf_adds(&c->res, "\nstddev=");
    td_fmt_double(&c->res, n > 1 ? sqrt(m2 / (double)(n - 1)) : 0);
    td_buf_addc(&c->res, '\n');
    if (nent > 0)
    {
        double tvd = 0;
        double maxdev = 0;
        double chi2 = 0;

        for (i = 0; i < nent; i++)
        {
            double obs = n == 0 ? 0 : (double)seen[i] / (double)n;
            double dev = fabs(obs - expect[i]);
            td_buf l = TD_BUF_INIT;

            tvd += dev;
            if (dev > maxdev)
                maxdev = dev;
            if (expect[i] > 0)
                chi2 += ((double)seen[i] - expect[i] * (double)n) *
                        ((double)seen[i] - expect[i] * (double)n) /
                        (expect[i] * (double)n);
            td_esc(&l, names[i], strlen(names[i]));
            td_buf_adds(&l, "\t");
            td_fmt_double(&l, expect[i]);
            td_buf_adds(&l, "\t");
            td_fmt_double(&l, obs);
            td_buf_printf(&l, "\t%llu", (unsigned long long)seen[i]);
            td_report_add(&c->rep, 0, i, 0, l.ptr);
            td_buf_free(&l);
        }
        if (n > 0)
            tvd += (double)other / (double)n;
        tvd *= 0.5;
        td_buf_adds(&c->res, "tvd=");
        td_fmt_double(&c->res, tvd);
        td_buf_adds(&c->res, "\nmax_dev=");
        td_fmt_double(&c->res, maxdev);
        td_buf_adds(&c->res, "\nchi2=");
        td_fmt_double(&c->res, chi2);
        td_buf_printf(&c->res, "\ndof=%zu\nunexpected_values=%llu\n",
                      nent > 0 ? nent - 1 : 0, (unsigned long long)other);
    }
out:
    td_split_free(ent, nent);
    if (names != NULL)
        td_split_free(names, nent);
    free(expect);
    free(seen);
    td_buf_free(&v);
    return rc;
}

/* ---- possible sensitive data -------------------------------------- */

typedef struct pii_det {
    const char *name;
    const char *re;     /* NULL: hand-written detector */
} pii_det;

static const pii_det pii_dets[] = {
    { "email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}" },
    { "phone", "(^|[^0-9])\\+[1-9][0-9 ().-]{7,16}[0-9]([^0-9]|$)" },
    { "ipv4", "(^|[^0-9.])((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.)"
              "{3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])([^0-9.]|$)" },
    { "us_ssn", "(^|[^0-9])[0-8][0-9]{2}-[0-9]{2}-[0-9]{4}([^0-9]|$)" },
    { "jwt", "eyJ[A-Za-z0-9_-]{8,}\\.eyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]+" },
    { "aws_access_key", "(^|[^A-Z0-9])(AKIA|ASIA)[A-Z0-9]{16}([^A-Z0-9]|$)" },
    { "credit_card", NULL },
    { "iban", NULL },
};
#define N_PII (sizeof(pii_dets) / sizeof(pii_dets[0]))

static bool
luhn_ok(const char *d, size_t n)
{
    int sum = 0;
    size_t i;

    for (i = 0; i < n; i++)
    {
        int x = d[n - 1 - i] - '0';

        if (i % 2 == 1)
        {
            x *= 2;
            if (x > 9)
                x -= 9;
        }
        sum += x;
    }
    return sum % 10 == 0;
}

/* 13-19 digits, optionally grouped by spaces or dashes, Luhn-valid. */
static bool
has_card(const char *s)
{
    size_t i = 0;
    size_t len = strlen(s);

    while (i < len)
    {
        char digs[24];
        size_t n = 0;
        size_t j = i;

        if (!isdigit((unsigned char)s[i]) ||
            (i > 0 && isdigit((unsigned char)s[i - 1])))
        {
            i++;
            continue;
        }
        while (j < len && n < 20 &&
               (isdigit((unsigned char)s[j]) ||
                ((s[j] == ' ' || s[j] == '-') && j + 1 < len &&
                 isdigit((unsigned char)s[j + 1]))))
        {
            if (isdigit((unsigned char)s[j]))
                digs[n++] = s[j];
            j++;
        }
        if (n >= 13 && n <= 19 && (j >= len || !isdigit((unsigned char)s[j]))
            && luhn_ok(digs, n))
            return true;
        i = j > i ? j : i + 1;
    }
    return false;
}

static bool
has_iban(const char *s)
{
    size_t len = strlen(s);
    size_t i;

    for (i = 0; i + 15 <= len; i++)
    {
        size_t j;
        size_t n;
        char buf[40];
        unsigned int rem = 0;
        size_t k;

        if (!isupper((unsigned char)s[i]) || !isupper((unsigned char)s[i + 1]) ||
            !isdigit((unsigned char)s[i + 2]) ||
            !isdigit((unsigned char)s[i + 3]) ||
            (i > 0 && isalnum((unsigned char)s[i - 1])))
            continue;
        for (j = i, n = 0; j < len && n < 34 &&
             (isupper((unsigned char)s[j]) || isdigit((unsigned char)s[j]));
             j++)
            buf[n++] = s[j];
        if (n < 15 || n > 34)
            continue;
        /* mod-97 over the rearranged string, letters as 10..35. */
        for (k = 0; k < n; k++)
        {
            char ch = buf[(k + 4) % n];

            if (isdigit((unsigned char)ch))
                rem = (rem * 10 + (unsigned)(ch - '0')) % 97;
            else
                rem = (rem * 100 + (unsigned)(ch - 'A' + 10)) % 97;
        }
        if (rem == 1)
            return true;
    }
    return false;
}

static const char *const pii_names[] = {
    "password", "passwd", "secret", "token", "ssn", "social_security",
    "email", "e_mail", "phone", "mobile", "card", "iban", "dob", "birth",
    "passport", "address", "salary", "api_key", "apikey",
};

static void
mask(const char *s, td_buf *out)
{
    size_t n = strlen(s);
    size_t i;

    td_buf_reset(out);
    for (i = 0; i < n; i++)
        td_buf_addc(out, (i < 2 || i + 2 >= n || n <= 6) &&
                    !(n <= 6 && i > 0) ? s[i] : '*');
}

static int
check_pii(chk *c, char **err)
{
    size_t *idx = NULL;
    size_t n = 0;
    regex_t re[N_PII];
    bool have[N_PII];
    uint64_t *hits = NULL;
    char **first = NULL;
    uint64_t *vals = NULL;
    td_scan sc;
    td_buf v = TD_BUF_INIT;
    td_buf m = TD_BUF_INIT;
    uint64_t findings = 0;
    size_t i;
    size_t d;
    int rc;

    memset(have, 0, sizeof(have));
    rc = td_cols_resolve(c->ds, td_kv_get(&c->opts, "columns", ""), &idx, &n,
                         err);
    if (rc != 0)
        return rc;
    for (d = 0; d < N_PII; d++)
    {
        if (pii_dets[d].re != NULL)
            have[d] = regcomp(&re[d], pii_dets[d].re,
                              REG_EXTENDED | REG_NOSUB) == 0;
    }
    hits = calloc(n * N_PII + 1, sizeof(uint64_t));
    first = calloc(n * N_PII + 1, sizeof(char *));
    vals = calloc(n + 1, sizeof(uint64_t));
    if (hits == NULL || first == NULL || vals == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    rc = td_scan_open(&sc, c->ds, &c->opts, err);
    if (rc != 0)
        goto out;
    while ((rc = td_scan_next(&sc, err)) == 0)
    {
        for (i = 0; i < n; i++)
        {
            const ta_data_cell *x = &sc.row.cells[idx[i]];

            if (x->null || x->type == TA_DATA_T_BOOL ||
                x->type == TA_DATA_T_FLOAT)
                continue;
            td_buf_reset(&v);
            td_cell_text(&v, x);
            if (v.ptr == NULL)
                continue;
            vals[i]++;
            for (d = 0; d < N_PII; d++)
            {
                bool hit;

                if (pii_dets[d].re != NULL)
                    hit = have[d] && regexec(&re[d], v.ptr, 0, NULL, 0) == 0;
                else if (strcmp(pii_dets[d].name, "credit_card") == 0)
                    hit = has_card(v.ptr);
                else
                    hit = has_iban(v.ptr);
                if (!hit)
                    continue;
                if (hits[i * N_PII + d]++ == 0)
                {
                    mask(v.ptr, &m);
                    first[i * N_PII + d] = td_buf_take(&m);
                }
            }
        }
    }
    if (rc == 1)
        rc = 0;
    c->checked = sc.rows;
    td_scan_close(&sc);
    if (rc != 0)
        goto out;
    for (i = 0; i < n; i++)
    {
        const char *cn = c->ds->schema.cols[idx[i]].name;
        size_t k;

        for (d = 0; d < N_PII; d++)
        {
            td_buf l = TD_BUF_INIT;

            if (hits[i * N_PII + d] == 0)
                continue;
            findings++;
            td_esc(&l, cn, strlen(cn));
            td_buf_printf(&l, "\t%s\t%llu\t", pii_dets[d].name,
                          (unsigned long long)hits[i * N_PII + d]);
            td_fmt_double(&l, vals[i] == 0 ? 0 :
                          (double)hits[i * N_PII + d] / (double)vals[i]);
            td_buf_addc(&l, '\t');
            td_esc(&l, first[i * N_PII + d], strlen(first[i * N_PII + d]));
            td_buf_adds(&l, "\tpossible sensitive data (pattern match)");
            td_report_add(&c->rep, 0, i, d, l.ptr);
            td_buf_free(&l);
        }
        for (k = 0; k < sizeof(pii_names) / sizeof(pii_names[0]); k++)
        {
            td_buf lower = TD_BUF_INIT;
            size_t j;

            for (j = 0; cn[j] != '\0'; j++)
                td_buf_addc(&lower, (char)tolower((unsigned char)cn[j]));
            if (strstr(lower.ptr, pii_names[k]) != NULL)
            {
                td_buf l = TD_BUF_INIT;

                findings++;
                td_esc(&l, cn, strlen(cn));
                td_buf_printf(&l, "\tcolumn_name\t0\t0\t%s\tcolumn name "
                              "suggests sensitive data", pii_names[k]);
                td_report_add(&c->rep, 0, i, N_PII + k, l.ptr);
                td_buf_free(&l);
                td_buf_free(&lower);
                break;
            }
            td_buf_free(&lower);
        }
    }
    c->violations = findings;
    td_buf_printf(&c->res, "findings=%llu\n", (unsigned long long)findings);
out:
    for (d = 0; d < N_PII; d++)
    {
        if (have[d])
            regfree(&re[d]);
    }
    for (i = 0; first != NULL && i < n * N_PII; i++)
        free(first[i]);
    free(first);
    free(hits);
    free(vals);
    free(idx);
    td_buf_free(&v);
    td_buf_free(&m);
    return rc;
}

/* ---- entry -------------------------------------------------------- */

int
ta_data_check(ta_data_ds *ds, ta_data_ds *ref, const char *kind,
              const char *params, char **result, char **report, char **err)
{
    chk c;
    int rc;

    memset(&c, 0, sizeof(c));
    c.ds = ds;
    c.ref = ref;
    rc = td_opts(ds, params, &c.opts);
    if (rc != 0)
        return rc;
    c.mem.limit = td_opts_mem(&c.opts);
    td_report_init(&c.rep, (size_t)td_kv_int(&c.opts, "max_report", 20));
    if (kind == NULL)
        kind = "";
    if (strcmp(kind, "schema") == 0)
        rc = check_schema(&c, err);
    else if (strcmp(kind, "nulls") == 0 || strcmp(kind, "range") == 0 ||
             strcmp(kind, "pattern") == 0)
        rc = check_rows(&c, kind, err);
    else if (strcmp(kind, "unique") == 0 || strcmp(kind, "reference") == 0)
        rc = check_keyed(&c, kind, err);
    else if (strcmp(kind, "freshness") == 0)
        rc = check_freshness(&c, err);
    else if (strcmp(kind, "distribution") == 0)
        rc = check_distribution(&c, err);
    else if (strcmp(kind, "pii") == 0)
        rc = check_pii(&c, err);
    else
        rc = td_fail(err, TA_DATA_EINVAL, "unknown check '%s'", kind);
    if (rc == 0)
    {
        td_buf out = TD_BUF_INIT;

        td_buf_printf(&out, "check=%s\nchecked=%llu\nviolations=%llu\n",
                      kind, (unsigned long long)c.checked,
                      (unsigned long long)c.violations);
        td_buf_add(&out, c.res.ptr, c.res.len);
        td_buf_printf(&out, "report_total=%llu\n",
                      (unsigned long long)c.rep.total);
        *result = td_buf_take(&out);
        *report = td_report_take(&c.rep);
    }
    td_buf_free(&c.res);
    td_report_free(&c.rep);
    td_kv_free(&c.opts);
    return rc;
}
