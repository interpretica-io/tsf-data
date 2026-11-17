/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: datasets, typed scans and writers
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>

#include "ta_data_int.h"

/* ---- options ------------------------------------------------------ */

int
td_opts(const ta_data_ds *ds, const char *params, td_kv *out)
{
    size_t i;
    int rc = 0;

    memset(out, 0, sizeof(*out));
    if (ds != NULL)
    {
        for (i = 0; i < ds->opts.n && rc == 0; i++)
            rc = td_kv_set(out, ds->opts.key[i], ds->opts.val[i]);
    }
    if (rc == 0)
        rc = td_kv_parse(out, params);
    return rc;
}

uint64_t
td_opts_mem(const td_kv *opts)
{
    int64_t m = td_kv_int(opts, "mem_limit", (int64_t)TD_MEM_DEFAULT);

    return m <= 0 ? 0 : (uint64_t)m;
}

/* ---- datasets ----------------------------------------------------- */

int
ta_data_ds_open(const char *path, const char *format, const char *params,
                const char *schema, ta_data_ds **out, char **err)
{
    ta_data_ds *ds;
    struct stat st;
    int rc;

    *out = NULL;
    if (path == NULL || *path == '\0')
        return td_fail(err, TA_DATA_EINVAL, "no dataset path");
    ds = calloc(1, sizeof(*ds));
    if (ds == NULL)
        return TA_DATA_ENOMEM;
    ds->path = td_strdup(path);
    rc = td_format_parse(format, path, &ds->format);
    if (rc != 0)
    {
        rc = td_fail(err, TA_DATA_EINVAL, "unknown format '%s' for '%s'",
                     format == NULL ? "" : format, path);
        goto fail;
    }
    if (stat(path, &st) != 0)
    {
        rc = td_fail(err, errno == ENOENT ? TA_DATA_ENOENT : TA_DATA_EIO,
                     "cannot open '%s': %s", path, strerror(errno));
        goto fail;
    }
    ds->file_bytes = (uint64_t)st.st_size;
    rc = td_kv_parse(&ds->opts, params);
    if (rc == 0 && schema != NULL && *schema != '\0')
        rc = td_schema_parse(schema, &ds->schema, err);
    if (rc == 0)
        rc = td_infer_schema(ds, err);
    if (rc == 0 && ds->schema.n == 0)
        rc = td_fail(err, TA_DATA_ECORRUPT, "'%s' has no columns", path);
    if (rc != 0)
        goto fail;
    *out = ds;
    return 0;

fail:
    ta_data_ds_free(ds);
    return rc;
}

void
ta_data_ds_free(ta_data_ds *ds)
{
    if (ds == NULL)
        return;
    free(ds->path);
    td_kv_free(&ds->opts);
    td_schema_free(&ds->schema);
    td_split_free(ds->src_names, ds->nsrc);
    free(ds);
}

int
ta_data_ds_schema(ta_data_ds *ds, char **out)
{
    td_buf b = TD_BUF_INIT;
    size_t i;

    for (i = 0; i < ds->schema.n; i++)
    {
        const td_col *c = &ds->schema.cols[i];

        td_buf_printf(&b, "%s\t", c->name);
        td_type_format(&b, c->type);
        td_buf_printf(&b, "\t%d\t%s\n", c->nullable ? 1 : 0,
                      ds->schema.explicit_ ? "explicit" : "inferred");
    }
    *out = td_buf_take(&b);
    return 0;
}

int
td_cols_resolve(const ta_data_ds *ds, const char *list, size_t **idx,
                size_t *n, char **err)
{
    char **v;
    size_t k = td_split(list, ',', &v);
    size_t i;

    *idx = NULL;
    *n = 0;
    if (k == 0)
    {
        /* No list: every column. */
        *idx = calloc(ds->schema.n + 1, sizeof(size_t));
        if (*idx == NULL)
            return TA_DATA_ENOMEM;
        for (i = 0; i < ds->schema.n; i++)
            (*idx)[i] = i;
        *n = ds->schema.n;
        return 0;
    }
    *idx = calloc(k, sizeof(size_t));
    if (*idx == NULL)
    {
        td_split_free(v, k);
        return TA_DATA_ENOMEM;
    }
    for (i = 0; i < k; i++)
    {
        int c = td_schema_find(&ds->schema, v[i]);

        if (c < 0)
        {
            int rc = td_fail(err, TA_DATA_ENOENT, "no column '%s' in '%s'",
                             v[i], ds->path);

            td_split_free(v, k);
            free(*idx);
            *idx = NULL;
            return rc;
        }
        (*idx)[i] = (size_t)c;
    }
    *n = k;
    td_split_free(v, k);
    return 0;
}

/* ---- typed scan --------------------------------------------------- */

int
td_scan_open(td_scan *s, ta_data_ds *ds, const td_kv *opts, char **err)
{
    const char *on_error = td_kv_get(opts, "on_error", "fail");
    int rc;

    memset(s, 0, sizeof(*s));
    s->ds = ds;
    s->fail_on_error = strcasecmp(on_error, "count") != 0 &&
                       strcasecmp(on_error, "skip") != 0;
    s->coerce = td_kv_bool(opts, "coerce", false);
    s->col_bad = calloc(ds->schema.n + 1, sizeof(uint64_t));
    if (s->col_bad == NULL)
        return TA_DATA_ENOMEM;
    rc = td_row_init(&s->row, ds->schema.n);
    if (rc == 0)
        rc = td_reader_open(ds, opts, &s->rd, err);
    if (rc != 0)
        td_scan_close(s);
    return rc;
}

static void
keep_error(td_scan *s, const char *e)
{
    if (s->first_error == NULL && e != NULL)
        s->first_error = td_strdup(e);
}

int
td_scan_next(td_scan *s, char **err)
{
    for (;;)
    {
        char *e = NULL;
        bool rowbad = false;
        size_t i;
        int rc = s->rd->next(s->rd, &s->row, &e);

        if (rc == 1)
            return 1;
        if (rc == TA_DATA_EILSEQ ||
            (rc == TA_DATA_ECORRUPT && s->ds->format == TD_FMT_JSON &&
             (s->rows > 0 || s->malformed > 0)))
        {
            s->malformed++;
            keep_error(s, e);
            if (s->fail_on_error)
            {
                rc = td_fail(err, TA_DATA_EILSEQ, "%s", e);
                free(e);
                return rc;
            }
            free(e);
            if (rc == TA_DATA_ECORRUPT)
                return 1;       /* nothing after a broken array */
            continue;
        }
        if (rc != 0)
        {
            rc = td_fail(err, rc, "%s", e == NULL ? ta_data_strerror(rc) : e);
            free(e);
            return rc;
        }
        free(e);
        for (i = 0; i < s->row.n; i++)
        {
            ta_data_cell *c = &s->row.cells[i];
            const td_col *col = &s->ds->schema.cols[i];
            bool coerced = false;
            int r;

            if (c->null)
                continue;
            r = td_cell_conform(c, col->type, s->coerce, &coerced);
            if (r < 0)
            {
                td_buf m = TD_BUF_INIT;
                td_buf v = TD_BUF_INIT;

                if (s->first_error == NULL || s->fail_on_error)
                {
                    c->raw = 0;
                    if (c->type == TA_DATA_T_STRING ||
                        c->type == TA_DATA_T_UNKNOWN)
                        td_buf_add(&v, c->s == NULL ? "" : c->s,
                                   c->len > 64 ? 64 : c->len);
                    else
                        td_cell_text(&v, c);
                    td_buf_printf(&m, "record %llu, column '%s': '%s' is "
                                  "not ", (unsigned long long)s->row.rowno,
                                  col->name, v.ptr == NULL ? "" : v.ptr);
                    td_type_format(&m, col->type);
                    keep_error(s, m.ptr);
                }
                ta_data_cell_clear(c);
                c->bad = 1;
                s->col_bad[i]++;
                rowbad = true;
                if (s->fail_on_error)
                {
                    rc = td_fail(err, TA_DATA_EILSEQ, "%s", m.ptr);
                    td_buf_free(&m);
                    td_buf_free(&v);
                    s->mismatched++;
                    return rc;
                }
                td_buf_free(&m);
                td_buf_free(&v);
            }
            else if (r == 1)
                s->coerced++;
        }
        if (rowbad)
            s->mismatched++;
        s->rows++;
        return 0;
    }
}

void
td_scan_close(td_scan *s)
{
    td_reader_close(s->rd);
    s->rd = NULL;
    td_row_free(&s->row);
    free(s->col_bad);
    s->col_bad = NULL;
    free(s->first_error);
    s->first_error = NULL;
}

void
td_scan_result(const td_scan *s, td_buf *res)
{
    td_buf_printf(res, "rows=%llu\nmalformed=%llu\nmismatched=%llu\n"
                  "coerced=%llu\nunexpected=%llu\n",
                  (unsigned long long)s->rows,
                  (unsigned long long)s->malformed,
                  (unsigned long long)s->mismatched,
                  (unsigned long long)s->coerced,
                  (unsigned long long)(s->rd == NULL ? 0 : s->rd->unexpected));
    if (s->rd != NULL && s->rd->unexpected_names != NULL)
    {
        td_buf_adds(res, "unexpected_columns=");
        td_esc(res, s->rd->unexpected_names, strlen(s->rd->unexpected_names));
        td_buf_addc(res, '\n');
    }
    if (s->first_error != NULL)
    {
        td_buf_adds(res, "first_error=");
        td_esc(res, s->first_error, strlen(s->first_error));
        td_buf_addc(res, '\n');
    }
}

/* ---- writers ------------------------------------------------------ */

struct td_writer {
    td_format       fmt;
    FILE           *f;
    char           *path;
    const td_schema *schema;
    char            delim;
    uint64_t        rows;
    void           *pq;
    ta_data_pq_col *pqcols;
    td_buf          line;
};

static bool
csv_needs_quote(const char *s, size_t len, char delim)
{
    size_t i;

    if (len == 0)
        return true;            /* "" is an empty string, not NULL */
    if (s[0] == ' ' || s[len - 1] == ' ')
        return true;
    for (i = 0; i < len; i++)
    {
        if (s[i] == delim || s[i] == '"' || s[i] == '\n' || s[i] == '\r')
            return true;
    }
    return false;
}

static void
csv_field(td_buf *b, const char *s, size_t len, char delim)
{
    size_t i;

    if (!csv_needs_quote(s, len, delim))
    {
        td_buf_add(b, s, len);
        return;
    }
    td_buf_addc(b, '"');
    for (i = 0; i < len; i++)
    {
        if (s[i] == '"')
            td_buf_addc(b, '"');
        td_buf_addc(b, s[i]);
    }
    td_buf_addc(b, '"');
}

int
td_writer_open(const char *path, td_format fmt, const td_schema *schema,
               const td_kv *opts, td_writer **out, char **err)
{
    td_writer *w = calloc(1, sizeof(*w));
    const char *d = td_kv_get(opts, "delimiter", ",");
    size_t i;
    int rc = 0;

    if (w == NULL)
        return TA_DATA_ENOMEM;
    w->fmt = fmt;
    w->schema = schema;
    w->path = td_strdup(path);
    w->delim = strcmp(d, "tab") == 0 || strcmp(d, "\\t") == 0 ? '\t' :
               (d[0] == '\0' ? ',' : d[0]);
    if (fmt == TD_FMT_PARQUET)
    {
        w->pqcols = calloc(schema->n + 1, sizeof(*w->pqcols));
        if (w->pqcols == NULL)
        {
            free(w->path);
            free(w);
            return TA_DATA_ENOMEM;
        }
        for (i = 0; i < schema->n; i++)
        {
            w->pqcols[i].name = schema->cols[i].name;
            w->pqcols[i].type = schema->cols[i].type->kind;
            w->pqcols[i].nullable = schema->cols[i].nullable;
            w->pqcols[i].precision = schema->cols[i].type->precision;
            w->pqcols[i].scale = schema->cols[i].type->scale;
        }
        rc = ta_data_pq_wopen(path, (int)schema->n, w->pqcols,
                              td_kv_int(opts, "row_group", 65536), &w->pq,
                              err);
    }
    else if (fmt == TD_FMT_SQLITE)
        rc = td_fail(err, TA_DATA_ENOTSUP, "writing SQLite is not supported");
    else
    {
        w->f = fopen(path, "wb");
        if (w->f == NULL)
            rc = td_fail(err, TA_DATA_EIO, "cannot create '%s': %s", path,
                         strerror(errno));
        else if (fmt == TD_FMT_CSV && td_kv_bool(opts, "header", true))
        {
            for (i = 0; i < schema->n; i++)
            {
                if (i > 0)
                    td_buf_addc(&w->line, w->delim);
                csv_field(&w->line, schema->cols[i].name,
                          strlen(schema->cols[i].name), w->delim);
            }
            td_buf_addc(&w->line, '\n');
            fwrite(w->line.ptr, 1, w->line.len, w->f);
        }
        else if (fmt == TD_FMT_JSON)
            fputs("[", w->f);
    }
    if (rc != 0)
    {
        if (w->f != NULL)
            fclose(w->f);
        free(w->pqcols);
        free(w->path);
        td_buf_free(&w->line);
        free(w);
        return rc;
    }
    *out = w;
    return 0;
}

static void
row_json(td_writer *w, const td_row *row, td_buf *b)
{
    size_t i;

    td_buf_addc(b, '{');
    for (i = 0; i < row->n; i++)
    {
        td_buf name = TD_BUF_INIT;

        if (i > 0)
            td_buf_addc(b, ',');
        /* Column names go through the cell JSON path for escaping. */
        {
            ta_data_cell c;

            memset(&c, 0, sizeof(c));
            c.type = TA_DATA_T_STRING;
            c.s = w->schema->cols[i].name;
            c.len = strlen(c.s);
            td_cell_json(&name, &c);
        }
        td_buf_add(b, name.ptr, name.len);
        td_buf_free(&name);
        td_buf_addc(b, ':');
        td_cell_json(b, &row->cells[i]);
    }
    td_buf_addc(b, '}');
}

int
td_writer_row(td_writer *w, const td_row *row, char **err)
{
    size_t i;

    td_buf_reset(&w->line);
    switch (w->fmt)
    {
        case TD_FMT_PARQUET:
            w->rows++;
            return ta_data_pq_write(w->pq, row->cells, err);

        case TD_FMT_CSV:
            for (i = 0; i < row->n; i++)
            {
                const ta_data_cell *c = &row->cells[i];

                if (i > 0)
                    td_buf_addc(&w->line, w->delim);
                if (!c->null)
                {
                    td_buf t = TD_BUF_INIT;

                    td_cell_text(&t, c);
                    csv_field(&w->line, t.ptr == NULL ? "" : t.ptr, t.len,
                              w->delim);
                    td_buf_free(&t);
                }
            }
            td_buf_addc(&w->line, '\n');
            break;

        case TD_FMT_JSON:
            td_buf_adds(&w->line, w->rows == 0 ? "\n" : ",\n");
            row_json(w, row, &w->line);
            break;

        case TD_FMT_JSONL:
            row_json(w, row, &w->line);
            td_buf_addc(&w->line, '\n');
            break;

        default:
            return TA_DATA_EINVAL;
    }
    w->rows++;
    if (fwrite(w->line.ptr, 1, w->line.len, w->f) != w->line.len)
        return td_fail(err, TA_DATA_EIO, "write to '%s' failed: %s", w->path,
                       strerror(errno));
    return 0;
}

int
td_writer_malformed(td_writer *w, const td_row *row, td_rng *rng, char **err)
{
    size_t i;

    td_buf_reset(&w->line);
    switch (w->fmt)
    {
        case TD_FMT_CSV:
            /* One field short: a record the header does not describe. */
            for (i = 0; i + 1 < row->n; i++)
            {
                td_buf t = TD_BUF_INIT;

                if (i > 0)
                    td_buf_addc(&w->line, w->delim);
                td_cell_text(&t, &row->cells[i]);
                csv_field(&w->line, t.ptr == NULL ? "" : t.ptr, t.len,
                          w->delim);
                td_buf_free(&t);
            }
            if (row->n <= 1)
                td_buf_printf(&w->line, "%cextra", w->delim);
            td_buf_addc(&w->line, '\n');
            break;

        case TD_FMT_JSONL:
        {
            /* A record cut short: not JSON at all. */
            td_buf t = TD_BUF_INIT;

            row_json(w, row, &t);
            td_buf_add(&w->line, t.ptr, t.len / 2 + 1);
            td_buf_addc(&w->line, '\n');
            td_buf_free(&t);
            break;
        }

        case TD_FMT_JSON:
            /* Valid JSON, but not a record object. */
            td_buf_adds(&w->line, w->rows == 0 ? "\n" : ",\n");
            td_buf_printf(&w->line, "\"malformed-%llu\"",
                          (unsigned long long)(td_rng_next(rng) % 1000000));
            break;

        default:
            return td_fail(err, TA_DATA_EINVAL, "%s cannot carry malformed "
                           "records (typed format)", td_format_name(w->fmt));
    }
    w->rows++;
    if (fwrite(w->line.ptr, 1, w->line.len, w->f) != w->line.len)
        return td_fail(err, TA_DATA_EIO, "write to '%s' failed", w->path);
    return 0;
}

int
td_writer_close(td_writer *w, uint64_t *bytes, char **err)
{
    struct stat st;
    int rc = 0;

    if (w == NULL)
        return 0;
    if (w->fmt == TD_FMT_PARQUET)
        rc = w->pq != NULL ? ta_data_pq_wclose(w->pq, err) : 0;
    else if (w->f != NULL)
    {
        if (w->fmt == TD_FMT_JSON)
            fputs(w->rows == 0 ? "]\n" : "\n]\n", w->f);
        if (fclose(w->f) != 0)
            rc = td_fail(err, TA_DATA_EIO, "close '%s': %s", w->path,
                         strerror(errno));
    }
    if (bytes != NULL)
        *bytes = stat(w->path, &st) == 0 ? (uint64_t)st.st_size : 0;
    free(w->pqcols);
    free(w->path);
    td_buf_free(&w->line);
    free(w);
    return rc;
}
