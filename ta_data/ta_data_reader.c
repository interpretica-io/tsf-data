/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: format readers and schema discovery
 *
 * One reader per source format, all streaming: CSV (RFC 4180, quoted
 * fields with embedded separators and newlines), JSON (a top-level array
 * of objects, split element by element so the document is never loaded
 * whole), JSON Lines, SQLite query results, and Parquet over the C ABI
 * of ta_data_parquet.cpp. Every reader delivers rows in schema order;
 * CSV cells come as raw text and are typed by the scan, the others are
 * typed at the source and conformed by the scan.
 *
 * A record that cannot be read as a record (a CSV line with the wrong
 * field count, an unterminated quote, a JSON line that is not an object)
 * is skipped and reported as TA_DATA_EILSEQ with its record number; the
 * scan decides whether that fails the operation (on_error=fail, the
 * default) or is only counted (on_error=count).
 */

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>

#include <jansson.h>

#ifdef TA_DATA_HAVE_SQLITE
#include <sqlite3.h>
#endif

#include "ta_data_int.h"

/* ---- formats ------------------------------------------------------ */

int
td_format_parse(const char *name, const char *path, td_format *fmt)
{
    const char *ext;

    if (name == NULL || *name == '\0')
    {
        ext = path == NULL ? NULL : strrchr(path, '.');
        if (ext == NULL)
            return TA_DATA_EINVAL;
        name = ext + 1;
    }
    if (strcasecmp(name, "csv") == 0 || strcasecmp(name, "tsv") == 0)
        *fmt = TD_FMT_CSV;
    else if (strcasecmp(name, "json") == 0)
        *fmt = TD_FMT_JSON;
    else if (strcasecmp(name, "jsonl") == 0 ||
             strcasecmp(name, "ndjson") == 0)
        *fmt = TD_FMT_JSONL;
    else if (strcasecmp(name, "parquet") == 0 || strcasecmp(name, "pq") == 0)
        *fmt = TD_FMT_PARQUET;
    else if (strcasecmp(name, "sqlite") == 0 || strcasecmp(name, "db") == 0 ||
             strcasecmp(name, "sqlite3") == 0)
        *fmt = TD_FMT_SQLITE;
    else
        return TA_DATA_EINVAL;
    return 0;
}

const char *
td_format_name(td_format fmt)
{
    switch (fmt)
    {
        case TD_FMT_CSV: return "csv";
        case TD_FMT_JSON: return "json";
        case TD_FMT_JSONL: return "jsonl";
        case TD_FMT_PARQUET: return "parquet";
        case TD_FMT_SQLITE: return "sqlite";
        default: return "unknown";
    }
}

/* ---- buffered input ----------------------------------------------- */

typedef struct td_in {
    FILE   *f;
    char    buf[65536];
    size_t  pos;
    size_t  len;
    bool    eof;
} td_in;

static int
in_open(td_in *in, const char *path, char **err)
{
    memset(in, 0, sizeof(*in));
    in->f = fopen(path, "rb");
    if (in->f == NULL)
        return td_fail(err, errno == ENOENT ? TA_DATA_ENOENT : TA_DATA_EIO,
                       "cannot open '%s': %s", path, strerror(errno));
    return 0;
}

/* Next byte, or -1 at end of file. */
static inline int
in_getc(td_in *in)
{
    if (in->pos == in->len)
    {
        if (in->eof)
            return -1;
        in->len = fread(in->buf, 1, sizeof(in->buf), in->f);
        in->pos = 0;
        if (in->len == 0)
        {
            in->eof = true;
            return -1;
        }
    }
    return (unsigned char)in->buf[in->pos++];
}

static inline int
in_peek(td_in *in)
{
    int c = in_getc(in);

    if (c >= 0)
        in->pos--;
    return c;
}

static void
in_close(td_in *in)
{
    if (in->f != NULL)
        fclose(in->f);
    in->f = NULL;
}

/* ---- CSV tokenizer ------------------------------------------------ */

typedef struct csv_tok {
    td_in       in;
    char        delim;
    char        quote;
    uint64_t    recno;      /**< data records read (header excluded) */
    td_buf      field;
    td_buf     *fields;     /**< current record */
    bool       *quoted;
    size_t      nfields;
    size_t      cap;
    bool        bom_checked;
} csv_tok;

static int
csv_push(csv_tok *t, bool quoted)
{
    if (t->nfields == t->cap)
    {
        size_t cap = t->cap == 0 ? 16 : t->cap * 2;
        td_buf *nf = realloc(t->fields, cap * sizeof(*nf));
        bool *nq;

        if (nf == NULL)
            return TA_DATA_ENOMEM;
        t->fields = nf;
        nq = realloc(t->quoted, cap * sizeof(*nq));
        if (nq == NULL)
            return TA_DATA_ENOMEM;
        t->quoted = nq;
        memset(t->fields + t->cap, 0, (cap - t->cap) * sizeof(*nf));
        t->cap = cap;
    }
    td_buf_reset(&t->fields[t->nfields]);
    td_buf_add(&t->fields[t->nfields], t->field.ptr == NULL ? "" :
               t->field.ptr, t->field.len);
    t->quoted[t->nfields] = quoted;
    t->nfields++;
    td_buf_reset(&t->field);
    return 0;
}

/*
 * Read one record. 0 a record, 1 EOF, TA_DATA_EILSEQ an unterminated
 * quoted field (the rest of the file was swallowed by it).
 */
static int
csv_next(csv_tok *t)
{
    int c;
    bool quoted = false;
    bool any = false;

    t->nfields = 0;
    td_buf_reset(&t->field);
    if (!t->bom_checked)
    {
        t->bom_checked = true;
        if (in_peek(&t->in) == 0xEF)
        {
            in_getc(&t->in);
            if (in_getc(&t->in) != 0xBB || in_getc(&t->in) != 0xBF)
                return TA_DATA_EILSEQ;
        }
    }
    for (;;)
    {
        c = in_getc(&t->in);
        if (c < 0)
        {
            if (!any)
                return 1;
            t->recno++;
            return csv_push(t, quoted);
        }
        any = true;
        if (c == t->quote && t->field.len == 0 && !quoted)
        {
            /* Quoted field: up to the closing quote, "" is a quote. */
            quoted = true;
            for (;;)
            {
                c = in_getc(&t->in);
                if (c < 0)
                {
                    t->recno++;
                    return TA_DATA_EILSEQ;
                }
                if (c == t->quote)
                {
                    if (in_peek(&t->in) == t->quote)
                    {
                        in_getc(&t->in);
                        td_buf_addc(&t->field, (char)c);
                        continue;
                    }
                    break;
                }
                td_buf_addc(&t->field, (char)c);
            }
            continue;
        }
        if (c == t->delim)
        {
            if (csv_push(t, quoted) != 0)
                return TA_DATA_ENOMEM;
            quoted = false;
            continue;
        }
        if (c == '\r' && in_peek(&t->in) == '\n')
            continue;
        if (c == '\n')
        {
            t->recno++;
            if (t->nfields == 0 && t->field.len == 0 && !quoted)
            {
                /* A blank line is not a record. */
                any = false;
                t->recno--;
                continue;
            }
            return csv_push(t, quoted);
        }
        td_buf_addc(&t->field, (char)c);
    }
}

static void
csv_tok_free(csv_tok *t)
{
    size_t i;

    in_close(&t->in);
    td_buf_free(&t->field);
    for (i = 0; i < t->cap; i++)
        td_buf_free(&t->fields[i]);
    free(t->fields);
    free(t->quoted);
}

static int
csv_tok_open(csv_tok *t, const ta_data_ds *ds, char **err)
{
    const char *d = td_kv_get(&ds->opts, "delimiter", NULL);
    const char *q = td_kv_get(&ds->opts, "quote", "\"");

    memset(t, 0, sizeof(*t));
    if (d == NULL)
    {
        const char *ext = strrchr(ds->path, '.');

        d = ext != NULL && strcasecmp(ext, ".tsv") == 0 ? "\t" : ",";
    }
    if (strcmp(d, "tab") == 0 || strcmp(d, "\\t") == 0)
        d = "\t";
    t->delim = d[0] == '\0' ? ',' : d[0];
    t->quote = q[0] == '\0' ? '"' : q[0];
    return in_open(&t->in, ds->path, err);
}

/* ---- CSV reader --------------------------------------------------- */

typedef struct csv_reader {
    csv_tok     tok;
    size_t     *map;        /**< schema column -> field index, or SIZE_MAX */
    size_t      nsrc;
    const char *null_token;
} csv_reader;

static bool
is_null_field(const csv_reader *cr, const td_buf *f, bool quoted)
{
    if (!quoted && f->len == 0)
        return true;
    if (cr->null_token != NULL && !quoted &&
        strlen(cr->null_token) == f->len &&
        memcmp(cr->null_token, f->ptr, f->len) == 0)
        return true;
    return false;
}

static int
csv_reader_next(td_reader *r, td_row *row, char **err)
{
    csv_reader *cr = r->priv;
    int rc = csv_next(&cr->tok);
    size_t i;

    td_row_clear(row);
    if (rc == 1)
        return 1;
    row->rowno = cr->tok.recno;
    if (rc == TA_DATA_EILSEQ)
        return td_fail(err, rc, "record %llu: unterminated quoted field "
                       "(truncated input?)",
                       (unsigned long long)cr->tok.recno);
    if (rc != 0)
        return rc;
    if (cr->tok.nfields != cr->nsrc)
        return td_fail(err, TA_DATA_EILSEQ,
                       "record %llu: %zu fields, the header has %zu",
                       (unsigned long long)cr->tok.recno, cr->tok.nfields,
                       cr->nsrc);
    for (i = 0; i < row->n; i++)
    {
        size_t k = cr->map[i];
        td_buf *f;

        if (k == SIZE_MAX)
            continue;
        f = &cr->tok.fields[k];
        if (is_null_field(cr, f, cr->tok.quoted[k]))
            continue;
        if (td_cell_set_str(&row->cells[i], TA_DATA_T_STRING,
                            f->ptr == NULL ? "" : f->ptr, f->len) != 0)
            return TA_DATA_ENOMEM;
        row->cells[i].raw = 1;
    }
    return 0;
}

static void
csv_reader_close(td_reader *r)
{
    csv_reader *cr = r->priv;

    csv_tok_free(&cr->tok);
    free(cr->map);
    free(cr);
}

/* Read the header (or make c1..cN names) into ds->src_names. */
static int
csv_header(csv_tok *t, ta_data_ds *ds, bool header, td_buf **first,
           size_t *nfirst, char **err)
{
    int rc = csv_next(t);
    size_t i;

    *first = NULL;
    *nfirst = 0;
    if (rc == 1)
        return td_fail(err, TA_DATA_ECORRUPT, "'%s' is empty", ds->path);
    if (rc != 0)
        return td_fail(err, TA_DATA_ECORRUPT, "'%s': unreadable header",
                       ds->path);
    if (ds->src_names == NULL)
    {
        ds->nsrc = t->nfields;
        ds->src_names = calloc(t->nfields, sizeof(char *));
        if (ds->src_names == NULL)
            return TA_DATA_ENOMEM;
        for (i = 0; i < t->nfields; i++)
        {
            if (header)
                ds->src_names[i] = td_strndup(t->fields[i].ptr == NULL ? "" :
                                              t->fields[i].ptr,
                                              t->fields[i].len);
            else
            {
                td_buf n = TD_BUF_INIT;

                td_buf_printf(&n, "c%zu", i + 1);
                ds->src_names[i] = td_buf_take(&n);
            }
        }
    }
    if (!header)
    {
        /* The first record is data: hand it back to the caller. */
        *first = t->fields;
        *nfirst = t->nfields;
    }
    return 0;
}

static int
csv_reader_open(td_reader *r, const td_kv *opts, char **err)
{
    ta_data_ds *ds = r->ds;
    csv_reader *cr = calloc(1, sizeof(*cr));
    bool header = td_kv_bool(opts, "header", true);
    td_buf *first;
    size_t nfirst;
    size_t i;
    int rc;

    if (cr == NULL)
        return TA_DATA_ENOMEM;
    r->priv = cr;
    r->next = csv_reader_next;
    r->close = csv_reader_close;
    cr->null_token = td_kv_get(opts, "null", NULL);
    rc = csv_tok_open(&cr->tok, ds, err);
    if (rc != 0)
        return rc;
    rc = csv_header(&cr->tok, ds, header, &first, &nfirst, err);
    if (rc != 0)
        return rc;
    cr->nsrc = ds->nsrc;
    cr->map = calloc(ds->schema.n == 0 ? 1 : ds->schema.n, sizeof(size_t));
    if (cr->map == NULL)
        return TA_DATA_ENOMEM;
    for (i = 0; i < ds->schema.n; i++)
    {
        size_t k;

        cr->map[i] = SIZE_MAX;
        for (k = 0; k < ds->nsrc; k++)
        {
            if (strcmp(ds->src_names[k], ds->schema.cols[i].name) == 0)
            {
                cr->map[i] = k;
                break;
            }
        }
    }
    if (!header)
    {
        /* Rewind: re-open so the first record is read as data. */
        csv_tok_free(&cr->tok);
        rc = csv_tok_open(&cr->tok, ds, err);
    }
    else
    {
        cr->tok.recno = 0;      /* the header read above is not data */
    }
    return rc;
}

/* ---- JSON array element splitter ---------------------------------- */

typedef struct json_split {
    td_in       in;
    bool        started;
    bool        done;
    uint64_t    recno;
    td_buf      elem;
} json_split;

/*
 * Next top-level element of a JSON array, as text in js->elem. 0 an
 * element, 1 end of array, TA_DATA_EILSEQ truncated element,
 * TA_DATA_ECORRUPT not an array / truncated array.
 */
static int
json_split_next(json_split *js, char **err)
{
    int c;
    int depth = 0;
    bool in_str = false;
    bool esc = false;

    td_buf_reset(&js->elem);
    if (js->done)
        return 1;
    do {
        c = in_getc(&js->in);
    } while (c >= 0 && isspace(c));
    if (!js->started)
    {
        if (c == 0xEF)
        {
            in_getc(&js->in);
            in_getc(&js->in);
            do {
                c = in_getc(&js->in);
            } while (c >= 0 && isspace(c));
        }
        if (c != '[')
            return td_fail(err, TA_DATA_ECORRUPT,
                           "JSON input is not an array of records");
        js->started = true;
        do {
            c = in_getc(&js->in);
        } while (c >= 0 && isspace(c));
        if (c == ']')
        {
            js->done = true;
            return 1;
        }
    }
    else if (c == ',')
    {
        do {
            c = in_getc(&js->in);
        } while (c >= 0 && isspace(c));
    }
    else if (c == ']')
    {
        js->done = true;
        return 1;
    }
    else
    {
        js->done = true;
        return td_fail(err, TA_DATA_ECORRUPT,
                       "after record %llu: expected ',' or ']'%s",
                       (unsigned long long)js->recno,
                       c < 0 ? " (truncated input)" : "");
    }
    if (c < 0)
    {
        js->done = true;
        return td_fail(err, TA_DATA_ECORRUPT,
                       "after record %llu: unterminated JSON array "
                       "(truncated input)", (unsigned long long)js->recno);
    }
    js->recno++;
    for (;;)
    {
        if (c < 0)
        {
            js->done = true;
            return td_fail(err, TA_DATA_EILSEQ,
                           "record %llu: truncated JSON value",
                           (unsigned long long)js->recno);
        }
        if (in_str)
        {
            if (esc)
                esc = false;
            else if (c == '\\')
                esc = true;
            else if (c == '"')
                in_str = false;
        }
        else if (c == '"')
            in_str = true;
        else if (c == '{' || c == '[')
            depth++;
        else if (c == '}' || c == ']')
        {
            if (depth == 0)
            {
                /* The array's own ']' after a scalar element. */
                js->in.pos--;
                break;
            }
            depth--;
        }
        else if (c == ',' && depth == 0)
        {
            js->in.pos--;
            break;
        }
        td_buf_addc(&js->elem, (char)c);
        if (depth == 0 && !in_str && (c == '}' || c == ']'))
            break;
        c = in_getc(&js->in);
    }
    return 0;
}

/* ---- JSON / JSONL reader ------------------------------------------ */

typedef struct json_reader {
    bool        lines;
    json_split  split;
    td_in       in;         /* JSONL */
    td_buf      line;
    uint64_t    recno;
    size_t      nnames;
} json_reader;

/* A JSON value into a typed cell (STRING/INT/FLOAT/BOOL/ARRAY/OBJECT). */
static int
json_to_cell(json_t *v, ta_data_cell *c)
{
    ta_data_cell_clear(c);
    if (v == NULL || json_is_null(v))
        return 0;
    c->null = 0;
    if (json_is_string(v))
        return td_cell_set_str(c, TA_DATA_T_STRING, json_string_value(v),
                               json_string_length(v));
    if (json_is_integer(v))
    {
        c->type = TA_DATA_T_INT;
        c->i = json_integer_value(v);
        return 0;
    }
    if (json_is_real(v))
    {
        c->type = TA_DATA_T_FLOAT;
        c->d = json_real_value(v);
        return 0;
    }
    if (json_is_boolean(v))
    {
        c->type = TA_DATA_T_BOOL;
        c->i = json_is_true(v) ? 1 : 0;
        return 0;
    }
    {
        char *s = json_dumps(v, JSON_COMPACT | JSON_SORT_KEYS |
                             JSON_ENCODE_ANY | JSON_REAL_PRECISION(17));

        if (s == NULL)
            return TA_DATA_ENOMEM;
        c->s = s;
        c->len = strlen(s);
        c->type = json_is_array(v) ? TA_DATA_T_ARRAY : TA_DATA_T_OBJECT;
    }
    return 0;
}

/* Next record as a parsed JSON object (caller decrefs). */
static int
json_next_object(json_reader *jr, td_reader *r, json_t **obj,
                 uint64_t *recno, char **err)
{
    json_error_t je;
    json_t *j;
    int rc;

    (void)r;
    *obj = NULL;
    if (!jr->lines)
    {
        rc = json_split_next(&jr->split, err);
        *recno = jr->split.recno;
        if (rc != 0)
            return rc;
        j = json_loadb(jr->split.elem.ptr, jr->split.elem.len, 0, &je);
        if (j == NULL)
            return td_fail(err, TA_DATA_EILSEQ, "record %llu: %s",
                           (unsigned long long)*recno, je.text);
    }
    else
    {
        int c;

        for (;;)
        {
            td_buf_reset(&jr->line);
            while ((c = in_getc(&jr->in)) >= 0 && c != '\n')
                td_buf_addc(&jr->line, (char)c);
            if (c < 0 && jr->line.len == 0)
                return 1;
            jr->recno++;
            if (jr->line.len > 0 && jr->line.ptr[jr->line.len - 1] == '\r')
                jr->line.ptr[--jr->line.len] = '\0';
            {
                size_t k = 0;

                while (k < jr->line.len && isspace((unsigned char)
                                                   jr->line.ptr[k]))
                    k++;
                if (k < jr->line.len)
                    break;
            }
            if (c < 0)
                return 1;
        }
        *recno = jr->recno;
        j = json_loadb(jr->line.ptr, jr->line.len, JSON_DECODE_ANY, &je);
        if (j == NULL)
            return td_fail(err, TA_DATA_EILSEQ, "line %llu: %s%s",
                           (unsigned long long)*recno, je.text,
                           c < 0 ? " (truncated input?)" : "");
    }
    if (!json_is_object(j))
    {
        json_decref(j);
        return td_fail(err, TA_DATA_EILSEQ, "record %llu: not a JSON object",
                       (unsigned long long)*recno);
    }
    *obj = j;
    return 0;
}

static void
note_unexpected(td_reader *r, const char *key)
{
    td_buf b = TD_BUF_INIT;

    r->unexpected++;
    if (r->unexpected_names != NULL &&
        (strlen(r->unexpected_names) > 200 ||
         strstr(r->unexpected_names, key) != NULL))
        return;
    if (r->unexpected_names != NULL)
    {
        /* Exact membership, not substring. */
        char **v;
        size_t n = td_split(r->unexpected_names, ',', &v);
        size_t i;
        bool seen = false;

        for (i = 0; i < n; i++)
            seen = seen || strcmp(v[i], key) == 0;
        td_split_free(v, n);
        if (seen)
            return;
        td_buf_adds(&b, r->unexpected_names);
        td_buf_addc(&b, ',');
    }
    td_buf_adds(&b, key);
    free(r->unexpected_names);
    r->unexpected_names = td_buf_take(&b);
}

static int
json_reader_next(td_reader *r, td_row *row, char **err)
{
    json_reader *jr = r->priv;
    json_t *obj;
    const char *key;
    json_t *v;
    uint64_t recno = 0;
    int rc;

    td_row_clear(row);
    rc = json_next_object(jr, r, &obj, &recno, err);
    row->rowno = recno;
    if (rc != 0)
        return rc;
    json_object_foreach(obj, key, v)
    {
        int k = td_schema_find(&r->ds->schema, key);

        if (k < 0)
        {
            note_unexpected(r, key);
            continue;
        }
        rc = json_to_cell(v, &row->cells[k]);
        if (rc != 0)
            break;
    }
    json_decref(obj);
    return rc;
}

static void
json_reader_close(td_reader *r)
{
    json_reader *jr = r->priv;

    in_close(&jr->split.in);
    td_buf_free(&jr->split.elem);
    in_close(&jr->in);
    td_buf_free(&jr->line);
    free(jr);
}

static int
json_reader_open(td_reader *r, char **err)
{
    json_reader *jr = calloc(1, sizeof(*jr));

    if (jr == NULL)
        return TA_DATA_ENOMEM;
    r->priv = jr;
    r->next = json_reader_next;
    r->close = json_reader_close;
    jr->lines = r->ds->format == TD_FMT_JSONL;
    if (jr->lines)
        return in_open(&jr->in, r->ds->path, err);
    return in_open(&jr->split.in, r->ds->path, err);
}

/* ---- SQLite reader ------------------------------------------------ */

#ifdef TA_DATA_HAVE_SQLITE
typedef struct sql_reader {
    sqlite3        *db;
    sqlite3_stmt   *st;
    int            *map;    /* schema column -> statement column */
    uint64_t        recno;
} sql_reader;

static int
sql_prepare(ta_data_ds *ds, const td_kv *opts, sqlite3 **db,
            sqlite3_stmt **st, char **err)
{
    const char *q = td_kv_get(opts, "query", NULL);
    const char *table = td_kv_get(opts, "table", NULL);
    td_buf sql = TD_BUF_INIT;
    int rc;

    if (q == NULL && table == NULL)
        return td_fail(err, TA_DATA_EINVAL,
                       "an sqlite source needs query= or table=");
    rc = sqlite3_open_v2(ds->path, db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK)
    {
        rc = td_fail(err, TA_DATA_ENOENT, "cannot open database '%s': %s",
                     ds->path, *db != NULL ? sqlite3_errmsg(*db) :
                     "out of memory");
        sqlite3_close(*db);
        *db = NULL;
        return rc;
    }
    if (q != NULL)
        td_buf_adds(&sql, q);
    else
        td_buf_printf(&sql, "SELECT * FROM \"%s\"", table);
    rc = sqlite3_prepare_v2(*db, sql.ptr, -1, st, NULL);
    td_buf_free(&sql);
    if (rc != SQLITE_OK)
    {
        rc = td_fail(err, TA_DATA_EINVAL, "query: %s", sqlite3_errmsg(*db));
        sqlite3_close(*db);
        *db = NULL;
        return rc;
    }
    return 0;
}

static int
sql_reader_next(td_reader *r, td_row *row, char **err)
{
    sql_reader *sr = r->priv;
    size_t i;
    int rc;

    td_row_clear(row);
    rc = sqlite3_step(sr->st);
    if (rc == SQLITE_DONE)
        return 1;
    if (rc != SQLITE_ROW)
        return td_fail(err, TA_DATA_ECORRUPT, "query step: %s",
                       sqlite3_errmsg(sr->db));
    row->rowno = ++sr->recno;
    for (i = 0; i < row->n; i++)
    {
        ta_data_cell *c = &row->cells[i];
        int k = sr->map[i];

        if (k < 0)
            continue;
        switch (sqlite3_column_type(sr->st, k))
        {
            case SQLITE_NULL:
                break;
            case SQLITE_INTEGER:
                c->null = 0;
                c->type = TA_DATA_T_INT;
                c->i = sqlite3_column_int64(sr->st, k);
                /* SQLite has no boolean: 0/1 in a bool column is native. */
                if (r->ds->schema.cols[i].type->kind == TA_DATA_T_BOOL &&
                    (c->i == 0 || c->i == 1))
                    c->type = TA_DATA_T_BOOL;
                break;
            case SQLITE_FLOAT:
                c->null = 0;
                c->type = TA_DATA_T_FLOAT;
                c->d = sqlite3_column_double(sr->st, k);
                break;
            case SQLITE_BLOB:
                td_cell_set_str(c, TA_DATA_T_BINARY,
                                sqlite3_column_blob(sr->st, k),
                                (size_t)sqlite3_column_bytes(sr->st, k));
                break;
            default:
                td_cell_set_str(c, TA_DATA_T_STRING,
                                (const char *)sqlite3_column_text(sr->st, k),
                                (size_t)sqlite3_column_bytes(sr->st, k));
                break;
        }
    }
    return 0;
}

static void
sql_reader_close(td_reader *r)
{
    sql_reader *sr = r->priv;

    sqlite3_finalize(sr->st);
    sqlite3_close(sr->db);
    free(sr->map);
    free(sr);
}

static int
sql_reader_open(td_reader *r, const td_kv *opts, char **err)
{
    sql_reader *sr = calloc(1, sizeof(*sr));
    size_t i;
    int rc;

    if (sr == NULL)
        return TA_DATA_ENOMEM;
    r->priv = sr;
    r->next = sql_reader_next;
    r->close = sql_reader_close;
    rc = sql_prepare(r->ds, opts, &sr->db, &sr->st, err);
    if (rc != 0)
        return rc;
    sr->map = calloc(r->ds->schema.n == 0 ? 1 : r->ds->schema.n, sizeof(int));
    if (sr->map == NULL)
        return TA_DATA_ENOMEM;
    for (i = 0; i < r->ds->schema.n; i++)
    {
        int k;

        sr->map[i] = -1;
        for (k = 0; k < sqlite3_column_count(sr->st); k++)
        {
            if (strcmp(sqlite3_column_name(sr->st, k),
                       r->ds->schema.cols[i].name) == 0)
            {
                sr->map[i] = k;
                break;
            }
        }
    }
    return 0;
}
#endif /* TA_DATA_HAVE_SQLITE */

/* ---- Parquet reader (over the C++ seam) --------------------------- */

typedef struct pq_reader {
    void           *h;
    int             ncols;
    int            *map;    /* schema column -> parquet column */
    ta_data_cell   *cells;
    uint64_t        recno;
} pq_reader;

static int
pq_reader_next(td_reader *r, td_row *row, char **err)
{
    pq_reader *pr = r->priv;
    size_t i;
    int k;
    int rc;

    td_row_clear(row);
    for (k = 0; k < pr->ncols; k++)
        ta_data_cell_clear(&pr->cells[k]);
    rc = ta_data_pq_next(pr->h, pr->cells, err);
    if (rc != 0)
        return rc;
    row->rowno = ++pr->recno;
    for (i = 0; i < row->n; i++)
    {
        if (pr->map[i] < 0)
            continue;
        row->cells[i] = pr->cells[pr->map[i]];
        memset(&pr->cells[pr->map[i]], 0, sizeof(ta_data_cell));
        pr->cells[pr->map[i]].null = 1;
        td_json_canon_cell(&row->cells[i]);
    }
    return 0;
}

static void
pq_reader_close(td_reader *r)
{
    pq_reader *pr = r->priv;
    int k;

    if (pr->h != NULL)
        ta_data_pq_close(pr->h);
    for (k = 0; pr->cells != NULL && k < pr->ncols; k++)
        ta_data_cell_clear(&pr->cells[k]);
    free(pr->cells);
    free(pr->map);
    free(pr);
}

static int
pq_reader_open(td_reader *r, const td_kv *opts, char **err)
{
    pq_reader *pr = calloc(1, sizeof(*pr));
    size_t i;
    int k;
    int rc;

    if (pr == NULL)
        return TA_DATA_ENOMEM;
    r->priv = pr;
    r->next = pq_reader_next;
    r->close = pq_reader_close;
    rc = ta_data_pq_open(r->ds->path, (int)td_kv_int(opts, "batch_rows", 8192),
                         &pr->h, &pr->ncols, err);
    if (rc != 0)
        return rc;
    pr->cells = calloc((size_t)pr->ncols + 1, sizeof(*pr->cells));
    pr->map = calloc(r->ds->schema.n + 1, sizeof(int));
    if (pr->cells == NULL || pr->map == NULL)
        return TA_DATA_ENOMEM;
    for (k = 0; k < pr->ncols; k++)
        pr->cells[k].null = 1;
    for (i = 0; i < r->ds->schema.n; i++)
    {
        pr->map[i] = -1;
        for (k = 0; k < (int)r->ds->nsrc && k < pr->ncols; k++)
        {
            if (strcmp(r->ds->src_names[k], r->ds->schema.cols[i].name) == 0)
            {
                pr->map[i] = k;
                break;
            }
        }
    }
    return 0;
}

/* ---- reader dispatch ---------------------------------------------- */

int
td_reader_open(ta_data_ds *ds, const td_kv *opts, td_reader **out, char **err)
{
    td_reader *r = calloc(1, sizeof(*r));
    int rc;

    if (r == NULL)
        return TA_DATA_ENOMEM;
    r->ds = ds;
    switch (ds->format)
    {
        case TD_FMT_CSV:
            rc = csv_reader_open(r, opts, err);
            break;
        case TD_FMT_JSON:
        case TD_FMT_JSONL:
            rc = json_reader_open(r, err);
            break;
        case TD_FMT_PARQUET:
            rc = pq_reader_open(r, opts, err);
            break;
        case TD_FMT_SQLITE:
#ifdef TA_DATA_HAVE_SQLITE
            rc = sql_reader_open(r, opts, err);
#else
            rc = td_fail(err, TA_DATA_ENOTSUP, "SQLite is not compiled in");
#endif
            break;
        default:
            rc = TA_DATA_EINVAL;
            break;
    }
    if (rc != 0)
    {
        td_reader_close(r);
        return rc;
    }
    *out = r;
    return 0;
}

void
td_reader_close(td_reader *r)
{
    if (r == NULL)
        return;
    if (r->close != NULL && r->priv != NULL)
        r->close(r);
    free(r->unexpected_names);
    free(r);
}

/* ---- schema inference --------------------------------------------- */

/* Candidate types, narrowest first; a sample value removes the ones it
 * does not parse as. */
enum {
    CAND_BOOL = 1 << 0,
    CAND_INT = 1 << 1,
    CAND_FLOAT = 1 << 2,
    CAND_DATE = 1 << 3,
    CAND_TS = 1 << 4,
    CAND_ALL = 0x1f,
};

typedef struct infer_col {
    unsigned int    cand;
    bool            seen;       /* a non-null value */
    bool            nulls;
    int             json_kind;  /* JSON: type seen, or -1 mixed */
    bool            json_mixed_num;
} infer_col;

static unsigned int
text_candidates(const char *s, size_t len)
{
    unsigned int c = 0;
    td_type t;
    ta_data_cell cell;

    memset(&t, 0, sizeof(t));
    memset(&cell, 0, sizeof(cell));
    if ((len == 4 && strncasecmp(s, "true", 4) == 0) ||
        (len == 5 && strncasecmp(s, "false", 5) == 0))
        c |= CAND_BOOL;
    t.kind = TA_DATA_T_INT;
    if (td_parse_text(s, len, &t, &cell) == 0)
        c |= CAND_INT | CAND_FLOAT;
    t.kind = TA_DATA_T_FLOAT;
    if (!(c & CAND_FLOAT) && td_parse_text(s, len, &t, &cell) == 0)
        c |= CAND_FLOAT;
    t.kind = TA_DATA_T_DATE;
    if (td_parse_text(s, len, &t, &cell) == 0)
        c |= CAND_DATE | CAND_TS;
    t.kind = TA_DATA_T_TIMESTAMP;
    if (!(c & CAND_TS) && td_parse_text(s, len, &t, &cell) == 0)
        c |= CAND_TS;
    ta_data_cell_clear(&cell);
    return c;
}

static int
cand_kind(unsigned int c)
{
    if (c & CAND_BOOL)
        return TA_DATA_T_BOOL;
    if (c & CAND_INT)
        return TA_DATA_T_INT;
    if (c & CAND_FLOAT)
        return TA_DATA_T_FLOAT;
    if (c & CAND_DATE)
        return TA_DATA_T_DATE;
    if (c & CAND_TS)
        return TA_DATA_T_TIMESTAMP;
    return TA_DATA_T_STRING;
}

static int
infer_csv(ta_data_ds *ds, size_t rows, char **err)
{
    csv_tok t;
    bool header = td_kv_bool(&ds->opts, "header", true);
    const char *null_token = td_kv_get(&ds->opts, "null", NULL);
    td_buf *first;
    size_t nfirst;
    infer_col *ic = NULL;
    size_t got = 0;
    size_t i;
    int rc;

    rc = csv_tok_open(&t, ds, err);
    if (rc != 0)
        return rc;
    rc = csv_header(&t, ds, header, &first, &nfirst, err);
    if (rc != 0)
        goto out;
    ic = calloc(ds->nsrc + 1, sizeof(*ic));
    if (ic == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    for (i = 0; i < ds->nsrc; i++)
        ic[i].cand = CAND_ALL;
    for (;;)
    {
        if (!(got == 0 && !header))
        {
            if (got >= rows)
                break;
            rc = csv_next(&t);
            if (rc == 1)
            {
                rc = 0;
                break;
            }
            if (rc != 0)
            {
                rc = 0;         /* reported by the scan, not here */
                continue;
            }
        }
        got++;
        if (t.nfields != ds->nsrc)
            continue;
        for (i = 0; i < ds->nsrc; i++)
        {
            td_buf *f = &t.fields[i];

            if ((!t.quoted[i] && f->len == 0) ||
                (null_token != NULL && !t.quoted[i] &&
                 strlen(null_token) == f->len &&
                 memcmp(null_token, f->ptr, f->len) == 0))
            {
                ic[i].nulls = true;
                continue;
            }
            ic[i].seen = true;
            ic[i].cand &= text_candidates(f->ptr, f->len);
        }
    }
    for (i = 0; i < ds->nsrc && rc == 0; i++)
    {
        int kind = ic[i].seen ? cand_kind(ic[i].cand) : TA_DATA_T_STRING;

        rc = td_schema_add(&ds->schema, ds->src_names[i],
                           td_type_simple(kind), ic[i].nulls || !ic[i].seen);
    }
out:
    free(ic);
    csv_tok_free(&t);
    return rc;
}

static int
infer_json(ta_data_ds *ds, size_t rows, char **err)
{
    td_reader rd;
    json_reader *jr = calloc(1, sizeof(*jr));
    infer_col *ic = NULL;
    char **names = NULL;
    size_t n = 0;
    size_t got = 0;
    size_t i;
    int rc;

    if (jr == NULL)
        return TA_DATA_ENOMEM;
    memset(&rd, 0, sizeof(rd));
    rd.ds = ds;
    rd.priv = jr;
    jr->lines = ds->format == TD_FMT_JSONL;
    rc = jr->lines ? in_open(&jr->in, ds->path, err) :
                     in_open(&jr->split.in, ds->path, err);
    while (rc == 0 && got < rows)
    {
        json_t *obj;
        const char *key;
        json_t *v;
        uint64_t recno;
        char *e2 = NULL;

        rc = json_next_object(jr, &rd, &obj, &recno, &e2);
        free(e2);
        if (rc == 1)
        {
            rc = 0;
            break;
        }
        if (rc == TA_DATA_EILSEQ)
        {
            rc = 0;
            if (!jr->lines && jr->split.done)
                break;
            continue;
        }
        if (rc != 0)
        {
            if (rc == TA_DATA_ECORRUPT && got > 0)
                rc = 0;     /* truncated tail: the scan reports it */
            else
                td_fail(err, rc, "'%s' is not JSON records", ds->path);
            break;
        }
        got++;
        /* New keys are appended in first-seen order. */
        json_object_foreach(obj, key, v)
        {
            int kind;

            for (i = 0; i < n; i++)
            {
                if (strcmp(names[i], key) == 0)
                    break;
            }
            if (i == n)
            {
                char **nn = realloc(names, (n + 1) * sizeof(*nn));
                infer_col *ni = realloc(ic, (n + 1) * sizeof(*ni));

                if (nn == NULL || ni == NULL)
                {
                    free(nn == NULL ? NULL : nn);
                    rc = TA_DATA_ENOMEM;
                    break;
                }
                names = nn;
                ic = ni;
                names[n] = td_strdup(key);
                memset(&ic[n], 0, sizeof(ic[n]));
                ic[n].cand = CAND_DATE | CAND_TS;
                ic[n].json_kind = 0;
                /* Seen only from this record on: earlier ones lacked it. */
                ic[n].nulls = got > 1;
                n++;
            }
            if (json_is_null(v))
            {
                ic[i].nulls = true;
                continue;
            }
            kind = json_is_string(v) ? TA_DATA_T_STRING :
                   json_is_integer(v) ? TA_DATA_T_INT :
                   json_is_real(v) ? TA_DATA_T_FLOAT :
                   json_is_boolean(v) ? TA_DATA_T_BOOL :
                   json_is_array(v) ? TA_DATA_T_ARRAY : TA_DATA_T_OBJECT;
            if (kind == TA_DATA_T_STRING)
                ic[i].cand &= text_candidates(json_string_value(v),
                                              json_string_length(v));
            if (ic[i].json_kind == 0)
                ic[i].json_kind = kind;
            else if (ic[i].json_kind != kind)
            {
                if ((ic[i].json_kind == TA_DATA_T_INT ||
                     ic[i].json_kind == TA_DATA_T_FLOAT) &&
                    (kind == TA_DATA_T_INT || kind == TA_DATA_T_FLOAT))
                    ic[i].json_kind = TA_DATA_T_FLOAT;
                else
                    ic[i].json_kind = -1;
            }
            ic[i].seen = true;
        }
        /* Keys absent from this record are NULL in it. */
        for (i = 0; i < n; i++)
        {
            if (json_object_get(obj, names[i]) == NULL)
                ic[i].nulls = true;
        }
        json_decref(obj);
    }
    if (rc == 0 && n == 0 && got == 0)
        rc = td_fail(err, TA_DATA_ECORRUPT, "'%s' has no records to infer "
                     "a schema from", ds->path);
    for (i = 0; i < n && rc == 0; i++)
    {
        int kind = ic[i].json_kind;

        if (kind <= 0)
            kind = TA_DATA_T_STRING;
        if (kind == TA_DATA_T_STRING && ic[i].seen)
        {
            if (ic[i].cand & CAND_DATE)
                kind = TA_DATA_T_DATE;
            else if (ic[i].cand & CAND_TS)
                kind = TA_DATA_T_TIMESTAMP;
        }
        rc = td_schema_add(&ds->schema, names[i], td_type_simple(kind),
                           ic[i].nulls || !ic[i].seen);
    }
    if (rc == 0)
    {
        ds->nsrc = n;
        ds->src_names = names;
        names = NULL;
    }
    if (names != NULL)
        td_split_free(names, n);
    free(ic);
    in_close(&jr->split.in);
    td_buf_free(&jr->split.elem);
    in_close(&jr->in);
    td_buf_free(&jr->line);
    free(jr);
    return rc;
}

static int
infer_parquet(ta_data_ds *ds, bool want_schema, char **err)
{
    void *h = NULL;
    int ncols = 0;
    int k;
    int rc = ta_data_pq_open(ds->path, 1024, &h, &ncols, err);

    if (rc != 0)
        return rc;
    ds->src_names = calloc((size_t)ncols + 1, sizeof(char *));
    if (ds->src_names == NULL)
    {
        ta_data_pq_close(h);
        return TA_DATA_ENOMEM;
    }
    ds->nsrc = (size_t)ncols;
    for (k = 0; k < ncols && rc == 0; k++)
    {
        ta_data_pq_col col;
        td_type *t = NULL;

        memset(&col, 0, sizeof(col));
        rc = ta_data_pq_column(h, k, &col);
        if (rc != 0)
            break;
        ds->src_names[k] = td_strdup(col.name);
        if (want_schema)
        {
            if (col.type_text == NULL ||
                td_type_parse(col.type_text, &t, NULL) != 0)
                t = td_type_simple(col.type == 0 ? TA_DATA_T_STRING :
                                   col.type);
            rc = td_schema_add(&ds->schema, col.name, t, col.nullable != 0);
        }
        free(col.name);
        free(col.type_text);
    }
    ta_data_pq_close(h);
    return rc;
}

#ifdef TA_DATA_HAVE_SQLITE
static int
sql_decl_kind(const char *decl, int storage, int *precision, int *scale)
{
    char up[64];
    size_t i;

    *precision = 18;
    *scale = 0;
    if (decl == NULL)
    {
        switch (storage)
        {
            case SQLITE_INTEGER: return TA_DATA_T_INT;
            case SQLITE_FLOAT: return TA_DATA_T_FLOAT;
            case SQLITE_BLOB: return TA_DATA_T_BINARY;
            default: return TA_DATA_T_STRING;
        }
    }
    for (i = 0; i + 1 < sizeof(up) && decl[i] != '\0'; i++)
        up[i] = (char)toupper((unsigned char)decl[i]);
    up[i] = '\0';
    if (strstr(up, "BOOL") != NULL)
        return TA_DATA_T_BOOL;
    if (strstr(up, "INT") != NULL)
        return TA_DATA_T_INT;
    if (strstr(up, "DECIMAL") != NULL || strstr(up, "NUMERIC") != NULL)
    {
        const char *p = strchr(up, '(');

        if (p != NULL && sscanf(p, "(%d,%d)", precision, scale) >= 1 &&
            *precision >= 1 && *precision <= 18 && *scale >= 0 &&
            *scale <= *precision)
            return TA_DATA_T_DECIMAL;
        *precision = 18;
        *scale = 0;
        return TA_DATA_T_FLOAT;
    }
    if (strstr(up, "REAL") != NULL || strstr(up, "FLOA") != NULL ||
        strstr(up, "DOUB") != NULL)
        return TA_DATA_T_FLOAT;
    if (strstr(up, "TIMESTAMP") != NULL || strstr(up, "DATETIME") != NULL)
        return TA_DATA_T_TIMESTAMP;
    if (strstr(up, "DATE") != NULL)
        return TA_DATA_T_DATE;
    if (strstr(up, "BLOB") != NULL)
        return TA_DATA_T_BINARY;
    return TA_DATA_T_STRING;
}

static int
infer_sqlite(ta_data_ds *ds, bool want_schema, char **err)
{
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int n;
    int k;
    int rc = sql_prepare(ds, &ds->opts, &db, &st, err);
    bool row;

    if (rc != 0)
        return rc;
    row = sqlite3_step(st) == SQLITE_ROW;
    n = sqlite3_column_count(st);
    ds->src_names = calloc((size_t)n + 1, sizeof(char *));
    ds->nsrc = (size_t)n;
    for (k = 0; k < n && rc == 0 && ds->src_names != NULL; k++)
    {
        int p;
        int s;
        int kind = sql_decl_kind(sqlite3_column_decltype(st, k),
                                 row ? sqlite3_column_type(st, k) :
                                 SQLITE_TEXT, &p, &s);
        td_type *t;

        ds->src_names[k] = td_strdup(sqlite3_column_name(st, k));
        if (!want_schema)
            continue;
        t = td_type_simple(kind);
        if (kind == TA_DATA_T_DECIMAL)
        {
            t->precision = p;
            t->scale = s;
        }
        rc = td_schema_add(&ds->schema, ds->src_names[k], t, true);
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return rc;
}
#endif

int
td_infer_schema(ta_data_ds *ds, char **err)
{
    size_t rows = (size_t)td_kv_int(&ds->opts, "infer_rows", 1000);
    bool want = ds->schema.n == 0;

    if (rows == 0)
        rows = 1;
    switch (ds->format)
    {
        case TD_FMT_CSV:
            if (!want)
            {
                csv_tok t;
                td_buf *first;
                size_t nfirst;
                int rc = csv_tok_open(&t, ds, err);

                if (rc == 0)
                    rc = csv_header(&t, ds, td_kv_bool(&ds->opts, "header",
                                                       true),
                                    &first, &nfirst, err);
                csv_tok_free(&t);
                return rc;
            }
            return infer_csv(ds, rows, err);
        case TD_FMT_JSON:
        case TD_FMT_JSONL:
            if (!want)
            {
                /* The source columns are what the schema names; extra
                 * keys are counted by the reader as unexpected. */
                size_t i;

                ds->src_names = calloc(ds->schema.n + 1, sizeof(char *));
                if (ds->src_names == NULL)
                    return TA_DATA_ENOMEM;
                ds->nsrc = ds->schema.n;
                for (i = 0; i < ds->schema.n; i++)
                    ds->src_names[i] = td_strdup(ds->schema.cols[i].name);
                return 0;
            }
            return infer_json(ds, rows, err);
        case TD_FMT_PARQUET:
            if (!ta_data_pq_supported())
                return td_fail(err, TA_DATA_ENOTSUP,
                               "Parquet (Arrow C++) is not compiled in");
            return infer_parquet(ds, want, err);
        case TD_FMT_SQLITE:
#ifdef TA_DATA_HAVE_SQLITE
            return infer_sqlite(ds, want, err);
#else
            return td_fail(err, TA_DATA_ENOTSUP, "SQLite is not compiled in");
#endif
        default:
            return TA_DATA_EINVAL;
    }
}
