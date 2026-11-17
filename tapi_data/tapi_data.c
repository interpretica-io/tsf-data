/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Dataset validation TAPI
 *
 * The engine side of tsf-data: option strings in, results parsed out,
 * thresholds applied and verdicts emitted. Everything heavy runs on the
 * agent; see tapi_data.h.
 */

#define TE_LGR_USER     "TAPI DATA"

#include "te_config.h"

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "te_string.h"
#include "logger_api.h"
#include "tapi_test_log.h"

#include "tapi_data.h"
#include "tapi_data_rpc.h"

const tapi_data_threshold TAPI_DATA_NONE_ALLOWED = { 0, -1, -1 };

static te_string last_error = TE_STRING_INIT;

/* See description in tapi_data.h */
const char *
tapi_data_last_error(void)
{
    return last_error.ptr != NULL ? last_error.ptr : "";
}

static char *
xstrdup(const char *s)
{
    return strdup(s == NULL ? "" : s);
}

/* ---- escaping and key=value --------------------------------------- */

static int
hexval(int c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    c = tolower(c);
    return c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

static char *
unescape_n(const char *s, size_t n)
{
    char *out = malloc(n + 1);
    size_t i;
    size_t k = 0;

    if (out == NULL)
        return NULL;
    for (i = 0; i < n; i++)
    {
        if (s[i] == '%' && i + 2 < n &&
            hexval((unsigned char)s[i + 1]) >= 0 &&
            hexval((unsigned char)s[i + 2]) >= 0)
        {
            out[k++] = (char)(hexval((unsigned char)s[i + 1]) * 16 +
                              hexval((unsigned char)s[i + 2]));
            i += 2;
        }
        else
            out[k++] = s[i];
    }
    out[k] = '\0';
    return out;
}

/* See description in tapi_data.h */
char *
tapi_data_unescape(const char *s)
{
    return unescape_n(s == NULL ? "" : s, s == NULL ? 0 : strlen(s));
}

static void
kv_add(tapi_data_kv *kv, const char *k, size_t kl, const char *v, size_t vl)
{
    char **nk = realloc(kv->key, (kv->n + 1) * sizeof(*nk));
    char **nv;

    if (nk == NULL)
        return;
    kv->key = nk;
    nv = realloc(kv->val, (kv->n + 1) * sizeof(*nv));
    if (nv == NULL)
        return;
    kv->val = nv;
    kv->key[kv->n] = strndup(k, kl);
    kv->val[kv->n] = unescape_n(v, vl);
    kv->n++;
}

/* Parse "k=v" lines; lines that are per-column records are skipped. */
static void
kv_parse(tapi_data_kv *kv, const char *text)
{
    const char *p = text;

    while (p != NULL && *p != '\0')
    {
        const char *e = strchr(p, '\n');
        size_t len = e != NULL ? (size_t)(e - p) : strlen(p);
        const char *eq = memchr(p, '=', len);

        if (eq != NULL && memchr(p, '\t', len) == NULL)
            kv_add(kv, p, (size_t)(eq - p), eq + 1, len - (size_t)(eq - p) - 1);
        p = e != NULL ? e + 1 : NULL;
    }
}

/* See description in tapi_data.h */
void
tapi_data_kv_parse(tapi_data_kv *kv, const char *text)
{
    kv_parse(kv, text);
}

/* See description in tapi_data.h */
const char *
tapi_data_kv_get(const tapi_data_kv *kv, const char *key, const char *def)
{
    size_t i;

    for (i = kv->n; i > 0; i--)
    {
        if (strcmp(kv->key[i - 1], key) == 0)
            return kv->val[i - 1];
    }
    return def;
}

/* See description in tapi_data.h */
int64_t
tapi_data_kv_int(const tapi_data_kv *kv, const char *key, int64_t def)
{
    const char *v = tapi_data_kv_get(kv, key, NULL);
    char *end;
    long long n;

    if (v == NULL || *v == '\0')
        return def;
    n = strtoll(v, &end, 10);
    return end == v ? def : n;
}

/* See description in tapi_data.h */
double
tapi_data_kv_dbl(const tapi_data_kv *kv, const char *key, double def)
{
    const char *v = tapi_data_kv_get(kv, key, NULL);
    char *end;
    double d;

    if (v == NULL || *v == '\0')
        return def;
    d = strtod(v, &end);
    return end == v ? def : d;
}

/* See description in tapi_data.h */
void
tapi_data_kv_free(tapi_data_kv *kv)
{
    size_t i;

    for (i = 0; i < kv->n; i++)
    {
        free(kv->key[i]);
        free(kv->val[i]);
    }
    free(kv->key);
    free(kv->val);
    memset(kv, 0, sizeof(*kv));
}

/* See description in tapi_data.h */
size_t
tapi_data_fields(const char *line, char ***fields)
{
    char **v = NULL;
    size_t n = 0;
    const char *p = line;

    for (;;)
    {
        const char *t = strchr(p, '\t');
        size_t len = t != NULL ? (size_t)(t - p) : strcspn(p, "\n");
        char **nv = realloc(v, (n + 1) * sizeof(*nv));

        if (nv == NULL)
            break;
        v = nv;
        v[n++] = unescape_n(p, len);
        if (t == NULL)
            break;
        p = t + 1;
    }
    *fields = v;
    return n;
}

/* See description in tapi_data.h */
void
tapi_data_fields_free(char **fields, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++)
        free(fields[i]);
    free(fields);
}

/* "a=1;b=2" -> "a=1\nb=2" (a convenience for test code). */
static void
params_add(te_string *dst, const char *params)
{
    const char *p;

    if (params == NULL)
        return;
    for (p = params; *p != '\0'; p++)
        te_string_append(dst, "%c", *p == ';' ? '\n' : *p);
    te_string_append(dst, "\n");
}

/* Run an RPC; on failure remember and log the agent's reason. */
static te_errno
call(rcf_rpc_server *rpcs, tapi_data_rpc_op op, const tapi_data_rpc_args *a,
     tapi_data_rpc_res *r)
{
    te_errno rc = rpc_data_call(rpcs, op, a, 0, r);

    if (rc != 0)
    {
        te_string_reset(&last_error);
        te_string_append(&last_error, "%s",
                         r->errmsg.ptr != NULL ? r->errmsg.ptr : "");
        ERROR("Dataset operation failed: %r%s%s", rc,
              last_error.len > 0 ? ": " : "",
              last_error.len > 0 ? last_error.ptr : "");
    }
    return rc;
}

/* ---- availability ------------------------------------------------- */

static te_errno
available(rcf_rpc_server *rpcs, unsigned int *features, te_string *versions)
{
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    te_errno rc = rpc_data_call(rpcs, TAPI_DATA_RPC_AVAILABLE, &a, 0, &r);

    if (rc == 0)
    {
        if (features != NULL)
            *features = r.features;
        if (versions != NULL && r.result.ptr != NULL)
            te_string_append(versions, "%s", r.result.ptr);
    }
    tapi_data_rpc_res_free(&r);
    return rc;
}

/* See description in tapi_data.h */
te_bool
tapi_data_available(rcf_rpc_server *rpcs)
{
    return available(rpcs, NULL, NULL) == 0;
}

/* See description in tapi_data.h */
te_bool
tapi_data_supports(rcf_rpc_server *rpcs, tapi_data_feature feature)
{
    unsigned int f = 0;

    return available(rpcs, &f, NULL) == 0 && (f & feature) != 0;
}

/* See description in tapi_data.h */
te_errno
tapi_data_versions(rcf_rpc_server *rpcs, te_string *out)
{
    return available(rpcs, NULL, out);
}

/* See description in tapi_data.h */
te_errno
tapi_data_get_stats(rcf_rpc_server *rpcs, tapi_data_stats *stats)
{
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    tapi_data_kv kv = TAPI_DATA_KV_INIT;
    te_errno rc = call(rpcs, TAPI_DATA_RPC_STATS, &a, &r);

    if (rc == 0)
    {
        kv_parse(&kv, r.result.ptr);
        stats->open = (unsigned int)tapi_data_kv_int(&kv, "open", 0);
        stats->ops = (uint64_t)tapi_data_kv_int(&kv, "ops", 0);
        stats->rss_kb = (long)tapi_data_kv_int(&kv, "rss_kb", 0);
        stats->maxrss_kb = (long)tapi_data_kv_int(&kv, "maxrss_kb", 0);
        tapi_data_kv_free(&kv);
    }
    tapi_data_rpc_res_free(&r);
    return rc;
}

/* ---- datasets ----------------------------------------------------- */

static void
schema_parse(const char *text, tapi_data_schema_info *s)
{
    const char *p = text;

    memset(s, 0, sizeof(*s));
    while (p != NULL && *p != '\0')
    {
        const char *e = strchr(p, '\n');
        size_t len = e != NULL ? (size_t)(e - p) : strlen(p);
        char *line = strndup(p, len);
        char **f;
        size_t n = tapi_data_fields(line, &f);

        if (n >= 3)
        {
            tapi_data_column *nc = realloc(s->cols, (s->n + 1) *
                                           sizeof(*nc));

            if (nc != NULL)
            {
                s->cols = nc;
                s->cols[s->n].name = xstrdup(f[0]);
                s->cols[s->n].type = xstrdup(f[1]);
                s->cols[s->n].nullable = strcmp(f[2], "1") == 0;
                s->n++;
                s->inferred = n >= 4 && strcmp(f[3], "inferred") == 0;
            }
        }
        tapi_data_fields_free(f, n);
        free(line);
        p = e != NULL ? e + 1 : NULL;
    }
}

/* See description in tapi_data.h */
void
tapi_data_schema_free(tapi_data_schema_info *s)
{
    size_t i;

    for (i = 0; i < s->n; i++)
    {
        free(s->cols[i].name);
        free(s->cols[i].type);
    }
    free(s->cols);
    memset(s, 0, sizeof(*s));
}

/* See description in tapi_data.h */
int
tapi_data_schema_find(const tapi_data_schema_info *s, const char *name)
{
    size_t i;

    for (i = 0; i < s->n; i++)
    {
        if (strcmp(s->cols[i].name, name) == 0)
            return (int)i;
    }
    return -1;
}

/* See description in tapi_data.h */
te_errno
tapi_data_open(rcf_rpc_server *rpcs, const char *path, const char *format,
               const char *params, const char *schema, tapi_data **ds)
{
    te_string p = TE_STRING_INIT;
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    tapi_data *d;
    te_errno rc;

    *ds = NULL;
    params_add(&p, params);
    a.path = path;
    a.format = format;
    a.params = p.ptr;
    a.spec = schema;
    rc = call(rpcs, TAPI_DATA_RPC_OPEN, &a, &r);
    te_string_free(&p);
    if (rc != 0)
    {
        tapi_data_rpc_res_free(&r);
        return rc;
    }
    d = calloc(1, sizeof(*d));
    if (d == NULL)
    {
        tapi_data_rpc_res_free(&r);
        return TE_RC(TE_TAPI, TE_ENOMEM);
    }
    d->rpcs = rpcs;
    d->handle = r.handle;
    d->path = xstrdup(path);
    d->format = xstrdup(format);
    schema_parse(r.result.ptr, &d->schema);
    RING("Opened dataset %s on %s: %zu columns (%s schema)", path, rpcs->ta,
         d->schema.n, d->schema.inferred ? "inferred" : "explicit");
    tapi_data_rpc_res_free(&r);
    *ds = d;
    return 0;
}

/* See description in tapi_data.h */
te_errno
tapi_data_close(tapi_data *ds)
{
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    te_errno rc;

    if (ds == NULL)
        return 0;
    a.ha = ds->handle;
    rc = call(ds->rpcs, TAPI_DATA_RPC_CLOSE, &a, &r);
    tapi_data_rpc_res_free(&r);
    tapi_data_schema_free(&ds->schema);
    free(ds->path);
    free(ds->format);
    free(ds);
    return rc;
}

/* See description in tapi_data.h */
void
tapi_data_forget(tapi_data *ds)
{
    if (ds == NULL)
        return;
    tapi_data_schema_free(&ds->schema);
    free(ds->path);
    free(ds->format);
    free(ds);
}

/* See description in tapi_data.h */
te_errno
tapi_data_schema(tapi_data *ds, tapi_data_schema_info *out)
{
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    te_errno rc;

    a.ha = ds->handle;
    rc = call(ds->rpcs, TAPI_DATA_RPC_SCHEMA, &a, &r);
    if (rc == 0)
        schema_parse(r.result.ptr, out);
    tapi_data_rpc_res_free(&r);
    return rc;
}

/* One-dataset operation with params: result into kv, report kept. */
static te_errno
simple_op(tapi_data *ds, tapi_data *b, tapi_data_rpc_op op, const char *kind,
          const char *path, const char *format, const char *params,
          tapi_data_kv *kv, te_string *report)
{
    te_string p = TE_STRING_INIT;
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    te_errno rc;

    params_add(&p, params);
    a.ha = ds->handle;
    a.hb = b != NULL ? b->handle : 0;
    a.kind = kind;
    a.path = path;
    a.format = format;
    a.params = p.ptr;
    rc = call(ds->rpcs, op, &a, &r);
    te_string_free(&p);
    if (rc == 0)
    {
        if (kv != NULL)
            kv_parse(kv, r.result.ptr);
        if (report != NULL && r.report.ptr != NULL)
            te_string_append(report, "%s", r.report.ptr);
        /* Profile column records live in the result itself. */
        if (report != NULL && op == TAPI_DATA_RPC_PROFILE &&
            r.result.ptr != NULL)
            te_string_append(report, "%s", r.result.ptr);
    }
    tapi_data_rpc_res_free(&r);
    return rc;
}

#define KVU(_kv, _k) ((uint64_t)tapi_data_kv_int((_kv), (_k), 0))

/* See description in tapi_data.h */
te_errno
tapi_data_count(tapi_data *ds, const char *params, tapi_data_counts *out)
{
    te_errno rc;

    memset(out, 0, sizeof(*out));
    rc = simple_op(ds, NULL, TAPI_DATA_RPC_COUNT, NULL, NULL, NULL, params,
                   &out->kv, NULL);
    if (rc != 0)
        return rc;
    out->rows = KVU(&out->kv, "rows");
    out->malformed = KVU(&out->kv, "malformed");
    out->mismatched = KVU(&out->kv, "mismatched");
    out->coerced = KVU(&out->kv, "coerced");
    out->unexpected = KVU(&out->kv, "unexpected");
    out->null_cells = KVU(&out->kv, "null_cells");
    if (tapi_data_kv_get(&out->kv, "first_error", NULL) != NULL)
        out->first_error = xstrdup(tapi_data_kv_get(&out->kv, "first_error",
                                                    ""));
    RING("%s: %llu rows, %llu malformed, %llu mismatched, %llu NULL cells%s%s",
         ds->path, (unsigned long long)out->rows,
         (unsigned long long)out->malformed,
         (unsigned long long)out->mismatched,
         (unsigned long long)out->null_cells,
         out->first_error != NULL ? "; first error: " : "",
         out->first_error != NULL ? out->first_error : "");
    return 0;
}

/* See description in tapi_data.h */
void
tapi_data_counts_free(tapi_data_counts *c)
{
    free(c->first_error);
    tapi_data_kv_free(&c->kv);
    memset(c, 0, sizeof(*c));
}

/* See description in tapi_data.h */
te_errno
tapi_data_sample(tapi_data *ds, const char *params, te_string *jsonl,
                 unsigned int *returned)
{
    tapi_data_kv kv = TAPI_DATA_KV_INIT;
    te_errno rc = simple_op(ds, NULL, TAPI_DATA_RPC_SAMPLE, NULL, NULL, NULL,
                            params, &kv, jsonl);

    if (rc == 0 && returned != NULL)
        *returned = (unsigned int)tapi_data_kv_int(&kv, "returned", 0);
    tapi_data_kv_free(&kv);
    return rc;
}

/* ---- profile ------------------------------------------------------ */

static void
col_profile_parse(const char *line, tapi_data_col_profile *c)
{
    const char *v;

    memset(c, 0, sizeof(*c));
    /* Each tab-separated field is key=value, the value escaped. */
    {
        const char *p = line;

        while (p != NULL && *p != '\0')
        {
            const char *t = strchr(p, '\t');
            size_t len = t != NULL ? (size_t)(t - p) : strcspn(p, "\n");
            const char *eq = memchr(p, '=', len);

            if (eq != NULL)
                kv_add(&c->kv, p, (size_t)(eq - p), eq + 1,
                       len - (size_t)(eq - p) - 1);
            p = t != NULL ? t + 1 : NULL;
        }
    }
    c->name = xstrdup(tapi_data_kv_get(&c->kv, "column", ""));
    c->type = xstrdup(tapi_data_kv_get(&c->kv, "type", ""));
    c->count = KVU(&c->kv, "count");
    c->nulls = KVU(&c->kv, "nulls");
    c->bad = KVU(&c->kv, "bad");
    c->null_pct = tapi_data_kv_dbl(&c->kv, "null_pct", 0);
    c->distinct = tapi_data_kv_dbl(&c->kv, "distinct", 0);
    c->distinct_algo = xstrdup(tapi_data_kv_get(&c->kv, "distinct_algo", ""));
    c->distinct_exact = strncmp(c->distinct_algo, "exact", 5) == 0;
    c->distinct_err = tapi_data_kv_dbl(&c->kv, "distinct_err", 0);
    v = tapi_data_kv_get(&c->kv, "min", NULL);
    c->min = v != NULL ? xstrdup(v) : NULL;
    v = tapi_data_kv_get(&c->kv, "max", NULL);
    c->max = v != NULL ? xstrdup(v) : NULL;
    c->has_moments = tapi_data_kv_get(&c->kv, "mean", NULL) != NULL;
    c->mean = tapi_data_kv_dbl(&c->kv, "mean", NAN);
    c->stddev = tapi_data_kv_dbl(&c->kv, "stddev", NAN);
    c->stddev_pop = tapi_data_kv_dbl(&c->kv, "stddev_pop", NAN);
    c->sum = tapi_data_kv_dbl(&c->kv, "sum", NAN);
    c->q_algo = xstrdup(tapi_data_kv_get(&c->kv, "q_algo", ""));
    c->q_exact = strncmp(c->q_algo, "exact", 5) == 0;
    c->q_err = tapi_data_kv_dbl(&c->kv, "q_err", 0);
    v = tapi_data_kv_get(&c->kv, "quantiles", NULL);
    if (v != NULL)
    {
        const char *p = v;

        while (*p != '\0' && c->nq < TAPI_DATA_MAX_Q)
        {
            const char *bar = strchr(p, '|');
            size_t len = bar != NULL ? (size_t)(bar - p) : strlen(p);
            const char *colon = memchr(p, ':', len);

            if (colon != NULL)
            {
                c->q[c->nq] = strtod(p, NULL);
                c->qv[c->nq] = strndup(colon + 1,
                                       len - (size_t)(colon - p) - 1);
                c->nq++;
            }
            if (bar == NULL)
                break;
            p = bar + 1;
        }
    }
    c->has_len = tapi_data_kv_get(&c->kv, "len_min", NULL) != NULL;
    c->len_min = tapi_data_kv_dbl(&c->kv, "len_min", 0);
    c->len_max = tapi_data_kv_dbl(&c->kv, "len_max", 0);
    c->len_mean = tapi_data_kv_dbl(&c->kv, "len_mean", 0);
    c->top_algo = xstrdup(tapi_data_kv_get(&c->kv, "top_algo", ""));
    c->top_exact = strcmp(c->top_algo, "exact") == 0;
    c->top_err = tapi_data_kv_dbl(&c->kv, "top_err", 0);
    /* top= was unescaped as a whole; split it from the raw line. */
    {
        const char *raw = strstr(line, "\ttop=");

        if (raw != NULL)
        {
            const char *p = raw + 5;
            size_t end = strcspn(p, "\t\n");

            while (end > 0 && c->ntop < TAPI_DATA_MAX_TOP)
            {
                const char *bar = memchr(p, '|', end);
                size_t len = bar != NULL ? (size_t)(bar - p) : end;
                const char *colon = NULL;
                size_t k;

                for (k = len; k > 0; k--)
                {
                    if (p[k - 1] == ':')
                    {
                        colon = p + k - 1;
                        break;
                    }
                }
                if (colon != NULL)
                {
                    c->top[c->ntop] = unescape_n(p, (size_t)(colon - p));
                    c->top_count[c->ntop] = strtoull(colon + 1, NULL, 10);
                    c->ntop++;
                }
                if (bar == NULL)
                    break;
                end -= len + 1;
                p = bar + 1;
            }
        }
    }
}

/* See description in tapi_data.h */
te_errno
tapi_data_profile(tapi_data *ds, const char *params,
                  tapi_data_profile_info *out)
{
    te_string lines = TE_STRING_INIT;
    const char *p;
    te_errno rc;

    memset(out, 0, sizeof(*out));
    rc = simple_op(ds, NULL, TAPI_DATA_RPC_PROFILE, NULL, NULL, NULL, params,
                   &out->kv, &lines);
    if (rc != 0)
        return rc;
    out->rows = KVU(&out->kv, "rows");
    out->passes = (unsigned int)tapi_data_kv_int(&out->kv, "passes", 1);
    out->mem_peak = KVU(&out->kv, "mem_peak");
    for (p = lines.ptr; p != NULL && *p != '\0'; )
    {
        const char *e = strchr(p, '\n');
        size_t len = e != NULL ? (size_t)(e - p) : strlen(p);

        if (strncmp(p, "column=", 7) == 0)
        {
            char *line = strndup(p, len);
            tapi_data_col_profile *nc = realloc(out->cols, (out->ncols + 1) *
                                                sizeof(*nc));

            if (nc != NULL)
            {
                out->cols = nc;
                col_profile_parse(line, &out->cols[out->ncols++]);
            }
            free(line);
        }
        p = e != NULL ? e + 1 : NULL;
    }
    te_string_free(&lines);
    return 0;
}

/* See description in tapi_data.h */
const tapi_data_col_profile *
tapi_data_profile_col(const tapi_data_profile_info *p, const char *name)
{
    size_t i;

    for (i = 0; i < p->ncols; i++)
    {
        if (strcmp(p->cols[i].name, name) == 0)
            return &p->cols[i];
    }
    return NULL;
}

/* See description in tapi_data.h */
double
tapi_data_profile_quantile(const tapi_data_col_profile *c, double q)
{
    size_t i;

    for (i = 0; c != NULL && i < c->nq; i++)
    {
        if (fabs(c->q[i] - q) < 1e-12)
            return strtod(c->qv[i], NULL);
    }
    return NAN;
}

/* See description in tapi_data.h */
void
tapi_data_profile_log(const tapi_data_profile_info *p)
{
    te_string s = TE_STRING_INIT;
    size_t i;

    te_string_append(&s, "Profile: %llu rows, %u pass(es), %llu bytes peak\n",
                     (unsigned long long)p->rows, p->passes,
                     (unsigned long long)p->mem_peak);
    for (i = 0; i < p->ncols; i++)
    {
        const tapi_data_col_profile *c = &p->cols[i];

        te_string_append(&s, "  %s (%s): %llu values, %llu NULL, distinct "
                         "%.0f (%s)", c->name, c->type,
                         (unsigned long long)c->count,
                         (unsigned long long)c->nulls, c->distinct,
                         c->distinct_algo);
        if (c->min != NULL)
            te_string_append(&s, ", min %s, max %s", c->min, c->max);
        if (c->has_moments)
            te_string_append(&s, ", mean %.6g, stddev %.6g", c->mean,
                             c->stddev);
        if (c->nq > 0)
            te_string_append(&s, ", quantiles by %s", c->q_algo);
        te_string_append(&s, "\n");
    }
    RING("%s", s.ptr);
    te_string_free(&s);
}

/* See description in tapi_data.h */
void
tapi_data_profile_free(tapi_data_profile_info *p)
{
    size_t i;
    size_t k;

    for (i = 0; i < p->ncols; i++)
    {
        tapi_data_col_profile *c = &p->cols[i];

        free(c->name);
        free(c->type);
        free(c->distinct_algo);
        free(c->min);
        free(c->max);
        free(c->q_algo);
        free(c->top_algo);
        for (k = 0; k < c->nq; k++)
            free(c->qv[k]);
        for (k = 0; k < c->ntop; k++)
            free(c->top[k]);
        tapi_data_kv_free(&c->kv);
    }
    free(p->cols);
    tapi_data_kv_free(&p->kv);
    memset(p, 0, sizeof(*p));
}

/* ---- compare ------------------------------------------------------ */

/* See description in tapi_data.h */
te_errno
tapi_data_compare(tapi_data *a, tapi_data *b, const char *params,
                  tapi_data_diff *out)
{
    te_errno rc;

    te_string_free(&out->report);
    tapi_data_kv_free(&out->kv);
    memset(out, 0, sizeof(*out));
    out->report = (te_string)TE_STRING_INIT;
    rc = simple_op(a, b, TAPI_DATA_RPC_COMPARE, NULL, NULL, NULL, params,
                   &out->kv, &out->report);
    if (rc != 0)
        return rc;
    out->rows_a = KVU(&out->kv, "rows_a");
    out->rows_b = KVU(&out->kv, "rows_b");
    out->matched = KVU(&out->kv, "matched");
    out->missing = KVU(&out->kv, "missing");
    out->extra = KVU(&out->kv, "extra");
    out->mismatched = KVU(&out->kv, "mismatched");
    out->dup_a = KVU(&out->kv, "dup_a");
    out->dup_b = KVU(&out->kv, "dup_b");
    out->missing_columns = KVU(&out->kv, "missing_columns");
    out->extra_columns = KVU(&out->kv, "extra_columns");
    out->type_diffs = KVU(&out->kv, "type_diffs");
    out->equal = (int)tapi_data_kv_int(&out->kv, "equal", 0);
    out->partitions = (unsigned int)tapi_data_kv_int(&out->kv, "partitions",
                                                     0);
    out->mem_peak = KVU(&out->kv, "mem_peak");
    out->report_total = KVU(&out->kv, "report_total");
    snprintf(out->digest, sizeof(out->digest), "%s",
             tapi_data_kv_get(&out->kv, "report_digest", ""));
    return 0;
}

/* See description in tapi_data.h */
void
tapi_data_diff_log(const tapi_data_diff *d)
{
    RING("Compare (%s): A %llu rows, B %llu rows; matched %llu, missing "
         "%llu, extra %llu, mismatched %llu, duplicates %llu/%llu, columns "
         "missing %llu extra %llu, type differences %llu; %llu differences "
         "(digest %s), %u partition(s)\n%s",
         tapi_data_kv_get(&d->kv, "mode", "?"),
         (unsigned long long)d->rows_a, (unsigned long long)d->rows_b,
         (unsigned long long)d->matched, (unsigned long long)d->missing,
         (unsigned long long)d->extra, (unsigned long long)d->mismatched,
         (unsigned long long)d->dup_a, (unsigned long long)d->dup_b,
         (unsigned long long)d->missing_columns,
         (unsigned long long)d->extra_columns,
         (unsigned long long)d->type_diffs,
         (unsigned long long)d->report_total, d->digest, d->partitions,
         d->report.ptr != NULL ? d->report.ptr : "");
}

/* See description in tapi_data.h */
void
tapi_data_diff_free(tapi_data_diff *d)
{
    tapi_data_kv_free(&d->kv);
    te_string_free(&d->report);
}

/* ---- checks ------------------------------------------------------- */

/* See description in tapi_data.h */
te_errno
tapi_data_check_run(tapi_data *ds, tapi_data *ref, const char *kind,
                    const char *target, const char *params,
                    const tapi_data_threshold *thr, tapi_data_check *res)
{
    te_string sum = TE_STRING_INIT;
    te_errno rc;

    if (thr == NULL)
        thr = &TAPI_DATA_NONE_ALLOWED;
    tapi_data_kv_free(&res->kv);
    te_string_free(&res->report);
    free(res->kind);
    free(res->target);
    free(res->summary);
    memset(res, 0, sizeof(*res));
    res->report = (te_string)TE_STRING_INIT;
    res->kind = xstrdup(kind);
    res->target = xstrdup(target);
    res->thr = *thr;
    rc = simple_op(ds, ref, TAPI_DATA_RPC_CHECK, kind, NULL, NULL, params,
                   &res->kv, &res->report);
    if (rc != 0)
        return rc;
    res->checked = KVU(&res->kv, "checked");
    res->violations = KVU(&res->kv, "violations");
    res->fraction = res->checked == 0 ? 0 :
                    (double)res->violations / (double)res->checked;
    res->stat = tapi_data_kv_dbl(&res->kv, "max_stat",
                                 tapi_data_kv_dbl(&res->kv, "tvd", 0));
    res->passed = true;
    if (thr->max_count >= 0 && (int64_t)res->violations > thr->max_count)
        res->passed = false;
    if (thr->max_fraction >= 0 && res->fraction > thr->max_fraction)
        res->passed = false;
    if (thr->max_stat >= 0 && res->stat > thr->max_stat)
        res->passed = false;

    /* Stable text: counts and the threshold, never a path or a time. */
    te_string_append(&sum, "%s check of %s: ", kind,
                     target != NULL && *target != '\0' ? target :
                     "the dataset");
    if (thr->max_stat >= 0)
        te_string_append(&sum, "statistic %.4f (limit %.4f)", res->stat,
                         thr->max_stat);
    else
        te_string_append(&sum, "%llu violation(s) in %llu row(s)",
                         (unsigned long long)res->violations,
                         (unsigned long long)res->checked);
    if (thr->max_count >= 0 && thr->max_stat < 0)
        te_string_append(&sum, ", %lld allowed", (long long)thr->max_count);
    if (thr->max_fraction >= 0)
        te_string_append(&sum, ", fraction %.4f allowed", thr->max_fraction);
    res->summary = xstrdup(sum.ptr);
    te_string_free(&sum);
    return 0;
}

static te_errno
check_with(tapi_data *ds, tapi_data *ref, const char *kind, const char *target,
           const char *extra, const char *params,
           const tapi_data_threshold *thr, tapi_data_check *res)
{
    te_string p = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&p, "%s\n", extra);
    params_add(&p, params);
    rc = tapi_data_check_run(ds, ref, kind, target, p.ptr, thr, res);
    te_string_free(&p);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_check_schema(tapi_data *ds, const char *schema, const char *params,
                       const tapi_data_threshold *thr, tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    const char *p;
    te_errno rc;

    /* The schema goes as one value: commas between columns stay, any
     * newline becomes a comma. */
    te_string_append(&e, "schema=");
    for (p = schema; p != NULL && *p != '\0'; p++)
        te_string_append(&e, "%c", *p == '\n' ? ',' : *p);
    rc = check_with(ds, NULL, "schema", "the schema", e.ptr, params, thr, res);
    te_string_free(&e);
    return rc;
}

#define ONE_COLUMN_CHECK(_fn, _kind, _key) \
te_errno                                                                \
_fn(tapi_data *ds, const char *column, const char *params,              \
    const tapi_data_threshold *thr, tapi_data_check *res)               \
{                                                                       \
    te_string e = TE_STRING_INIT;                                       \
    te_errno rc;                                                        \
                                                                        \
    te_string_append(&e, _key "=%s", column);                           \
    rc = check_with(ds, NULL, _kind, column, e.ptr, params, thr, res);  \
    te_string_free(&e);                                                 \
    return rc;                                                          \
}

/* See description in tapi_data.h */
ONE_COLUMN_CHECK(tapi_data_check_nulls, "nulls", "column")
/* See description in tapi_data.h */
ONE_COLUMN_CHECK(tapi_data_check_unique, "unique", "columns")
/* See description in tapi_data.h */
ONE_COLUMN_CHECK(tapi_data_check_freshness, "freshness", "event")

/* See description in tapi_data.h */
te_errno
tapi_data_check_pii(tapi_data *ds, const char *columns, const char *params,
                    const tapi_data_threshold *thr, tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&e, "columns=%s", columns == NULL ? "" : columns);
    rc = check_with(ds, NULL, "pii", columns, e.ptr, params, thr, res);
    te_string_free(&e);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_check_range(tapi_data *ds, const char *column, const char *min,
                      const char *max, const char *params,
                      const tapi_data_threshold *thr, tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&e, "column=%s\n", column);
    if (min != NULL)
        te_string_append(&e, "min=%s\n", min);
    if (max != NULL)
        te_string_append(&e, "max=%s\n", max);
    rc = check_with(ds, NULL, "range", column, e.ptr, params, thr, res);
    te_string_free(&e);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_check_pattern(tapi_data *ds, const char *column, const char *regex,
                        const char *params, const tapi_data_threshold *thr,
                        tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&e, "column=%s\n", column);
    if (regex != NULL && regex[0] == '@')
        te_string_append(&e, "pattern=%s", regex + 1);
    else
        te_string_append(&e, "regex=%s", regex == NULL ? "" : regex);
    /* A regex is passed as is, so not through the ';' shorthand. */
    te_string_append(&e, "\n");
    {
        te_string p = TE_STRING_INIT;

        te_string_append(&p, "%s", e.ptr);
        params_add(&p, params);
        rc = tapi_data_check_run(ds, NULL, "pattern", column, p.ptr, thr,
                                 res);
        te_string_free(&p);
    }
    te_string_free(&e);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_check_distribution(tapi_data *ds, const char *column,
                             tapi_data *ref, const char *params,
                             const tapi_data_threshold *thr,
                             tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&e, "column=%s", column);
    rc = check_with(ds, ref, "distribution", column, e.ptr, params, thr, res);
    te_string_free(&e);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_check_reference(tapi_data *ds, const char *column, tapi_data *ref,
                          const char *ref_column, const char *params,
                          const tapi_data_threshold *thr,
                          tapi_data_check *res)
{
    te_string e = TE_STRING_INIT;
    te_errno rc;

    te_string_append(&e, "column=%s\nref_column=%s", column,
                     ref_column != NULL ? ref_column : column);
    rc = check_with(ds, ref, "reference", column, e.ptr, params, thr, res);
    te_string_free(&e);
    return rc;
}

/* See description in tapi_data.h */
te_bool
tapi_data_check_verdict(const tapi_data_check *res)
{
    RING("%s: %s\n%s", res->passed ? "PASS" : "FAIL",
         res->summary != NULL ? res->summary : "",
         res->report.ptr != NULL ? res->report.ptr : "");
    if (!res->passed)
        ERROR_VERDICT("Data quality: %s", res->summary);
    return res->passed;
}

/* See description in tapi_data.h */
void
tapi_data_check_free(tapi_data_check *res)
{
    free(res->kind);
    free(res->target);
    free(res->summary);
    tapi_data_kv_free(&res->kv);
    te_string_free(&res->report);
    memset(res, 0, sizeof(*res));
    res->report = (te_string)TE_STRING_INIT;
}

/* ---- generation, conversion, reconciliation ----------------------- */

/* See description in tapi_data.h */
te_errno
tapi_data_generate(rcf_rpc_server *rpcs, const char *path, const char *format,
                   const char *spec, const char *params, tapi_data_gen *out)
{
    te_string p = TE_STRING_INIT;
    te_string sp = TE_STRING_INIT;
    tapi_data_rpc_args a = { 0 };
    tapi_data_rpc_res r = TAPI_DATA_RPC_RES_INIT;
    const char *c;
    te_errno rc;

    memset(out, 0, sizeof(*out));
    params_add(&p, params);
    /* ';' separates columns in the spec shorthand. */
    for (c = spec; c != NULL && *c != '\0'; c++)
        te_string_append(&sp, "%c", *c == ';' ? '\n' : *c);
    a.path = path;
    a.format = format;
    a.params = p.ptr;
    a.spec = sp.ptr;
    rc = call(rpcs, TAPI_DATA_RPC_GENERATE, &a, &r);
    te_string_free(&p);
    te_string_free(&sp);
    if (rc == 0)
    {
        kv_parse(&out->kv, r.result.ptr);
        out->rows = KVU(&out->kv, "rows");
        out->written = KVU(&out->kv, "written");
        out->duplicates = KVU(&out->kv, "duplicates");
        out->malformed = KVU(&out->kv, "malformed");
        out->bytes = KVU(&out->kv, "bytes");
        snprintf(out->digest, sizeof(out->digest), "%s",
                 tapi_data_kv_get(&out->kv, "digest", ""));
        snprintf(out->digest_unordered, sizeof(out->digest_unordered), "%s",
                 tapi_data_kv_get(&out->kv, "digest_unordered", ""));
        out->schema = xstrdup(tapi_data_kv_get(&out->kv, "schema", ""));
        RING("Generated %s: %llu rows (%llu duplicate, %llu malformed), "
             "%llu bytes, digest %s", path, (unsigned long long)out->rows,
             (unsigned long long)out->duplicates,
             (unsigned long long)out->malformed,
             (unsigned long long)out->bytes, out->digest);
    }
    tapi_data_rpc_res_free(&r);
    return rc;
}

/* See description in tapi_data.h */
void
tapi_data_gen_free(tapi_data_gen *g)
{
    free(g->schema);
    tapi_data_kv_free(&g->kv);
    memset(g, 0, sizeof(*g));
}

/* See description in tapi_data.h */
te_errno
tapi_data_digest(tapi_data *ds, const char *params, tapi_data_digest_info *out)
{
    tapi_data_kv kv = TAPI_DATA_KV_INIT;
    te_errno rc = simple_op(ds, NULL, TAPI_DATA_RPC_DIGEST, NULL, NULL, NULL,
                            params, &kv, NULL);

    memset(out, 0, sizeof(*out));
    if (rc == 0)
    {
        out->rows = KVU(&kv, "digest_rows");
        snprintf(out->digest, sizeof(out->digest), "%s",
                 tapi_data_kv_get(&kv, "digest", ""));
        snprintf(out->unordered, sizeof(out->unordered), "%s",
                 tapi_data_kv_get(&kv, "digest_unordered", ""));
    }
    tapi_data_kv_free(&kv);
    return rc;
}

/* See description in tapi_data.h */
te_errno
tapi_data_convert(tapi_data *ds, const char *path, const char *format,
                  const char *params, tapi_data_kv *out)
{
    return simple_op(ds, NULL, TAPI_DATA_RPC_CONVERT, NULL, path, format,
                     params, out, NULL);
}

/* See description in tapi_data.h */
te_errno
tapi_data_aggregate(tapi_data *ds, const char *path, const char *format,
                    const char *params, tapi_data_kv *out)
{
    return simple_op(ds, NULL, TAPI_DATA_RPC_AGGREGATE, NULL, path, format,
                     params, out, NULL);
}

/* See description in tapi_data.h */
te_errno
tapi_data_reconcile(tapi_data *src, tapi_data *dst, const char *params,
                    tapi_data_recon *out)
{
    te_errno rc;

    te_string_free(&out->report);
    tapi_data_kv_free(&out->kv);
    memset(out, 0, sizeof(*out));
    out->report = (te_string)TE_STRING_INIT;
    rc = simple_op(src, dst, TAPI_DATA_RPC_RECONCILE, NULL, NULL, NULL,
                   params, &out->kv, &out->report);
    if (rc != 0)
        return rc;
    out->rows_src = KVU(&out->kv, "rows_src");
    out->rows_dst = KVU(&out->kv, "rows_dst");
    out->keys_matched = KVU(&out->kv, "keys_matched");
    out->keys_lost = KVU(&out->kv, "keys_lost");
    out->keys_phantom = KVU(&out->kv, "keys_phantom");
    out->dup_src = KVU(&out->kv, "dup_src");
    out->dup_dst = KVU(&out->kv, "dup_dst");
    return 0;
}

/* See description in tapi_data.h */
void
tapi_data_recon_free(tapi_data_recon *r)
{
    tapi_data_kv_free(&r->kv);
    te_string_free(&r->report);
}
