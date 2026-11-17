/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: buffers, options, budget, hashing
 */

#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "ta_data_int.h"

/* ---- buffer ------------------------------------------------------- */

int
td_buf_reserve(td_buf *b, size_t extra)
{
    size_t need = b->len + extra + 1;
    size_t cap;
    char *p;

    if (need <= b->cap)
        return 0;
    cap = b->cap == 0 ? 64 : b->cap;
    while (cap < need)
        cap *= 2;
    p = realloc(b->ptr, cap);
    if (p == NULL)
        return TA_DATA_ENOMEM;
    b->ptr = p;
    b->cap = cap;
    return 0;
}

int
td_buf_add(td_buf *b, const void *data, size_t len)
{
    if (td_buf_reserve(b, len) != 0)
        return TA_DATA_ENOMEM;
    if (len > 0)
        memcpy(b->ptr + b->len, data, len);
    b->len += len;
    b->ptr[b->len] = '\0';
    return 0;
}

int
td_buf_addc(td_buf *b, char c)
{
    return td_buf_add(b, &c, 1);
}

int
td_buf_adds(td_buf *b, const char *s)
{
    return td_buf_add(b, s, strlen(s));
}

int
td_buf_vprintf(td_buf *b, const char *fmt, va_list ap)
{
    va_list ap2;
    int n;

    va_copy(ap2, ap);
    n = vsnprintf(NULL, 0, fmt, ap2);
    va_end(ap2);
    if (n < 0)
        return TA_DATA_EINVAL;
    if (td_buf_reserve(b, (size_t)n) != 0)
        return TA_DATA_ENOMEM;
    vsnprintf(b->ptr + b->len, (size_t)n + 1, fmt, ap);
    b->len += (size_t)n;
    return 0;
}

int
td_buf_printf(td_buf *b, const char *fmt, ...)
{
    va_list ap;
    int rc;

    va_start(ap, fmt);
    rc = td_buf_vprintf(b, fmt, ap);
    va_end(ap);
    return rc;
}

void
td_buf_reset(td_buf *b)
{
    b->len = 0;
    if (b->ptr != NULL)
        b->ptr[0] = '\0';
}

void
td_buf_free(td_buf *b)
{
    free(b->ptr);
    b->ptr = NULL;
    b->len = b->cap = 0;
}

char *
td_buf_take(td_buf *b)
{
    char *p = b->ptr;

    if (p == NULL)
        p = td_strdup("");
    b->ptr = NULL;
    b->len = b->cap = 0;
    return p;
}

void
td_esc(td_buf *b, const char *s, size_t len)
{
    size_t i;

    for (i = 0; i < len; i++)
    {
        unsigned char c = (unsigned char)s[i];

        if (c == '\t' || c == '\n' || c == '\r' || c == '%' || c == ',' ||
            c == ':' || c == '=' || c == '|' || c < 0x20)
            td_buf_printf(b, "%%%02X", c);
        else
            td_buf_addc(b, (char)c);
    }
}

int
td_fail(char **err, int rc, const char *fmt, ...)
{
    va_list ap;
    td_buf b = TD_BUF_INIT;

    if (err == NULL || *err != NULL)
        return rc;
    va_start(ap, fmt);
    td_buf_vprintf(&b, fmt, ap);
    va_end(ap);
    *err = td_buf_take(&b);
    return rc;
}

char *
td_strdup(const char *s)
{
    return td_strndup(s, s == NULL ? 0 : strlen(s));
}

char *
td_strndup(const char *s, size_t n)
{
    char *p = malloc(n + 1);

    if (p == NULL)
        return NULL;
    if (n > 0)
        memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

/* ---- options ------------------------------------------------------ */

static char *
trim_dup(const char *s, size_t n)
{
    while (n > 0 && isspace((unsigned char)*s))
    {
        s++;
        n--;
    }
    while (n > 0 && isspace((unsigned char)s[n - 1]))
        n--;
    return td_strndup(s, n);
}

int
td_kv_parse(td_kv *kv, const char *text)
{
    const char *p = text;

    if (text == NULL)
        return 0;
    while (*p != '\0')
    {
        const char *end = p + strcspn(p, "\n");
        const char *eq = memchr(p, '=', (size_t)(end - p));

        if (eq != NULL)
        {
            char **k = realloc(kv->key, (kv->n + 1) * sizeof(*k));
            char **v;

            if (k == NULL)
                return TA_DATA_ENOMEM;
            kv->key = k;
            v = realloc(kv->val, (kv->n + 1) * sizeof(*v));
            if (v == NULL)
                return TA_DATA_ENOMEM;
            kv->val = v;
            kv->key[kv->n] = trim_dup(p, (size_t)(eq - p));
            kv->val[kv->n] = trim_dup(eq + 1, (size_t)(end - eq - 1));
            kv->n++;
        }
        p = *end == '\0' ? end : end + 1;
    }
    return 0;
}

int
td_kv_set(td_kv *kv, const char *key, const char *val)
{
    char **k = realloc(kv->key, (kv->n + 1) * sizeof(*k));
    char **v;

    if (k == NULL)
        return TA_DATA_ENOMEM;
    kv->key = k;
    v = realloc(kv->val, (kv->n + 1) * sizeof(*v));
    if (v == NULL)
        return TA_DATA_ENOMEM;
    kv->val = v;
    kv->key[kv->n] = td_strdup(key);
    kv->val[kv->n] = td_strdup(val);
    kv->n++;
    return 0;
}

void
td_kv_free(td_kv *kv)
{
    size_t i;

    for (i = 0; i < kv->n; i++)
    {
        free(kv->key[i]);
        free(kv->val[i]);
    }
    free(kv->key);
    free(kv->val);
    kv->key = kv->val = NULL;
    kv->n = 0;
}

const char *
td_kv_get(const td_kv *kv, const char *key, const char *def)
{
    size_t i;

    for (i = kv->n; i > 0; i--)
    {
        if (strcmp(kv->key[i - 1], key) == 0)
            return kv->val[i - 1];
    }
    return def;
}

/* Integer with an optional k/m/g binary suffix (for sizes). */
int64_t
td_kv_int(const td_kv *kv, const char *key, int64_t def)
{
    const char *v = td_kv_get(kv, key, NULL);
    char *end;
    long long n;

    if (v == NULL || *v == '\0')
        return def;
    errno = 0;
    n = strtoll(v, &end, 0);
    if (errno != 0 || end == v)
        return def;
    switch (tolower((unsigned char)*end))
    {
        case 'k': n <<= 10; break;
        case 'm': n <<= 20; break;
        case 'g': n <<= 30; break;
        default: break;
    }
    return n;
}

double
td_kv_dbl(const td_kv *kv, const char *key, double def)
{
    const char *v = td_kv_get(kv, key, NULL);
    char *end;
    double d;

    if (v == NULL || *v == '\0')
        return def;
    d = strtod(v, &end);
    return end == v ? def : d;
}

bool
td_kv_bool(const td_kv *kv, const char *key, bool def)
{
    const char *v = td_kv_get(kv, key, NULL);

    if (v == NULL || *v == '\0')
        return def;
    return strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
           strcasecmp(v, "yes") == 0 || strcasecmp(v, "on") == 0;
}

size_t
td_split(const char *s, char sep, char ***out)
{
    char **v = NULL;
    size_t n = 0;
    const char *p = s;

    *out = NULL;
    if (s == NULL || *s == '\0')
        return 0;
    for (;;)
    {
        const char *e = strchr(p, sep);
        size_t len = e != NULL ? (size_t)(e - p) : strlen(p);
        char **nv = realloc(v, (n + 1) * sizeof(*v));

        if (nv == NULL)
            break;
        v = nv;
        v[n++] = trim_dup(p, len);
        if (e == NULL)
            break;
        p = e + 1;
    }
    *out = v;
    return n;
}

void
td_split_free(char **v, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++)
        free(v[i]);
    free(v);
}

/* ---- memory budget ------------------------------------------------ */

bool
td_mem_take(td_mem *m, uint64_t n)
{
    if (m == NULL)
        return true;
    if (m->limit != 0 && m->used + n > m->limit)
        return false;
    m->used += n;
    if (m->used > m->peak)
        m->peak = m->used;
    return true;
}

void
td_mem_give(td_mem *m, uint64_t n)
{
    if (m == NULL)
        return;
    m->used = n > m->used ? 0 : m->used - n;
}

/* ---- hashing ------------------------------------------------------ */

static inline uint64_t
rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

static inline uint64_t
mix64(uint64_t z)
{
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

/*
 * 8 bytes at a time multiply-rotate, finished with the splitmix64
 * avalanche. Deterministic across runs and hosts of the same byte order,
 * which is what the partition layout and the digests need.
 */
uint64_t
td_hash64(const void *data, size_t len, uint64_t seed)
{
    const unsigned char *p = data;
    uint64_t h = seed ^ (0x9e3779b97f4a7c15ULL * (len + 1));

    while (len >= 8)
    {
        uint64_t k;

        memcpy(&k, p, 8);
        h ^= mix64(k);
        h = rotl(h, 27) * 0x9e3779b97f4a7c15ULL + 0x632be59bd9b4e019ULL;
        p += 8;
        len -= 8;
    }
    if (len > 0)
    {
        uint64_t k = 0;

        memcpy(&k, p, len);
        h ^= mix64(k ^ ((uint64_t)len << 56));
        h = rotl(h, 31) * 0x9e3779b97f4a7c15ULL;
    }
    return mix64(h);
}

/* ---- PRNG: xoshiro256**, seeded by splitmix64 --------------------- */

void
td_rng_seed(td_rng *r, uint64_t seed)
{
    int i;

    for (i = 0; i < 4; i++)
    {
        seed += 0x9e3779b97f4a7c15ULL;
        r->s[i] = mix64(seed);
    }
}

uint64_t
td_rng_next(td_rng *r)
{
    uint64_t *s = r->s;
    uint64_t res = rotl(s[1] * 5, 7) * 9;
    uint64_t t = s[1] << 17;

    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl(s[3], 45);
    return res;
}

double
td_rng_double(td_rng *r)
{
    return (double)(td_rng_next(r) >> 11) * (1.0 / 9007199254740992.0);
}

int64_t
td_rng_range(td_rng *r, int64_t lo, int64_t hi)
{
    uint64_t span;

    if (hi <= lo)
        return lo;
    span = (uint64_t)hi - (uint64_t)lo + 1;
    if (span == 0)
        return (int64_t)td_rng_next(r);
    return lo + (int64_t)(td_rng_next(r) % span);
}

double
td_rng_normal(td_rng *r)
{
    double u1 = td_rng_double(r);
    double u2 = td_rng_double(r);

    if (u1 < 1e-300)
        u1 = 1e-300;
    return sqrt(-2.0 * log(u1)) * cos(6.283185307179586 * u2);
}

/* ---- HyperLogLog -------------------------------------------------- */

void
td_hll_add(td_hll *h, uint64_t hash)
{
    unsigned int idx = (unsigned int)(hash >> (64 - TD_HLL_P));
    uint64_t rest = (hash << TD_HLL_P) | (1ULL << (TD_HLL_P - 1));
    uint8_t rank = (uint8_t)(__builtin_clzll(rest) + 1);

    if (rank > h->reg[idx])
        h->reg[idx] = rank;
}

double
td_hll_count(const td_hll *h)
{
    const double m = (double)(1u << TD_HLL_P);
    double alpha = 0.7213 / (1.0 + 1.079 / m);
    double sum = 0;
    unsigned int zeros = 0;
    unsigned int i;
    double e;

    for (i = 0; i < (1u << TD_HLL_P); i++)
    {
        sum += ldexp(1.0, -(int)h->reg[i]);
        if (h->reg[i] == 0)
            zeros++;
    }
    e = alpha * m * m / sum;
    if (e <= 2.5 * m && zeros > 0)
        e = m * log(m / (double)zeros);    /* linear counting */
    return e;
}

/* ---- bounded report ----------------------------------------------- */

void
td_report_init(td_report *r, size_t max)
{
    memset(r, 0, sizeof(*r));
    r->max = max;
}

static int
ent_cmp(const void *x, const void *y)
{
    const struct td_report_ent *a = x;
    const struct td_report_ent *b = y;

    if (a->kind != b->kind)
        return a->kind < b->kind ? -1 : 1;
    if (a->ka != b->ka)
        return a->ka < b->ka ? -1 : 1;
    if (a->kb != b->kb)
        return a->kb < b->kb ? -1 : 1;
    return strcmp(a->line, b->line);
}

void
td_report_add(td_report *r, int kind, uint64_t ka, uint64_t kb,
              const char *line)
{
    struct td_report_ent e = { kind, ka, kb, (char *)line };
    size_t i;
    size_t worst = 0;

    r->total++;
    r->digest += td_hash64(line, strlen(line), 0x5eed);
    if (r->max == 0)
        return;
    if (r->ent == NULL)
    {
        r->ent = calloc(r->max, sizeof(*r->ent));
        if (r->ent == NULL)
            return;
    }
    if (r->n < r->max)
    {
        r->ent[r->n] = e;
        r->ent[r->n].line = td_strdup(line);
        r->n++;
        return;
    }
    for (i = 1; i < r->n; i++)
    {
        if (ent_cmp(&r->ent[i], &r->ent[worst]) > 0)
            worst = i;
    }
    if (ent_cmp(&e, &r->ent[worst]) < 0)
    {
        free(r->ent[worst].line);
        r->ent[worst] = e;
        r->ent[worst].line = td_strdup(line);
    }
}

char *
td_report_take(td_report *r)
{
    td_buf b = TD_BUF_INIT;
    size_t i;

    if (r->n > 0)
        qsort(r->ent, r->n, sizeof(*r->ent), ent_cmp);
    for (i = 0; i < r->n; i++)
    {
        td_buf_adds(&b, r->ent[i].line);
        td_buf_addc(&b, '\n');
    }
    return td_buf_take(&b);
}

void
td_report_free(td_report *r)
{
    size_t i;

    for (i = 0; i < r->n; i++)
        free(r->ent[i].line);
    free(r->ent);
    memset(r, 0, sizeof(*r));
}

/* ---- rows and cells ----------------------------------------------- */

void
ta_data_cell_clear(ta_data_cell *c)
{
    free(c->s);
    memset(c, 0, sizeof(*c));
    c->null = 1;
}

int
td_row_init(td_row *r, size_t n)
{
    size_t i;

    r->n = n;
    r->rowno = 0;
    r->cells = calloc(n == 0 ? 1 : n, sizeof(*r->cells));
    if (r->cells == NULL)
        return TA_DATA_ENOMEM;
    for (i = 0; i < n; i++)
        r->cells[i].null = 1;
    return 0;
}

void
td_row_clear(td_row *r)
{
    size_t i;

    for (i = 0; i < r->n; i++)
        ta_data_cell_clear(&r->cells[i]);
}

void
td_row_free(td_row *r)
{
    if (r->cells != NULL)
        td_row_clear(r);
    free(r->cells);
    r->cells = NULL;
    r->n = 0;
}

const char *
ta_data_strerror(int rc)
{
    switch (rc)
    {
        case TA_DATA_OK: return "ok";
        case TA_DATA_EINVAL: return "invalid argument";
        case TA_DATA_ENOENT: return "not found";
        case TA_DATA_EIO: return "I/O error";
        case TA_DATA_ECORRUPT: return "corrupted input";
        case TA_DATA_EILSEQ: return "malformed record";
        case TA_DATA_E2BIG: return "memory limit exceeded";
        case TA_DATA_ENOMEM: return "out of memory";
        case TA_DATA_ENOTSUP: return "not supported";
        default: return "unknown error";
    }
}
