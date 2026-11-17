/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: logical types, schemas and values
 *
 * The type grammar (bool, int, float, decimal(p,s), string, binary,
 * date, timestamp, array<T>, object{name:T[!],...}), the schema text,
 * the parsing of text into typed values, canonical formatting, the
 * conformance of a source value to its column (native, widening, text
 * interpretation or opt-in coercion), ordering, tolerant equality and
 * the decodable serialization the hash partitions key on.
 */

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <jansson.h>

#include "ta_data_int.h"

/* ---- type grammar ------------------------------------------------- */

static const struct {
    const char *name;
    int kind;
} kind_names[] = {
    { "bool", TA_DATA_T_BOOL }, { "boolean", TA_DATA_T_BOOL },
    { "int", TA_DATA_T_INT }, { "integer", TA_DATA_T_INT },
    { "int64", TA_DATA_T_INT }, { "long", TA_DATA_T_INT },
    { "bigint", TA_DATA_T_INT },
    { "float", TA_DATA_T_FLOAT }, { "double", TA_DATA_T_FLOAT },
    { "real", TA_DATA_T_FLOAT },
    { "decimal", TA_DATA_T_DECIMAL }, { "numeric", TA_DATA_T_DECIMAL },
    { "string", TA_DATA_T_STRING }, { "text", TA_DATA_T_STRING },
    { "utf8", TA_DATA_T_STRING }, { "varchar", TA_DATA_T_STRING },
    { "binary", TA_DATA_T_BINARY }, { "bytes", TA_DATA_T_BINARY },
    { "blob", TA_DATA_T_BINARY },
    { "date", TA_DATA_T_DATE },
    { "timestamp", TA_DATA_T_TIMESTAMP }, { "datetime", TA_DATA_T_TIMESTAMP },
    { "array", TA_DATA_T_ARRAY }, { "list", TA_DATA_T_ARRAY },
    { "object", TA_DATA_T_OBJECT }, { "struct", TA_DATA_T_OBJECT },
};

const char *
td_kind_name(int kind)
{
    switch (kind)
    {
        case TA_DATA_T_BOOL: return "bool";
        case TA_DATA_T_INT: return "int";
        case TA_DATA_T_FLOAT: return "float";
        case TA_DATA_T_DECIMAL: return "decimal";
        case TA_DATA_T_STRING: return "string";
        case TA_DATA_T_BINARY: return "binary";
        case TA_DATA_T_DATE: return "date";
        case TA_DATA_T_TIMESTAMP: return "timestamp";
        case TA_DATA_T_ARRAY: return "array";
        case TA_DATA_T_OBJECT: return "object";
        default: return "unknown";
    }
}

bool
td_kind_numeric(int kind)
{
    return kind == TA_DATA_T_INT || kind == TA_DATA_T_FLOAT ||
           kind == TA_DATA_T_DECIMAL;
}

td_type *
td_type_simple(int kind)
{
    td_type *t = calloc(1, sizeof(*t));

    if (t != NULL)
    {
        t->kind = kind;
        if (kind == TA_DATA_T_DECIMAL)
        {
            t->precision = 18;
            t->scale = 0;
        }
    }
    return t;
}

void
td_type_free(td_type *t)
{
    size_t i;

    if (t == NULL)
        return;
    td_type_free(t->elem);
    for (i = 0; i < t->nfields; i++)
    {
        free(t->fields[i].name);
        td_type_free(t->fields[i].type);
    }
    free(t->fields);
    free(t);
}

td_type *
td_type_dup(const td_type *t)
{
    td_type *d;
    size_t i;

    if (t == NULL)
        return NULL;
    d = calloc(1, sizeof(*d));
    if (d == NULL)
        return NULL;
    *d = *t;
    d->elem = td_type_dup(t->elem);
    d->fields = NULL;
    if (t->nfields > 0)
    {
        d->fields = calloc(t->nfields, sizeof(*d->fields));
        for (i = 0; d->fields != NULL && i < t->nfields; i++)
        {
            d->fields[i].name = td_strdup(t->fields[i].name);
            d->fields[i].type = td_type_dup(t->fields[i].type);
            d->fields[i].required = t->fields[i].required;
        }
    }
    return d;
}

void
td_type_format(td_buf *b, const td_type *t)
{
    size_t i;

    if (t == NULL)
    {
        td_buf_adds(b, "unknown");
        return;
    }
    td_buf_adds(b, td_kind_name(t->kind));
    if (t->kind == TA_DATA_T_DECIMAL)
        td_buf_printf(b, "(%d,%d)", t->precision, t->scale);
    else if (t->kind == TA_DATA_T_ARRAY && t->elem != NULL)
    {
        td_buf_addc(b, '<');
        td_type_format(b, t->elem);
        td_buf_addc(b, '>');
    }
    else if (t->kind == TA_DATA_T_OBJECT && t->nfields > 0)
    {
        td_buf_addc(b, '{');
        for (i = 0; i < t->nfields; i++)
        {
            if (i > 0)
                td_buf_addc(b, ',');
            td_buf_printf(b, "%s:", t->fields[i].name);
            td_type_format(b, t->fields[i].type);
            if (t->fields[i].required)
                td_buf_addc(b, '!');
        }
        td_buf_addc(b, '}');
    }
}

typedef struct tparser {
    const char *p;
    char **err;
} tparser;

static void
skip_ws(tparser *tp)
{
    while (isspace((unsigned char)*tp->p))
        tp->p++;
}

static int parse_type(tparser *tp, td_type **out);

static int
parse_ident(tparser *tp, char **out)
{
    const char *s;

    skip_ws(tp);
    s = tp->p;
    if (*s == '"')
    {
        const char *e = strchr(s + 1, '"');

        if (e == NULL)
            return td_fail(tp->err, TA_DATA_EINVAL, "unterminated quoted name");
        *out = td_strndup(s + 1, (size_t)(e - s - 1));
        tp->p = e + 1;
        return 0;
    }
    while (*tp->p != '\0' && *tp->p != ':' && *tp->p != ',' &&
           *tp->p != '}' && *tp->p != '<' && *tp->p != '>' &&
           *tp->p != '{' && *tp->p != '(' && *tp->p != '!' &&
           !isspace((unsigned char)*tp->p))
        tp->p++;
    if (tp->p == s)
        return td_fail(tp->err, TA_DATA_EINVAL, "expected a name at '%s'", s);
    *out = td_strndup(s, (size_t)(tp->p - s));
    return 0;
}

static int
parse_type(tparser *tp, td_type **out)
{
    char *name = NULL;
    td_type *t;
    size_t i;
    int rc;

    rc = parse_ident(tp, &name);
    if (rc != 0)
        return rc;
    for (i = 0; i < sizeof(kind_names) / sizeof(kind_names[0]); i++)
    {
        if (strcasecmp(name, kind_names[i].name) == 0)
            break;
    }
    if (i == sizeof(kind_names) / sizeof(kind_names[0]))
    {
        rc = td_fail(tp->err, TA_DATA_EINVAL, "unknown type '%s'", name);
        free(name);
        return rc;
    }
    free(name);
    t = td_type_simple(kind_names[i].kind);
    if (t == NULL)
        return TA_DATA_ENOMEM;
    skip_ws(tp);
    if (t->kind == TA_DATA_T_DECIMAL && *tp->p == '(')
    {
        int p = 0;
        int s = 0;
        int n = 0;

        if (sscanf(tp->p, "(%d,%d)%n", &p, &s, &n) < 2 || n == 0)
        {
            n = 0;
            if (sscanf(tp->p, "(%d)%n", &p, &n) < 1 || n == 0)
            {
                td_type_free(t);
                return td_fail(tp->err, TA_DATA_EINVAL,
                               "bad decimal parameters at '%s'", tp->p);
            }
            s = 0;
        }
        if (p < 1 || p > 18 || s < 0 || s > p)
        {
            td_type_free(t);
            return td_fail(tp->err, TA_DATA_EINVAL,
                           "decimal(%d,%d) is outside 1 <= scale <= "
                           "precision <= 18", p, s);
        }
        t->precision = p;
        t->scale = s;
        tp->p += n;
    }
    else if (t->kind == TA_DATA_T_ARRAY && *tp->p == '<')
    {
        tp->p++;
        rc = parse_type(tp, &t->elem);
        skip_ws(tp);
        if (rc == 0 && *tp->p != '>')
            rc = td_fail(tp->err, TA_DATA_EINVAL, "expected '>' at '%s'",
                         tp->p);
        if (rc != 0)
        {
            td_type_free(t);
            return rc;
        }
        tp->p++;
    }
    else if (t->kind == TA_DATA_T_OBJECT && *tp->p == '{')
    {
        tp->p++;
        for (;;)
        {
            td_field f = { NULL, NULL, false };
            td_field *nf;

            skip_ws(tp);
            if (*tp->p == '}')
                break;
            rc = parse_ident(tp, &f.name);
            skip_ws(tp);
            if (rc == 0 && *tp->p != ':')
                rc = td_fail(tp->err, TA_DATA_EINVAL,
                             "expected ':' after field '%s'", f.name);
            if (rc == 0)
            {
                tp->p++;
                rc = parse_type(tp, &f.type);
            }
            if (rc != 0)
            {
                free(f.name);
                td_type_free(t);
                return rc;
            }
            skip_ws(tp);
            if (*tp->p == '!')
            {
                f.required = true;
                tp->p++;
            }
            nf = realloc(t->fields, (t->nfields + 1) * sizeof(*nf));
            if (nf == NULL)
            {
                free(f.name);
                td_type_free(f.type);
                td_type_free(t);
                return TA_DATA_ENOMEM;
            }
            t->fields = nf;
            t->fields[t->nfields++] = f;
            skip_ws(tp);
            if (*tp->p == ',')
                tp->p++;
            else if (*tp->p != '}')
            {
                td_type_free(t);
                return td_fail(tp->err, TA_DATA_EINVAL,
                               "expected ',' or '}' at '%s'", tp->p);
            }
        }
        tp->p++;
    }
    *out = t;
    return 0;
}

int
td_type_parse(const char *text, td_type **out, char **err)
{
    tparser tp = { text, err };
    int rc = parse_type(&tp, out);

    if (rc != 0)
        return rc;
    skip_ws(&tp);
    if (*tp.p != '\0' && *tp.p != '!')
    {
        td_type_free(*out);
        *out = NULL;
        return td_fail(err, TA_DATA_EINVAL, "trailing text '%s' in type '%s'",
                       tp.p, text);
    }
    return 0;
}

/* ---- schemas ------------------------------------------------------ */

int
td_schema_add(td_schema *s, const char *name, td_type *type, bool nullable)
{
    td_col *c = realloc(s->cols, (s->n + 1) * sizeof(*c));

    if (c == NULL)
        return TA_DATA_ENOMEM;
    s->cols = c;
    s->cols[s->n].name = td_strdup(name);
    s->cols[s->n].type = type;
    s->cols[s->n].nullable = nullable;
    s->n++;
    return 0;
}

void
td_schema_free(td_schema *s)
{
    size_t i;

    for (i = 0; i < s->n; i++)
    {
        free(s->cols[i].name);
        td_type_free(s->cols[i].type);
    }
    free(s->cols);
    memset(s, 0, sizeof(*s));
}

int
td_schema_find(const td_schema *s, const char *name)
{
    size_t i;

    for (i = 0; i < s->n; i++)
    {
        if (strcmp(s->cols[i].name, name) == 0)
            return (int)i;
    }
    return -1;
}

/* Split schema text into entries on newlines and depth-0 commas. */
static size_t
schema_entries(const char *text, char ***out)
{
    char **v = NULL;
    size_t n = 0;
    int depth = 0;
    const char *start = text;
    const char *p;

    for (p = text; ; p++)
    {
        if (*p == '<' || *p == '{' || *p == '(')
            depth++;
        else if ((*p == '>' || *p == '}' || *p == ')') && depth > 0)
            depth--;
        if (*p == '\0' || *p == '\n' || (*p == ',' && depth == 0))
        {
            const char *s = start;
            const char *e = p;

            while (s < e && isspace((unsigned char)*s))
                s++;
            while (e > s && isspace((unsigned char)e[-1]))
                e--;
            if (e > s && *s != '#')
            {
                char **nv = realloc(v, (n + 1) * sizeof(*v));

                if (nv == NULL)
                    break;
                v = nv;
                v[n++] = td_strndup(s, (size_t)(e - s));
            }
            if (*p == '\0')
                break;
            start = p + 1;
        }
    }
    *out = v;
    return n;
}

int
td_schema_parse(const char *text, td_schema *s, char **err)
{
    char **ent = NULL;
    size_t n;
    size_t i;
    int rc = 0;

    memset(s, 0, sizeof(*s));
    if (text == NULL)
        return 0;
    n = schema_entries(text, &ent);
    for (i = 0; i < n && rc == 0; i++)
    {
        char *e = ent[i];
        char *tab = strchr(e, '\t');
        char *name;
        const char *ttext;
        bool nullable = true;
        td_type *t = NULL;
        char *bang;

        if (tab != NULL)
        {
            /* name \t type \t nullable [\t source] */
            char *tab2;

            *tab = '\0';
            name = e;
            ttext = tab + 1;
            tab2 = strchr(tab + 1, '\t');
            if (tab2 != NULL)
            {
                *tab2 = '\0';
                nullable = tab2[1] != '0';
                {
                    char *tab3 = strchr(tab2 + 1, '\t');

                    if (tab3 != NULL)
                        *tab3 = '\0';
                }
            }
        }
        else
        {
            char *colon = strchr(e, ':');

            if (colon == NULL)
            {
                rc = td_fail(err, TA_DATA_EINVAL,
                             "schema entry '%s' has no ':type'", e);
                break;
            }
            *colon = '\0';
            name = e;
            ttext = colon + 1;
            /* Required marker: a trailing '!' after the type. */
            bang = strrchr(colon + 1, '!');
            if (bang != NULL && bang[1] == '\0')
            {
                nullable = false;
                *bang = '\0';
            }
        }
        while (*name == ' ')
            name++;
        {
            size_t l = strlen(name);

            while (l > 0 && name[l - 1] == ' ')
                name[--l] = '\0';
        }
        rc = td_type_parse(ttext, &t, err);
        if (rc == 0)
        {
            if (td_schema_find(s, name) >= 0)
            {
                td_type_free(t);
                rc = td_fail(err, TA_DATA_EINVAL,
                             "column '%s' is declared twice", name);
            }
            else
                rc = td_schema_add(s, name, t, nullable);
        }
    }
    td_split_free(ent, n);
    if (rc != 0)
        td_schema_free(s);
    else
        s->explicit_ = true;
    return rc;
}

/* ---- dates and times ---------------------------------------------- */

/* Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant). */
static int64_t
days_from_civil(int64_t y, unsigned m, unsigned d)
{
    int64_t era;
    unsigned yoe;
    unsigned doy;
    unsigned doe;

    y -= m <= 2;
    era = (y >= 0 ? y : y - 399) / 400;
    yoe = (unsigned)(y - era * 400);
    doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static void
civil_from_days(int64_t z, int64_t *y, unsigned *m, unsigned *d)
{
    int64_t era;
    unsigned doe;
    unsigned yoe;
    unsigned doy;
    unsigned mp;

    z += 719468;
    era = (z >= 0 ? z : z - 146096) / 146097;
    doe = (unsigned)(z - era * 146097);
    yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    *y = (int64_t)yoe + era * 400;
    doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    mp = (5 * doy + 2) / 153;
    *d = doy - (153 * mp + 2) / 5 + 1;
    *m = mp < 10 ? mp + 3 : mp - 9;
    *y += *m <= 2;
}

static bool
digits(const char *s, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++)
    {
        if (!isdigit((unsigned char)s[i]))
            return false;
    }
    return true;
}

static int
num(const char *s, size_t n)
{
    int v = 0;
    size_t i;

    for (i = 0; i < n; i++)
        v = v * 10 + (s[i] - '0');
    return v;
}

static bool
valid_ymd(int y, int m, int d)
{
    static const int mdays[] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31,
                                 30, 31 };
    int dim;

    (void)y;
    if (m < 1 || m > 12 || d < 1)
        return false;
    dim = mdays[m - 1];
    if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0))
        dim = 29;
    return d <= dim;
}

int
td_parse_date(const char *s, size_t len, int64_t *days)
{
    int y;
    int m;
    int d;

    if (len != 10 || s[4] != '-' || s[7] != '-' || !digits(s, 4) ||
        !digits(s + 5, 2) || !digits(s + 8, 2))
        return -1;
    y = num(s, 4);
    m = num(s + 5, 2);
    d = num(s + 8, 2);
    if (!valid_ymd(y, m, d))
        return -1;
    *days = days_from_civil(y, (unsigned)m, (unsigned)d);
    return 0;
}

int
td_parse_timestamp(const char *s, size_t len, int64_t *us)
{
    int64_t days;
    int hh = 0;
    int mi = 0;
    int ss = 0;
    int64_t frac = 0;
    int64_t off = 0;
    size_t p;

    if (len < 10 || td_parse_date(s, 10, &days) != 0)
        return -1;
    p = 10;
    if (p < len)
    {
        if ((s[p] != 'T' && s[p] != ' ' && s[p] != 't') || len < p + 6 ||
            !digits(s + p + 1, 2) || s[p + 3] != ':' || !digits(s + p + 4, 2))
            return -1;
        hh = num(s + p + 1, 2);
        mi = num(s + p + 4, 2);
        p += 6;
        if (p < len && s[p] == ':')
        {
            if (len < p + 3 || !digits(s + p + 1, 2))
                return -1;
            ss = num(s + p + 1, 2);
            p += 3;
            if (p < len && (s[p] == '.' || s[p] == ','))
            {
                int nd = 0;

                p++;
                while (p < len && isdigit((unsigned char)s[p]))
                {
                    if (nd < 6)
                    {
                        frac = frac * 10 + (s[p] - '0');
                        nd++;
                    }
                    p++;
                }
                if (nd == 0)
                    return -1;
                while (nd++ < 6)
                    frac *= 10;
            }
        }
        if (hh > 23 || mi > 59 || ss > 60)
            return -1;
        if (p < len)
        {
            if ((s[p] == 'Z' || s[p] == 'z') && p + 1 == len)
                p++;
            else if (s[p] == '+' || s[p] == '-')
            {
                int sign = s[p] == '-' ? -1 : 1;
                int oh;
                int om = 0;

                if (len < p + 3 || !digits(s + p + 1, 2))
                    return -1;
                oh = num(s + p + 1, 2);
                p += 3;
                if (p < len && s[p] == ':')
                    p++;
                if (p + 2 <= len && digits(s + p, 2))
                {
                    om = num(s + p, 2);
                    p += 2;
                }
                off = sign * ((int64_t)oh * 3600 + om * 60);
            }
            if (p != len)
                return -1;
        }
    }
    *us = ((days * 86400 + hh * 3600 + mi * 60 + ss) - off) * 1000000 + frac;
    return 0;
}

void
td_fmt_date(td_buf *b, int64_t days)
{
    int64_t y;
    unsigned m;
    unsigned d;

    civil_from_days(days, &y, &m, &d);
    td_buf_printf(b, "%04lld-%02u-%02u", (long long)y, m, d);
}

void
td_fmt_timestamp(td_buf *b, int64_t us)
{
    int64_t secs = us >= 0 ? us / 1000000 : -((-us + 999999) / 1000000);
    int64_t frac = us - secs * 1000000;
    int64_t days = secs >= 0 ? secs / 86400 : -((-secs + 86399) / 86400);
    int64_t rem = secs - days * 86400;

    td_fmt_date(b, days);
    td_buf_printf(b, "T%02d:%02d:%02d", (int)(rem / 3600),
                  (int)(rem / 60 % 60), (int)(rem % 60));
    if (frac != 0)
        td_buf_printf(b, ".%06lld", (long long)frac);
    td_buf_addc(b, 'Z');
}

/* Shortest representation that reads back to the same double. */
void
td_fmt_double(td_buf *b, double d)
{
    char tmp[40];
    int prec;

    if (isnan(d))
    {
        td_buf_adds(b, "NaN");
        return;
    }
    if (isinf(d))
    {
        td_buf_adds(b, d > 0 ? "Infinity" : "-Infinity");
        return;
    }
    if (d == 0)
    {
        td_buf_adds(b, "0");
        return;
    }
    for (prec = 15; prec <= 17; prec++)
    {
        snprintf(tmp, sizeof(tmp), "%.*g", prec, d);
        if (strtod(tmp, NULL) == d)
            break;
    }
    td_buf_adds(b, tmp);
}

static const int64_t pow10s[] = {
    1LL, 10LL, 100LL, 1000LL, 10000LL, 100000LL, 1000000LL, 10000000LL,
    100000000LL, 1000000000LL, 10000000000LL, 100000000000LL,
    1000000000000LL, 10000000000000LL, 100000000000000LL,
    1000000000000000LL, 10000000000000000LL, 100000000000000000LL,
    1000000000000000000LL,
};

void
td_fmt_decimal(td_buf *b, int64_t v, int scale)
{
    uint64_t a = v < 0 ? (uint64_t)0 - (uint64_t)v : (uint64_t)v;
    uint64_t p = (uint64_t)pow10s[scale];

    if (v < 0)
        td_buf_addc(b, '-');
    if (scale == 0)
        td_buf_printf(b, "%llu", (unsigned long long)a);
    else
        td_buf_printf(b, "%llu.%0*llu", (unsigned long long)(a / p), scale,
                      (unsigned long long)(a % p));
}

int
td_parse_decimal(const char *s, size_t len, int scale, int64_t *out)
{
    size_t i = 0;
    bool neg = false;
    uint64_t v = 0;
    int frac = -1;
    int nd = 0;

    if (len == 0)
        return -1;
    if (s[0] == '-' || s[0] == '+')
    {
        neg = s[0] == '-';
        i++;
    }
    if (i == len)
        return -1;
    for (; i < len; i++)
    {
        char c = s[i];

        if (c == '.')
        {
            if (frac >= 0)
                return -1;
            frac = 0;
            continue;
        }
        if (!isdigit((unsigned char)c))
            return -1;
        if (frac >= 0)
        {
            if (frac >= scale)
            {
                if (c != '0')
                    return -1;      /* more fractional digits than scale */
                continue;
            }
            frac++;
        }
        if (v > (UINT64_MAX - 9) / 10)
            return -1;
        v = v * 10 + (uint64_t)(c - '0');
        nd++;
    }
    if (nd == 0)
        return -1;
    if (frac < 0)
        frac = 0;
    for (; frac < scale; frac++)
    {
        if (v > UINT64_MAX / 10)
            return -1;
        v *= 10;
    }
    if (v > (uint64_t)INT64_MAX)
        return -1;
    *out = neg ? -(int64_t)v : (int64_t)v;
    return 0;
}

static int
decimal_fits(int64_t v, int precision)
{
    uint64_t a = v < 0 ? (uint64_t)0 - (uint64_t)v : (uint64_t)v;

    return precision >= 18 || a < (uint64_t)pow10s[precision];
}

/* ---- values ------------------------------------------------------- */

int
td_cell_set_str(ta_data_cell *c, int type, const char *s, size_t len)
{
    char *p = td_strndup(s, len);

    if (p == NULL)
        return TA_DATA_ENOMEM;
    free(c->s);
    c->s = p;
    c->len = len;
    c->type = type;
    c->null = 0;
    return 0;
}

int
td_cell_copy(ta_data_cell *dst, const ta_data_cell *src)
{
    ta_data_cell_clear(dst);
    *dst = *src;
    dst->s = NULL;
    if (src->s != NULL)
    {
        dst->s = td_strndup(src->s, src->len);
        if (dst->s == NULL)
            return TA_DATA_ENOMEM;
    }
    return 0;
}

static int
parse_bool(const char *s, size_t len, int64_t *v)
{
    static const char *const t[] = { "true", "t", "yes", "y", "1" };
    static const char *const f[] = { "false", "f", "no", "n", "0" };
    size_t i;

    for (i = 0; i < 5; i++)
    {
        if (strlen(t[i]) == len && strncasecmp(s, t[i], len) == 0)
        {
            *v = 1;
            return 0;
        }
        if (strlen(f[i]) == len && strncasecmp(s, f[i], len) == 0)
        {
            *v = 0;
            return 0;
        }
    }
    return -1;
}

static int
parse_int(const char *s, size_t len, int64_t *v)
{
    char tmp[32];
    char *end;
    long long n;

    if (len == 0 || len >= sizeof(tmp))
        return -1;
    memcpy(tmp, s, len);
    tmp[len] = '\0';
    if (isspace((unsigned char)tmp[0]))
        return -1;
    errno = 0;
    n = strtoll(tmp, &end, 10);
    if (errno != 0 || *end != '\0')
        return -1;
    *v = n;
    return 0;
}

static int
parse_float(const char *s, size_t len, double *v)
{
    char tmp[64];
    char *end;

    if (len == 0 || len >= sizeof(tmp))
        return -1;
    memcpy(tmp, s, len);
    tmp[len] = '\0';
    if (isspace((unsigned char)tmp[0]))
        return -1;
    *v = strtod(tmp, &end);
    if (*end != '\0')
        return -1;
    /* Reject hex floats and other strtod extensions as data. */
    if (strpbrk(tmp, "xXpP") != NULL)
        return -1;
    return 0;
}

static int hexval(int c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    c = tolower(c);
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    return -1;
}

/* Canonical compact JSON with sorted keys, or NULL. */
static char *
json_canon(json_t *j)
{
    return json_dumps(j, JSON_COMPACT | JSON_SORT_KEYS | JSON_ENCODE_ANY |
                      JSON_REAL_PRECISION(17));
}

int
td_json_canon_cell(ta_data_cell *c)
{
    json_error_t je;
    json_t *j;
    char *canon;

    if (c->null || c->s == NULL ||
        (c->type != TA_DATA_T_ARRAY && c->type != TA_DATA_T_OBJECT))
        return 0;
    j = json_loadb(c->s, c->len, JSON_DECODE_ANY, &je);
    if (j == NULL)
        return -1;
    canon = json_canon(j);
    json_decref(j);
    if (canon == NULL)
        return -1;
    free(c->s);
    c->s = canon;
    c->len = strlen(canon);
    return 0;
}

int
td_parse_text(const char *s, size_t len, const td_type *t, ta_data_cell *out)
{
    ta_data_cell c;

    memset(&c, 0, sizeof(c));
    c.type = t->kind;
    switch (t->kind)
    {
        case TA_DATA_T_BOOL:
            if (parse_bool(s, len, &c.i) != 0)
                return -1;
            break;

        case TA_DATA_T_INT:
            if (parse_int(s, len, &c.i) != 0)
                return -1;
            break;

        case TA_DATA_T_FLOAT:
            if (parse_float(s, len, &c.d) != 0)
                return -1;
            break;

        case TA_DATA_T_DECIMAL:
            if (td_parse_decimal(s, len, t->scale, &c.i) != 0 ||
                !decimal_fits(c.i, t->precision))
                return -1;
            c.scale = t->scale;
            break;

        case TA_DATA_T_DATE:
            if (td_parse_date(s, len, &c.i) != 0)
                return -1;
            break;

        case TA_DATA_T_TIMESTAMP:
            if (td_parse_timestamp(s, len, &c.i) != 0)
                return -1;
            break;

        case TA_DATA_T_STRING:
            if (td_cell_set_str(&c, TA_DATA_T_STRING, s, len) != 0)
                return -1;
            break;

        case TA_DATA_T_BINARY:
            if (len >= 2 && s[0] == '\\' && s[1] == 'x' && len % 2 == 0)
            {
                size_t n = (len - 2) / 2;
                char *p = malloc(n + 1);
                size_t i;

                if (p == NULL)
                    return -1;
                for (i = 0; i < n; i++)
                {
                    int hi = hexval((unsigned char)s[2 + 2 * i]);
                    int lo = hexval((unsigned char)s[3 + 2 * i]);

                    if (hi < 0 || lo < 0)
                    {
                        free(p);
                        return -1;
                    }
                    p[i] = (char)(hi * 16 + lo);
                }
                p[n] = '\0';
                c.s = p;
                c.len = n;
            }
            else if (td_cell_set_str(&c, TA_DATA_T_BINARY, s, len) != 0)
                return -1;
            c.type = TA_DATA_T_BINARY;
            break;

        case TA_DATA_T_ARRAY:
        case TA_DATA_T_OBJECT:
        {
            json_error_t je;
            json_t *j = json_loadb(s, len, JSON_DECODE_ANY, &je);
            char *canon;
            td_buf path = TD_BUF_INIT;
            bool ok;

            if (j == NULL ||
                (t->kind == TA_DATA_T_ARRAY && !json_is_array(j)) ||
                (t->kind == TA_DATA_T_OBJECT && !json_is_object(j)))
            {
                json_decref(j);
                return -1;
            }
            canon = json_canon(j);
            json_decref(j);
            if (canon == NULL)
                return -1;
            ok = td_json_conforms(canon, strlen(canon), t, &path);
            td_buf_free(&path);
            if (!ok)
            {
                free(canon);
                return -1;
            }
            c.s = canon;
            c.len = strlen(canon);
            break;
        }

        default:
            return -1;
    }
    c.null = 0;
    ta_data_cell_clear(out);
    *out = c;
    return 0;
}

void
td_cell_text(td_buf *b, const ta_data_cell *c)
{
    size_t i;

    if (c->null)
        return;
    switch (c->type)
    {
        case TA_DATA_T_BOOL:
            td_buf_adds(b, c->i ? "true" : "false");
            break;
        case TA_DATA_T_INT:
            td_buf_printf(b, "%lld", (long long)c->i);
            break;
        case TA_DATA_T_FLOAT:
            td_fmt_double(b, c->d);
            break;
        case TA_DATA_T_DECIMAL:
            td_fmt_decimal(b, c->i, c->scale);
            break;
        case TA_DATA_T_DATE:
            td_fmt_date(b, c->i);
            break;
        case TA_DATA_T_TIMESTAMP:
            td_fmt_timestamp(b, c->i);
            break;
        case TA_DATA_T_BINARY:
            td_buf_adds(b, "\\x");
            for (i = 0; i < c->len; i++)
                td_buf_printf(b, "%02x", (unsigned char)c->s[i]);
            break;
        default:
            if (c->s != NULL)
                td_buf_add(b, c->s, c->len);
            break;
    }
}

static void
json_str(td_buf *b, const char *s, size_t len)
{
    json_t *j = json_stringn(s, len);
    char *t = NULL;

    if (j != NULL)
        t = json_dumps(j, JSON_ENCODE_ANY);
    if (t != NULL)
        td_buf_adds(b, t);
    else
    {
        /* Not valid UTF-8: emit hex so the output stays valid JSON. */
        size_t i;

        td_buf_adds(b, "\"\\\\x");
        for (i = 0; i < len; i++)
            td_buf_printf(b, "%02x", (unsigned char)s[i]);
        td_buf_addc(b, '"');
    }
    free(t);
    json_decref(j);
}

void
td_cell_json(td_buf *b, const ta_data_cell *c)
{
    td_buf t = TD_BUF_INIT;

    if (c->null)
    {
        td_buf_adds(b, "null");
        return;
    }
    switch (c->type)
    {
        case TA_DATA_T_BOOL:
        case TA_DATA_T_INT:
        case TA_DATA_T_DECIMAL:
            td_cell_text(b, c);
            break;
        case TA_DATA_T_FLOAT:
            if (isnan(c->d) || isinf(c->d))
            {
                td_cell_text(&t, c);
                json_str(b, t.ptr, t.len);
            }
            else
                td_cell_text(b, c);
            break;
        case TA_DATA_T_ARRAY:
        case TA_DATA_T_OBJECT:
            td_buf_add(b, c->s, c->len);
            break;
        default:
            td_cell_text(&t, c);
            json_str(b, t.ptr == NULL ? "" : t.ptr, t.len);
            break;
    }
    td_buf_free(&t);
}

bool
td_cell_num(const ta_data_cell *c, double *d)
{
    if (c->null)
        return false;
    switch (c->type)
    {
        case TA_DATA_T_BOOL:
        case TA_DATA_T_INT:
        case TA_DATA_T_DATE:
        case TA_DATA_T_TIMESTAMP:
            *d = (double)c->i;
            return true;
        case TA_DATA_T_FLOAT:
            *d = c->d;
            return true;
        case TA_DATA_T_DECIMAL:
            *d = (double)c->i / (double)pow10s[c->scale];
            return true;
        default:
            return false;
    }
}

/* ---- conformance -------------------------------------------------- */

static bool
json_value_conforms(json_t *j, const td_type *t, td_buf *path)
{
    size_t i;
    int64_t tmp;

    if (t == NULL || json_is_null(j))
        return true;
    switch (t->kind)
    {
        case TA_DATA_T_BOOL:
            return json_is_boolean(j);
        case TA_DATA_T_INT:
            return json_is_integer(j);
        case TA_DATA_T_FLOAT:
            return json_is_number(j);
        case TA_DATA_T_DECIMAL:
            if (json_is_integer(j))
                return true;
            if (json_is_real(j))
                return true;
            return json_is_string(j) &&
                   td_parse_decimal(json_string_value(j),
                                    json_string_length(j), t->scale,
                                    &tmp) == 0;
        case TA_DATA_T_STRING:
        case TA_DATA_T_BINARY:
            return json_is_string(j);
        case TA_DATA_T_DATE:
            return json_is_string(j) &&
                   td_parse_date(json_string_value(j), json_string_length(j),
                                 &tmp) == 0;
        case TA_DATA_T_TIMESTAMP:
            return json_is_string(j) &&
                   td_parse_timestamp(json_string_value(j),
                                      json_string_length(j), &tmp) == 0;
        case TA_DATA_T_ARRAY:
            if (!json_is_array(j))
                return false;
            for (i = 0; i < json_array_size(j); i++)
            {
                size_t mark = path->len;

                td_buf_printf(path, "[%zu]", i);
                if (!json_value_conforms(json_array_get(j, i), t->elem, path))
                    return false;
                path->len = mark;
                path->ptr[mark] = '\0';
            }
            return true;
        case TA_DATA_T_OBJECT:
        {
            const char *key;
            json_t *v;

            if (!json_is_object(j))
                return false;
            if (t->nfields == 0)
                return true;
            for (i = 0; i < t->nfields; i++)
            {
                size_t mark = path->len;
                json_t *f = json_object_get(j, t->fields[i].name);

                td_buf_printf(path, ".%s", t->fields[i].name);
                if (f == NULL || json_is_null(f))
                {
                    if (t->fields[i].required)
                        return false;
                }
                else if (!json_value_conforms(f, t->fields[i].type, path))
                    return false;
                path->len = mark;
                path->ptr[mark] = '\0';
            }
            json_object_foreach(j, key, v)
            {
                bool known = false;

                (void)v;
                for (i = 0; i < t->nfields && !known; i++)
                    known = strcmp(t->fields[i].name, key) == 0;
                if (!known)
                {
                    td_buf_printf(path, ".%s(unexpected)", key);
                    return false;
                }
            }
            return true;
        }
        default:
            return false;
    }
}

bool
td_json_conforms(const char *s, size_t len, const td_type *t, td_buf *path)
{
    json_error_t je;
    json_t *j = json_loadb(s, len, JSON_DECODE_ANY, &je);
    bool ok;

    if (j == NULL)
        return false;
    if (path->ptr == NULL)
        td_buf_adds(path, "");
    ok = json_value_conforms(j, t, path);
    json_decref(j);
    return ok;
}

/* Interpret a string as @p t (text interpretation, not coercion). */
static int
from_text(ta_data_cell *c, const td_type *t)
{
    ta_data_cell n;
    int rc;

    memset(&n, 0, sizeof(n));
    rc = td_parse_text(c->s == NULL ? "" : c->s, c->len, t, &n);
    if (rc != 0)
        return -1;
    ta_data_cell_clear(c);
    *c = n;
    return 0;
}

int
td_cell_conform(ta_data_cell *c, const td_type *t, bool coerce, bool *coerced)
{
    int from;
    td_buf tmp = TD_BUF_INIT;
    int rc = -1;

    if (c->null)
        return 0;
    if (c->raw)
    {
        c->raw = 0;
        return from_text(c, t);
    }
    from = c->type;
    if (from == t->kind)
    {
        if (from == TA_DATA_T_DECIMAL && c->scale != t->scale)
        {
            int64_t v = c->i;
            int s = c->scale;

            while (s < t->scale)
            {
                if (v > INT64_MAX / 10 || v < INT64_MIN / 10)
                    return -1;
                v *= 10;
                s++;
            }
            while (s > t->scale)
            {
                if (v % 10 != 0)
                    return -1;
                v /= 10;
                s--;
            }
            c->i = v;
            c->scale = t->scale;
        }
        if (from == TA_DATA_T_DECIMAL && !decimal_fits(c->i, t->precision))
            return -1;
        if (from == TA_DATA_T_ARRAY || from == TA_DATA_T_OBJECT)
        {
            bool ok = td_json_conforms(c->s, c->len, t, &tmp);

            td_buf_free(&tmp);
            return ok ? 0 : -1;
        }
        return 0;
    }

    /* Lossless widenings and natural interpretations: not coercion. */
    switch (t->kind)
    {
        case TA_DATA_T_FLOAT:
            if (from == TA_DATA_T_INT)
            {
                c->d = (double)c->i;
                c->type = TA_DATA_T_FLOAT;
                return 0;
            }
            if (from == TA_DATA_T_DECIMAL)
            {
                c->d = (double)c->i / (double)pow10s[c->scale];
                c->type = TA_DATA_T_FLOAT;
                return 0;
            }
            break;

        case TA_DATA_T_DECIMAL:
            if (from == TA_DATA_T_INT)
            {
                int64_t p = pow10s[t->scale];

                if (c->i > INT64_MAX / p || c->i < INT64_MIN / p ||
                    !decimal_fits(c->i * p, t->precision))
                    return -1;
                c->i *= p;
                c->scale = t->scale;
                c->type = TA_DATA_T_DECIMAL;
                return 0;
            }
            if (from == TA_DATA_T_FLOAT)
            {
                /* JSON has no decimal: accept a double that is exact at
                 * the column scale once printed. */
                td_fmt_double(&tmp, c->d);
                rc = td_parse_decimal(tmp.ptr, tmp.len, t->scale, &c->i);
                td_buf_free(&tmp);
                if (rc != 0 || !decimal_fits(c->i, t->precision))
                    return -1;
                c->scale = t->scale;
                c->type = TA_DATA_T_DECIMAL;
                return 0;
            }
            if (from == TA_DATA_T_STRING)
                return from_text(c, t);
            break;

        case TA_DATA_T_TIMESTAMP:
            if (from == TA_DATA_T_DATE)
            {
                c->i *= 86400LL * 1000000LL;
                c->type = TA_DATA_T_TIMESTAMP;
                return 0;
            }
            if (from == TA_DATA_T_STRING)
                return from_text(c, t);
            break;

        case TA_DATA_T_DATE:
        case TA_DATA_T_BINARY:
        case TA_DATA_T_ARRAY:
        case TA_DATA_T_OBJECT:
            if (from == TA_DATA_T_STRING)
                return from_text(c, t);
            break;

        default:
            break;
    }

    if (!coerce)
        return -1;

    /* Opt-in coercion: through text, or numeric truncation when exact. */
    if (t->kind == TA_DATA_T_STRING)
    {
        td_cell_text(&tmp, c);
        rc = td_cell_set_str(c, TA_DATA_T_STRING, tmp.ptr == NULL ? "" :
                             tmp.ptr, tmp.len);
        td_buf_free(&tmp);
        c->raw = 0;
        if (rc != 0)
            return -1;
        *coerced = true;
        return 1;
    }
    if (from == TA_DATA_T_STRING)
    {
        if (from_text(c, t) != 0)
            return -1;
        *coerced = true;
        return 1;
    }
    if (t->kind == TA_DATA_T_INT && from == TA_DATA_T_FLOAT &&
        c->d == floor(c->d) && fabs(c->d) < 9.2e18)
    {
        c->i = (int64_t)c->d;
        c->type = TA_DATA_T_INT;
        *coerced = true;
        return 1;
    }
    if (t->kind == TA_DATA_T_INT && from == TA_DATA_T_BOOL)
    {
        c->type = TA_DATA_T_INT;
        *coerced = true;
        return 1;
    }
    if (t->kind == TA_DATA_T_BOOL && from == TA_DATA_T_INT &&
        (c->i == 0 || c->i == 1))
    {
        c->type = TA_DATA_T_BOOL;
        *coerced = true;
        return 1;
    }
    if (t->kind == TA_DATA_T_DATE && from == TA_DATA_T_TIMESTAMP &&
        c->i % (86400LL * 1000000LL) == 0)
    {
        c->i /= 86400LL * 1000000LL;
        c->type = TA_DATA_T_DATE;
        *coerced = true;
        return 1;
    }
    return -1;
}

/* ---- comparison --------------------------------------------------- */

static int
cmp_bytes(const ta_data_cell *a, const ta_data_cell *b)
{
    size_t n = a->len < b->len ? a->len : b->len;
    int r = n == 0 ? 0 : memcmp(a->s, b->s, n);

    if (r != 0)
        return r < 0 ? -1 : 1;
    return a->len == b->len ? 0 : (a->len < b->len ? -1 : 1);
}

static int64_t
time_us(const ta_data_cell *c)
{
    return c->type == TA_DATA_T_DATE ? c->i * 86400LL * 1000000LL : c->i;
}

int
td_cell_order(const ta_data_cell *a, const ta_data_cell *b)
{
    double x;
    double y;

    if ((a->type == TA_DATA_T_DATE || a->type == TA_DATA_T_TIMESTAMP) &&
        (b->type == TA_DATA_T_DATE || b->type == TA_DATA_T_TIMESTAMP))
    {
        int64_t p = time_us(a);
        int64_t q = time_us(b);

        return p < q ? -1 : (p > q ? 1 : 0);
    }
    if (a->type == b->type &&
        (a->type == TA_DATA_T_INT || a->type == TA_DATA_T_BOOL ||
         (a->type == TA_DATA_T_DECIMAL && a->scale == b->scale)))
        return a->i < b->i ? -1 : (a->i > b->i ? 1 : 0);
    if (td_cell_num(a, &x) && td_cell_num(b, &y))
        return x < y ? -1 : (x > y ? 1 : 0);
    if (a->s != NULL || b->s != NULL)
        return cmp_bytes(a, b);
    return 0;
}

bool
td_cell_equal(const ta_data_cell *a, const ta_data_cell *b, const td_tol *tol)
{
    double x;
    double y;

    if (a->null || b->null)
        return a->null && b->null && (tol == NULL || tol->null_eq);
    if ((a->type == TA_DATA_T_DATE || a->type == TA_DATA_T_TIMESTAMP) &&
        (b->type == TA_DATA_T_DATE || b->type == TA_DATA_T_TIMESTAMP))
    {
        int64_t d = time_us(a) - time_us(b);

        if (d < 0)
            d = -d;
        return d <= (tol == NULL ? 0 : tol->ts_tol_us);
    }
    if (td_kind_numeric(a->type) && td_kind_numeric(b->type))
    {
        double diff;
        double lim;

        if (a->type == b->type && a->type != TA_DATA_T_FLOAT &&
            a->scale == b->scale &&
            (tol == NULL || (tol->abs_tol == 0 && tol->rel_tol == 0)))
            return a->i == b->i;
        td_cell_num(a, &x);
        td_cell_num(b, &y);
        if (isnan(x) || isnan(y))
            return isnan(x) && isnan(y);
        if (x == y)
            return true;
        diff = fabs(x - y);
        lim = tol == NULL ? 0 : tol->abs_tol;
        if (tol != NULL && tol->rel_tol * fmax(fabs(x), fabs(y)) > lim)
            lim = tol->rel_tol * fmax(fabs(x), fabs(y));
        return diff <= lim;
    }
    if (a->type == TA_DATA_T_BOOL && b->type == TA_DATA_T_BOOL)
        return a->i == b->i;
    if (a->type == TA_DATA_T_STRING && b->type == TA_DATA_T_STRING &&
        tol != NULL && (tol->trim || tol->nocase))
    {
        const char *p = a->s;
        const char *q = b->s;
        size_t lp = a->len;
        size_t lq = b->len;

        if (tol->trim)
        {
            while (lp > 0 && isspace((unsigned char)*p)) { p++; lp--; }
            while (lp > 0 && isspace((unsigned char)p[lp - 1])) lp--;
            while (lq > 0 && isspace((unsigned char)*q)) { q++; lq--; }
            while (lq > 0 && isspace((unsigned char)q[lq - 1])) lq--;
        }
        if (lp != lq)
            return false;
        return tol->nocase ? strncasecmp(p, q, lp) == 0 :
                             memcmp(p, q, lp) == 0;
    }
    if (a->type != b->type)
        return false;
    return cmp_bytes(a, b) == 0;
}

/* ---- serialization ------------------------------------------------ */

static void
put64(td_buf *b, uint64_t v)
{
    unsigned char t[8];
    int i;

    for (i = 0; i < 8; i++)
        t[i] = (unsigned char)(v >> (8 * i));
    td_buf_add(b, t, 8);
}

static uint64_t
get64(const char *p)
{
    uint64_t v = 0;
    int i;

    for (i = 0; i < 8; i++)
        v |= (uint64_t)(unsigned char)p[i] << (8 * i);
    return v;
}

void
td_cell_ser(td_buf *b, const ta_data_cell *c, bool as_float)
{
    uint64_t bits;
    double d;

    if (c->null)
    {
        td_buf_addc(b, 0);
        return;
    }
    if (as_float && td_kind_numeric(c->type))
    {
        td_cell_num(c, &d);
        if (d == 0)
            d = 0;              /* -0.0 hashes as 0.0 */
        if (isnan(d))
            d = NAN;
        memcpy(&bits, &d, 8);
        td_buf_addc(b, TA_DATA_T_FLOAT);
        put64(b, bits);
        return;
    }
    td_buf_addc(b, (char)c->type);
    switch (c->type)
    {
        case TA_DATA_T_FLOAT:
            d = c->d == 0 ? 0 : c->d;
            if (isnan(d))
                d = NAN;
            memcpy(&bits, &d, 8);
            put64(b, bits);
            break;
        case TA_DATA_T_DECIMAL:
            td_buf_addc(b, (char)c->scale);
            put64(b, (uint64_t)c->i);
            break;
        case TA_DATA_T_BOOL:
        case TA_DATA_T_INT:
        case TA_DATA_T_DATE:
        case TA_DATA_T_TIMESTAMP:
            put64(b, (uint64_t)c->i);
            break;
        default:
            put64(b, (uint64_t)c->len);
            td_buf_add(b, c->s, c->len);
            break;
    }
}

int
td_cell_deser(const char **pp, const char *end, ta_data_cell *c)
{
    const char *p = *pp;
    int type;
    uint64_t v;

    ta_data_cell_clear(c);
    if (p >= end)
        return -1;
    type = (unsigned char)*p++;
    if (type == 0)
    {
        *pp = p;
        return 0;
    }
    c->null = 0;
    c->type = type;
    if (type == TA_DATA_T_DECIMAL)
    {
        if (p >= end)
            return -1;
        c->scale = (unsigned char)*p++;
    }
    if (end - p < 8)
        return -1;
    v = get64(p);
    p += 8;
    switch (type)
    {
        case TA_DATA_T_FLOAT:
            memcpy(&c->d, &v, 8);
            break;
        case TA_DATA_T_DECIMAL:
        case TA_DATA_T_BOOL:
        case TA_DATA_T_INT:
        case TA_DATA_T_DATE:
        case TA_DATA_T_TIMESTAMP:
            c->i = (int64_t)v;
            break;
        default:
            if ((uint64_t)(end - p) < v)
                return -1;
            c->s = td_strndup(p, (size_t)v);
            c->len = (size_t)v;
            p += v;
            break;
    }
    *pp = p;
    return 0;
}
