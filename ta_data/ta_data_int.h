/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: internals shared by the core units
 *
 * Not installed and not part of any contract: the buffer, option list,
 * memory budget, hash, PRNG, type tree, row, reader and spill-partition
 * primitives the core's translation units share. Plain C, no TE header.
 */

#ifndef __TA_DATA_INT_H__
#define __TA_DATA_INT_H__

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>

#include "ta_data_core.h"

/* ---- growable byte buffer --------------------------------------- */

typedef struct td_buf {
    char   *ptr;
    size_t  len;
    size_t  cap;
} td_buf;

#define TD_BUF_INIT { NULL, 0, 0 }

extern int td_buf_reserve(td_buf *b, size_t extra);
extern int td_buf_add(td_buf *b, const void *data, size_t len);
extern int td_buf_addc(td_buf *b, char c);
extern int td_buf_adds(td_buf *b, const char *s);
extern int td_buf_printf(td_buf *b, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
extern int td_buf_vprintf(td_buf *b, const char *fmt, va_list ap);
extern void td_buf_reset(td_buf *b);
extern void td_buf_free(td_buf *b);
/** Hand the buffer's text over (never NULL); the buffer is left empty. */
extern char *td_buf_take(td_buf *b);

/** Append @p s percent-escaped: TAB, LF, CR, '%', ',', ':', '=', '|'. */
extern void td_esc(td_buf *b, const char *s, size_t len);

/** Set *err (when err != NULL and *err is NULL) and return @p rc. */
extern int td_fail(char **err, int rc, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

extern char *td_strdup(const char *s);
extern char *td_strndup(const char *s, size_t n);

/* ---- key=value options -------------------------------------------- */

typedef struct td_kv {
    size_t  n;
    char  **key;
    char  **val;
} td_kv;

#define TD_KV_INIT { 0, NULL, NULL }

/** Parse "k=v" lines and append to @p kv. */
extern int td_kv_parse(td_kv *kv, const char *text);
/** Append one entry as is (no parsing: the value may hold anything). */
extern int td_kv_set(td_kv *kv, const char *key, const char *val);
extern void td_kv_free(td_kv *kv);
/** Later entries win. */
extern const char *td_kv_get(const td_kv *kv, const char *key,
                             const char *def);
extern int64_t td_kv_int(const td_kv *kv, const char *key, int64_t def);
extern double td_kv_dbl(const td_kv *kv, const char *key, double def);
extern bool td_kv_bool(const td_kv *kv, const char *key, bool def);

/** Split @p s on @p sep into a heap array of heap strings (trimmed). */
extern size_t td_split(const char *s, char sep, char ***out);
extern void td_split_free(char **v, size_t n);

/* ---- memory budget ------------------------------------------------ */

typedef struct td_mem {
    uint64_t limit;     /**< 0 = unlimited */
    uint64_t used;
    uint64_t peak;
} td_mem;

/** Charge @p n bytes; false (nothing charged) when over the limit. */
extern bool td_mem_take(td_mem *m, uint64_t n);
extern void td_mem_give(td_mem *m, uint64_t n);

/** Default operation memory budget: 256 MiB. */
#define TD_MEM_DEFAULT (256ULL << 20)

/* ---- hashing and PRNG --------------------------------------------- */

extern uint64_t td_hash64(const void *data, size_t len, uint64_t seed);

typedef struct td_rng {
    uint64_t s[4];
} td_rng;

extern void td_rng_seed(td_rng *r, uint64_t seed);
extern uint64_t td_rng_next(td_rng *r);
/** Uniform in [0, 1). */
extern double td_rng_double(td_rng *r);
/** Uniform integer in [lo, hi]. */
extern int64_t td_rng_range(td_rng *r, int64_t lo, int64_t hi);
extern double td_rng_normal(td_rng *r);

/* ---- types and schemas -------------------------------------------- */

typedef struct td_type td_type;

typedef struct td_field {
    char       *name;
    td_type    *type;
    bool        required;
} td_field;

struct td_type {
    int         kind;       /**< ta_data_type */
    int         precision;  /**< DECIMAL */
    int         scale;      /**< DECIMAL */
    td_type    *elem;       /**< ARRAY element, NULL = any */
    size_t      nfields;    /**< OBJECT fields, 0 = any */
    td_field   *fields;
};

typedef struct td_col {
    char       *name;
    td_type    *type;
    bool        nullable;
} td_col;

typedef struct td_schema {
    size_t      n;
    td_col     *cols;
    bool        explicit_;
} td_schema;

extern int td_type_parse(const char *text, td_type **out, char **err);
extern td_type *td_type_simple(int kind);
extern td_type *td_type_dup(const td_type *t);
extern void td_type_free(td_type *t);
extern void td_type_format(td_buf *b, const td_type *t);
extern const char *td_kind_name(int kind);
extern bool td_kind_numeric(int kind);

/** Parse schema text: lines "name:type[!]" or "name<TAB>type<TAB>nullable". */
extern int td_schema_parse(const char *text, td_schema *s, char **err);
extern void td_schema_free(td_schema *s);
extern int td_schema_find(const td_schema *s, const char *name);
extern int td_schema_add(td_schema *s, const char *name, td_type *type,
                         bool nullable);

/* ---- cells -------------------------------------------------------- */

/** Parse text as a value of @p t. 0 ok, else not representable. */
extern int td_parse_text(const char *s, size_t len, const td_type *t,
                         ta_data_cell *out);
extern int td_parse_date(const char *s, size_t len, int64_t *days);
extern int td_parse_timestamp(const char *s, size_t len, int64_t *us);
extern int td_parse_decimal(const char *s, size_t len, int scale,
                            int64_t *unscaled);

/** Rewrite an ARRAY/OBJECT cell's JSON in canonical form. */
extern int td_json_canon_cell(ta_data_cell *c);

/** Canonical text of a value (no escaping). NULL prints as "". */
extern void td_cell_text(td_buf *b, const ta_data_cell *c);
/** The value as a JSON literal. */
extern void td_cell_json(td_buf *b, const ta_data_cell *c);
extern void td_fmt_date(td_buf *b, int64_t days);
extern void td_fmt_timestamp(td_buf *b, int64_t us);
extern void td_fmt_double(td_buf *b, double d);
extern void td_fmt_decimal(td_buf *b, int64_t unscaled, int scale);

/** The value as a double (numeric, bool, date, timestamp). */
extern bool td_cell_num(const ta_data_cell *c, double *d);

extern int td_cell_copy(ta_data_cell *dst, const ta_data_cell *src);
extern int td_cell_set_str(ta_data_cell *c, int type, const char *s,
                           size_t len);

/**
 * Bring a source value to its column type.
 *
 * @return 0 when it fits (natively, by a lossless widening, or by text
 *         interpretation), 1 when it fits only by coercion (and coercion
 *         was allowed: then *coerced is set), -1 when it does not fit.
 */
extern int td_cell_conform(ta_data_cell *c, const td_type *t, bool coerce,
                           bool *coerced);

/** Does JSON text @p s conform to the nested type @p t? Path on fail. */
extern bool td_json_conforms(const char *s, size_t len, const td_type *t,
                             td_buf *path);

/** Ordering of two non-NULL values of comparable kinds. */
extern int td_cell_order(const ta_data_cell *a, const ta_data_cell *b);

/** Comparison tolerances. */
typedef struct td_tol {
    double  abs_tol;
    double  rel_tol;
    int64_t ts_tol_us;
    bool    null_eq;        /**< NULL equals NULL */
    bool    trim;           /**< ignore surrounding blanks in strings */
    bool    nocase;         /**< case-insensitive strings */
} td_tol;

extern bool td_cell_equal(const ta_data_cell *a, const ta_data_cell *b,
                          const td_tol *tol);

/**
 * Canonical, decodable serialization of a value. @p as_float folds
 * every numeric kind into a double so that 5 and 5.0 hash alike.
 */
extern void td_cell_ser(td_buf *b, const ta_data_cell *c, bool as_float);
extern int td_cell_deser(const char **p, const char *end, ta_data_cell *c);

/* ---- rows --------------------------------------------------------- */

typedef struct td_row {
    size_t          n;
    ta_data_cell   *cells;
    uint64_t        rowno;      /**< 1-based record number in the source */
} td_row;

extern int td_row_init(td_row *r, size_t n);
extern void td_row_clear(td_row *r);
extern void td_row_free(td_row *r);

/* ---- datasets and readers ----------------------------------------- */

typedef enum td_format {
    TD_FMT_CSV = 1,
    TD_FMT_JSON,
    TD_FMT_JSONL,
    TD_FMT_PARQUET,
    TD_FMT_SQLITE,
} td_format;

extern int td_format_parse(const char *name, const char *path,
                           td_format *fmt);
extern const char *td_format_name(td_format fmt);

struct ta_data_ds {
    char       *path;
    td_format   format;
    td_kv       opts;
    td_schema   schema;
    char      **src_names;      /**< columns the source declares */
    size_t      nsrc;
    uint64_t    file_bytes;
};

typedef struct td_reader td_reader;

struct td_reader {
    ta_data_ds *ds;
    /** 0 row, 1 EOF, TA_DATA_EILSEQ malformed (skipped), else fatal. */
    int (*next)(td_reader *r, td_row *row, char **err);
    void (*close)(td_reader *r);
    /** Fields the source had that the schema does not (JSON). */
    uint64_t    unexpected;
    char       *unexpected_names;   /**< first few, ',' separated */
    void       *priv;
};

extern int td_reader_open(ta_data_ds *ds, const td_kv *opts, td_reader **r,
                          char **err);
extern void td_reader_close(td_reader *r);

/** Schema discovery, per format (td_reader.c). */
extern int td_infer_schema(ta_data_ds *ds, char **err);

/** A typed scan over a dataset: reader + conversion + counters. */
typedef struct td_scan {
    ta_data_ds *ds;
    td_reader  *rd;
    td_row      row;
    bool        fail_on_error;
    bool        coerce;
    uint64_t    rows;           /**< rows delivered */
    uint64_t    malformed;      /**< records skipped */
    uint64_t    mismatched;     /**< rows with a value not fitting */
    uint64_t    coerced;        /**< values coerced */
    uint64_t   *col_bad;        /**< per column: values not fitting */
    char       *first_error;
} td_scan;

/** Options: merged dataset options + per-call options. */
extern int td_scan_open(td_scan *s, ta_data_ds *ds, const td_kv *opts,
                        char **err);
/** 0 row in s->row, 1 EOF, else error. */
extern int td_scan_next(td_scan *s, char **err);
extern void td_scan_close(td_scan *s);
/** Append rows/malformed/mismatched/coerced/first_error to a result. */
extern void td_scan_result(const td_scan *s, td_buf *res);

/** Merge ds->opts and the call's params into @p out. */
extern int td_opts(const ta_data_ds *ds, const char *params, td_kv *out);
extern uint64_t td_opts_mem(const td_kv *opts);

/* ---- writers ------------------------------------------------------ */

typedef struct td_writer td_writer;

extern int td_writer_open(const char *path, td_format fmt,
                          const td_schema *schema, const td_kv *opts,
                          td_writer **w, char **err);
extern int td_writer_row(td_writer *w, const td_row *row, char **err);
/** A deliberately malformed record (generator). */
extern int td_writer_malformed(td_writer *w, const td_row *row, td_rng *rng,
                               char **err);
extern int td_writer_close(td_writer *w, uint64_t *bytes, char **err);

/* ---- spill partitions / grouping ---------------------------------- */

/** One record handed to a group callback. */
typedef struct td_rec {
    int             side;       /**< 0 = first input, 1 = second */
    uint64_t        rowno;
    const char     *payload;
    size_t          plen;
} td_rec;

/**
 * Called once per distinct key with every record of that key, in input
 * order per side. Return non-zero to abort.
 */
typedef int (*td_group_cb)(void *ctx, const char *key, size_t klen,
                           const td_rec *recs, size_t nrecs, char **err);

typedef struct td_group td_group;

/**
 * A hash-partitioned group-by over spill files. Records are written to
 * partitions on disk (unlinked temporaries, mode 0600, in @p tmpdir);
 * td_group_run() then loads one partition at a time and, when one does
 * not fit the budget, re-splits it with a different hash seed. So the
 * whole input never has to be in memory at once.
 */
extern int td_group_open(td_group **g, td_mem *mem, const char *tmpdir,
                         uint64_t size_hint, char **err);
extern int td_group_add(td_group *g, int side, uint64_t rowno,
                        const void *key, size_t klen, const void *payload,
                        size_t plen, char **err);
extern int td_group_run(td_group *g, td_group_cb cb, void *ctx, char **err);
extern void td_group_close(td_group *g);
/** Partitions used (after run), for reporting. */
extern unsigned int td_group_partitions(const td_group *g);

/* ---- bounded deterministic report --------------------------------- */

typedef struct td_report {
    size_t      max;
    size_t      n;
    uint64_t    total;      /**< entries ever added */
    uint64_t    digest;     /**< order-independent digest of all of them */
    struct td_report_ent {
        int         kind;
        uint64_t    ka;
        uint64_t    kb;
        char       *line;
    } *ent;
} td_report;

extern void td_report_init(td_report *r, size_t max);
/** Keep the @c max smallest (kind, ka, kb) entries. */
extern void td_report_add(td_report *r, int kind, uint64_t ka, uint64_t kb,
                          const char *line);
/** Sorted lines, and a 64-bit digest of every line ever added. */
extern char *td_report_take(td_report *r);
extern void td_report_free(td_report *r);

/* ---- HyperLogLog -------------------------------------------------- */

#define TD_HLL_P 14
typedef struct td_hll {
    uint8_t reg[1u << TD_HLL_P];
} td_hll;

extern void td_hll_add(td_hll *h, uint64_t hash);
extern double td_hll_count(const td_hll *h);

/* ---- check helpers shared by profile/check/compare ---------------- */

/** Resolve a comma-separated column list to schema indices. */
extern int td_cols_resolve(const ta_data_ds *ds, const char *list,
                           size_t **idx, size_t *n, char **err);

#endif /* !__TA_DATA_INT_H__ */
