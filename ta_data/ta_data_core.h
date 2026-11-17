/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: the plain-C core
 *
 * Everything tsf-data does with data happens here, on the agent, next to
 * the data: reading CSV, JSON, JSON Lines, Parquet and SQLite query
 * results into one logical row/column model, and running profiling,
 * comparison, quality checks, generation and conversion over it as a
 * stream.
 *
 * The core includes no Test Environment header. It is plain C (C11) over
 * jansson, SQLite and POSIX regex, and it reaches Apache Arrow/Parquet
 * only through the C ABI in ta_data_parquet.cpp (the one C++ unit). That
 * keeps it buildable and testable on its own (see ci/), and it is what
 * the TE glue in ta_data.c wraps for the RPC server.
 *
 * Every operation takes its options as a @c "key=value" list, one per
 * line, and returns its result the same way, plus an optional report of
 * newline-separated, tab-separated records. Strings returned through
 * @c char** are heap-allocated and owned by the caller. Every function
 * returns a @c TA_DATA_E* code; on failure @p err (when not @c NULL)
 * receives a human-readable reason that names the row and the column
 * where there is one.
 */

#ifndef __TA_DATA_CORE_H__
#define __TA_DATA_CORE_H__

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Return codes of the core. */
enum {
    TA_DATA_OK = 0,
    TA_DATA_EINVAL = 1,     /**< Bad option, schema or argument. */
    TA_DATA_ENOENT = 2,     /**< File or column not found. */
    TA_DATA_EIO = 3,        /**< Read/write failure. */
    TA_DATA_ECORRUPT = 4,   /**< Input is corrupted or truncated. */
    TA_DATA_EILSEQ = 5,     /**< A malformed record, or a value that does
                                 not fit its column (on_error=fail). */
    TA_DATA_E2BIG = 6,      /**< Needs more memory than mem_limit allows. */
    TA_DATA_ENOMEM = 7,     /**< Allocation failure. */
    TA_DATA_ENOTSUP = 8,    /**< Feature not compiled in. */
};

/** Logical types of the common dataset model. Wire values. */
typedef enum ta_data_type {
    TA_DATA_T_UNKNOWN = 0,
    TA_DATA_T_BOOL = 1,
    TA_DATA_T_INT = 2,
    TA_DATA_T_FLOAT = 3,
    TA_DATA_T_DECIMAL = 4,
    TA_DATA_T_STRING = 5,
    TA_DATA_T_BINARY = 6,
    TA_DATA_T_DATE = 7,
    TA_DATA_T_TIMESTAMP = 8,
    TA_DATA_T_ARRAY = 9,
    TA_DATA_T_OBJECT = 10,
} ta_data_type;

/** Optional features, reported by ta_data_features(). Wire values. */
enum {
    TA_DATA_FEAT_PARQUET = 1,   /**< Apache Parquet, via Arrow C++. */
    TA_DATA_FEAT_SQLITE = 2,    /**< SQLite query results. */
};

/**
 * One value. The representation depends on @c type:
 *
 * - BOOL: @c i is 0 or 1;
 * - INT: @c i;
 * - FLOAT: @c d;
 * - DECIMAL: @c i is the unscaled value at @c scale (precision <= 18);
 * - DATE: @c i is days since 1970-01-01;
 * - TIMESTAMP: @c i is microseconds since 1970-01-01T00:00:00Z;
 * - STRING, BINARY: @c s / @c len (heap, owned by the cell);
 * - ARRAY, OBJECT: @c s / @c len holds canonical compact JSON;
 * - a CSV/raw source delivers STRING cells with @c raw set: the text
 *   has not been interpreted yet.
 */
typedef struct ta_data_cell {
    int         null;       /**< SQL NULL / JSON null / empty CSV field. */
    int         bad;        /**< The value did not fit its column. */
    int         raw;        /**< Uninterpreted text (CSV). */
    int         type;       /**< ta_data_type of the value. */
    int         scale;      /**< DECIMAL scale. */
    int64_t     i;
    double      d;
    char       *s;
    size_t      len;
} ta_data_cell;

/** Free the heap part of a cell and reset it to NULL. */
extern void ta_data_cell_clear(ta_data_cell *c);

/*
 * ---- Datasets ------------------------------------------------------
 */

/** An opened dataset: its source, options and schema. */
typedef struct ta_data_ds ta_data_ds;

/**
 * Open a dataset and settle its schema.
 *
 * @param path     File (or, for "sqlite", the database file).
 * @param format   "csv", "json", "jsonl", "parquet", "sqlite", or ""
 *                 to take it from the file extension.
 * @param params   Options: delimiter, quote, header, null, infer_rows,
 *                 coerce, on_error (fail|count), mem_limit, tmpdir,
 *                 query (sqlite). See README.
 * @param schema   Explicit schema text (see ta_data_schema_parse()), or
 *                 NULL / "" to infer it from the data.
 */
extern int ta_data_ds_open(const char *path, const char *format,
                           const char *params, const char *schema,
                           ta_data_ds **ds, char **err);

/** Release a dataset. */
extern void ta_data_ds_free(ta_data_ds *ds);

/**
 * The dataset's schema as text: one column per line,
 * @c "name<TAB>type<TAB>nullable<TAB>source", where source is
 * "explicit" or "inferred" and type is in the schema grammar.
 */
extern int ta_data_ds_schema(ta_data_ds *ds, char **out);

/*
 * ---- Operations (results as key=value lines) -----------------------
 *
 * @p params of every operation may override the dataset's own options
 * (mem_limit, on_error, coerce, tmpdir) for that one call.
 */

extern int ta_data_count(ta_data_ds *ds, const char *params, char **result,
                         char **err);

extern int ta_data_sample(ta_data_ds *ds, const char *params, char **result,
                          char **rows, char **err);

extern int ta_data_profile(ta_data_ds *ds, const char *params, char **result,
                           char **err);

extern int ta_data_compare(ta_data_ds *a, ta_data_ds *b, const char *params,
                           char **result, char **report, char **err);

extern int ta_data_check(ta_data_ds *ds, ta_data_ds *ref, const char *kind,
                         const char *params, char **result, char **report,
                         char **err);

extern int ta_data_generate(const char *path, const char *format,
                            const char *params, const char *spec,
                            char **result, char **err);

extern int ta_data_convert(ta_data_ds *ds, const char *path,
                           const char *format, const char *params,
                           char **result, char **err);

extern int ta_data_aggregate(ta_data_ds *ds, const char *path,
                             const char *format, const char *params,
                             char **result, char **err);

extern int ta_data_reconcile(ta_data_ds *src, ta_data_ds *dst,
                             const char *params, char **result,
                             char **report, char **err);

extern int ta_data_digest(ta_data_ds *ds, const char *params, char **result,
                          char **err);

/** Features bitmask (TA_DATA_FEAT_*) and library versions. */
extern int ta_data_features(unsigned int *features, char **versions);

/** Human-readable name of a TA_DATA_E* code. */
extern const char *ta_data_strerror(int rc);

/*
 * ---- Parquet seam (ta_data_parquet.cpp) ----------------------------
 *
 * Plain C over Arrow C++. Return 0 ok, 1 end of data (next only),
 * TA_DATA_E* on failure with @p err set. Strings in cells are heap and
 * owned by the caller after a successful next.
 */

/** Column description used by the Parquet seam. */
typedef struct ta_data_pq_col {
    char       *name;       /**< heap */
    int         type;       /**< ta_data_type */
    int         nullable;
    int         precision;
    int         scale;
    char       *type_text;  /**< heap: full type, e.g. "array<int>" */
} ta_data_pq_col;

extern int ta_data_pq_supported(void);
extern const char *ta_data_pq_version(void);

extern int ta_data_pq_open(const char *path, int batch_rows, void **h,
                           int *ncols, char **err);
extern int ta_data_pq_column(void *h, int i, ta_data_pq_col *col);
extern int ta_data_pq_num_rows(void *h, int64_t *rows);
extern int ta_data_pq_next(void *h, ta_data_cell *cells, char **err);
extern void ta_data_pq_close(void *h);

extern int ta_data_pq_wopen(const char *path, int ncols,
                            const ta_data_pq_col *cols, int64_t group_rows,
                            void **h, char **err);
extern int ta_data_pq_write(void *h, const ta_data_cell *cells, char **err);
extern int ta_data_pq_wclose(void *h, char **err);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TA_DATA_CORE_H__ */
