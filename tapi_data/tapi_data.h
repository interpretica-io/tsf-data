/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Dataset validation TAPI
 *
 * Engine-side API of tsf-data. A dataset is opened on a Test Agent and
 * stays there: profiling, comparison, quality checks, generation and
 * conversion all run in the agent's RPC server, next to the data, and
 * only bounded samples and reports come back. So a test can validate a
 * multi-gigabyte dataset without the engine ever holding it.
 *
 * Results come as structures with the common numbers decoded, plus the
 * full "key=value" result (@c kv) and the report records for anything
 * else. Quality checks take an explicit threshold and decide pass/fail
 * here, on the engine; tapi_data_check_verdict() turns a failure into a
 * TE verdict with a stable, descriptive text.
 *
 * Options are passed as one string of "key=value" lines (or ';'
 * separated, as a shorthand - not for values that contain ';').
 *
 * @code
 * tapi_data *src = NULL;
 * tapi_data_check chk = TAPI_DATA_CHECK_INIT;
 *
 * if (!tapi_data_available(pco))
 *     TEST_SKIP("The agent has no dataset engine");
 * CHECK_RC(tapi_data_open(pco, "/data/orders.parquet", NULL, NULL, NULL,
 *                         &src));
 * CHECK_RC(tapi_data_check_unique(src, "order_id", NULL,
 *                                 &TAPI_DATA_NONE_ALLOWED, &chk));
 * tapi_data_check_verdict(&chk);
 * tapi_data_check_free(&chk);
 * tapi_data_close(src);
 * @endcode
 */

#ifndef __TAPI_DATA_H__
#define __TAPI_DATA_H__

#include <stdint.h>

#include "te_defs.h"
#include "te_errno.h"
#include "te_string.h"
#include "rcf_rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Optional engine features (tapi_data_supports()). */
typedef enum tapi_data_feature {
    TAPI_DATA_FEAT_PARQUET = 1,     /**< Apache Parquet (Arrow C++). */
    TAPI_DATA_FEAT_SQLITE = 2,      /**< SQLite query results. */
} tapi_data_feature;

/* ---- generic result access ---------------------------------------- */

/** A parsed "key=value" result. Values are unescaped. */
typedef struct tapi_data_kv {
    size_t      n;
    char      **key;
    char      **val;
} tapi_data_kv;

#define TAPI_DATA_KV_INIT { 0, NULL, NULL }

/** Parse "key=value" lines into @p kv (appending). */
extern void tapi_data_kv_parse(tapi_data_kv *kv, const char *text);
extern const char *tapi_data_kv_get(const tapi_data_kv *kv, const char *key,
                                    const char *def);
extern int64_t tapi_data_kv_int(const tapi_data_kv *kv, const char *key,
                                int64_t def);
extern double tapi_data_kv_dbl(const tapi_data_kv *kv, const char *key,
                               double def);
extern void tapi_data_kv_free(tapi_data_kv *kv);

/** Unescape one percent-escaped field (heap). */
extern char *tapi_data_unescape(const char *s);

/**
 * Split a report record into its tab-separated fields, unescaped.
 *
 * @return Number of fields; free with tapi_data_fields_free().
 */
extern size_t tapi_data_fields(const char *line, char ***fields);
extern void tapi_data_fields_free(char **fields, size_t n);

/** The agent's reason for the last failed call of this process. */
extern const char *tapi_data_last_error(void);

/* ---- availability ------------------------------------------------- */

/** Is the dataset engine in the agent's RPC server? */
extern te_bool tapi_data_available(rcf_rpc_server *rpcs);

/** Is the optional feature compiled into the agent? */
extern te_bool tapi_data_supports(rcf_rpc_server *rpcs,
                                  tapi_data_feature feature);

/** Library versions on the agent ("jansson=... sqlite=... arrow=..."). */
extern te_errno tapi_data_versions(rcf_rpc_server *rpcs, te_string *out);

/** The RPC server's open datasets and memory use. */
typedef struct tapi_data_stats {
    unsigned int    open;       /**< datasets open */
    uint64_t        ops;        /**< operations served */
    long            rss_kb;     /**< resident set size now */
    long            maxrss_kb;  /**< peak resident set size */
} tapi_data_stats;

extern te_errno tapi_data_get_stats(rcf_rpc_server *rpcs,
                                    tapi_data_stats *stats);

/* ---- datasets ----------------------------------------------------- */

/** One column of a schema. */
typedef struct tapi_data_column {
    char       *name;
    char       *type;       /**< in the schema grammar, e.g. "decimal(10,2)" */
    te_bool     nullable;
} tapi_data_column;

/** A schema: what tapi_data_schema() reads back. */
typedef struct tapi_data_schema_info {
    size_t              n;
    tapi_data_column   *cols;
    te_bool             inferred;
} tapi_data_schema_info;

/** A dataset opened on an agent. */
typedef struct tapi_data {
    rcf_rpc_server         *rpcs;
    int                     handle;
    char                   *path;
    char                   *format;
    tapi_data_schema_info   schema;     /**< as opened */
} tapi_data;

/**
 * Open a dataset on the agent.
 *
 * @param rpcs     RPC server on the agent that holds the data.
 * @param path     File on the agent (for "sqlite": the database).
 * @param format   "csv", "json", "jsonl", "parquet", "sqlite", or NULL
 *                 to take it from the extension.
 * @param params   Options: delimiter, header, null, infer_rows, coerce,
 *                 on_error=fail|count, mem_limit, tmpdir, query/table
 *                 (sqlite).
 * @param schema   Explicit schema ("id:int!, name:string, ...") or NULL
 *                 to infer it.
 * @param ds       The dataset; release with tapi_data_close().
 */
extern te_errno tapi_data_open(rcf_rpc_server *rpcs, const char *path,
                               const char *format, const char *params,
                               const char *schema, tapi_data **ds);

/** Release a dataset (NULL is a no-op). */
extern te_errno tapi_data_close(tapi_data *ds);

/**
 * Free the engine-side handle only, without an RPC: for a dataset whose
 * RPC server is gone (killed, restarted), where there is nothing left on
 * the agent to release.
 */
extern void tapi_data_forget(tapi_data *ds);

/** Read the schema back. Free with tapi_data_schema_free(). */
extern te_errno tapi_data_schema(tapi_data *ds, tapi_data_schema_info *out);
extern void tapi_data_schema_free(tapi_data_schema_info *s);
/** Index of a column, or -1. */
extern int tapi_data_schema_find(const tapi_data_schema_info *s,
                                 const char *name);

/** What a full pass over the data found. */
typedef struct tapi_data_counts {
    uint64_t        rows;
    uint64_t        malformed;      /**< records that did not parse */
    uint64_t        mismatched;     /**< rows with a value not fitting */
    uint64_t        coerced;        /**< values coerced (coerce=1) */
    uint64_t        unexpected;     /**< fields outside the schema */
    uint64_t        null_cells;
    char           *first_error;    /**< or NULL */
    tapi_data_kv    kv;
} tapi_data_counts;

extern te_errno tapi_data_count(tapi_data *ds, const char *params,
                                tapi_data_counts *out);
extern void tapi_data_counts_free(tapi_data_counts *c);

/**
 * A bounded sample as JSON Lines, one object per row with "_row".
 * params: offset, limit (at most 10000), mode=head|random, seed,
 * columns.
 */
extern te_errno tapi_data_sample(tapi_data *ds, const char *params,
                                 te_string *jsonl, unsigned int *returned);

/* ---- profiling ---------------------------------------------------- */

#define TAPI_DATA_MAX_Q    16
#define TAPI_DATA_MAX_TOP  32

/** Statistics of one column. */
typedef struct tapi_data_col_profile {
    char       *name;
    char       *type;
    uint64_t    count;          /**< non-NULL values */
    uint64_t    nulls;
    uint64_t    bad;            /**< values not fitting the type */
    double      null_pct;
    double      distinct;
    te_bool     distinct_exact;
    char       *distinct_algo;
    double      distinct_err;   /**< relative standard error */
    char       *min;
    char       *max;
    te_bool     has_moments;
    double      mean;
    double      stddev;         /**< sample (n-1) */
    double      stddev_pop;
    double      sum;
    size_t      nq;
    double      q[TAPI_DATA_MAX_Q];
    char       *qv[TAPI_DATA_MAX_Q];    /**< as text (dates stay dates) */
    te_bool     q_exact;
    char       *q_algo;
    double      q_err;          /**< absolute error bound */
    te_bool     has_len;
    double      len_min;
    double      len_max;
    double      len_mean;
    size_t      ntop;
    char       *top[TAPI_DATA_MAX_TOP];
    uint64_t    top_count[TAPI_DATA_MAX_TOP];
    te_bool     top_exact;
    char       *top_algo;
    double      top_err;        /**< count undercount bound */
    tapi_data_kv kv;
} tapi_data_col_profile;

typedef struct tapi_data_profile_info {
    uint64_t                rows;
    unsigned int            passes;
    uint64_t                mem_peak;
    size_t                  ncols;
    tapi_data_col_profile  *cols;
    tapi_data_kv            kv;
} tapi_data_profile_info;

/**
 * Profile a dataset. params: columns, quantiles (e.g. "0.1,0.5,0.9"),
 * topk, mode=auto|exact|approx, mem_limit.
 */
extern te_errno tapi_data_profile(tapi_data *ds, const char *params,
                                  tapi_data_profile_info *out);
extern const tapi_data_col_profile *tapi_data_profile_col(
                                    const tapi_data_profile_info *p,
                                    const char *name);
/** The quantile @p q of a column (NAN when not computed). */
extern double tapi_data_profile_quantile(const tapi_data_col_profile *c,
                                         double q);
extern void tapi_data_profile_log(const tapi_data_profile_info *p);
extern void tapi_data_profile_free(tapi_data_profile_info *p);

/* ---- comparison --------------------------------------------------- */

/** What tapi_data_compare() found. */
typedef struct tapi_data_diff {
    uint64_t        rows_a;
    uint64_t        rows_b;
    uint64_t        matched;
    uint64_t        missing;        /**< in A, not in B */
    uint64_t        extra;          /**< in B, not in A */
    uint64_t        mismatched;     /**< in both, values differ */
    uint64_t        dup_a;
    uint64_t        dup_b;
    uint64_t        missing_columns;
    uint64_t        extra_columns;
    uint64_t        type_diffs;
    int             equal;          /**< 1, 0, or -1 (distribution) */
    unsigned int    partitions;
    uint64_t        mem_peak;
    uint64_t        report_total;
    char            digest[17];     /**< of every difference found */
    tapi_data_kv    kv;
    te_string       report;
} tapi_data_diff;

#define TAPI_DATA_DIFF_INIT { .report = TE_STRING_INIT }

/**
 * Compare A (expected) with B (actual).
 *
 * params: mode=exact|unordered|key|schema|distribution, keys, ignore,
 * columns, map (a:b,...), abs_tol, rel_tol, ts_tol_us, null_eq, trim,
 * nocase, dups=first|multiset, round, max_report, mem_limit, tmpdir.
 */
extern te_errno tapi_data_compare(tapi_data *a, tapi_data *b,
                                  const char *params, tapi_data_diff *out);
extern void tapi_data_diff_log(const tapi_data_diff *d);
extern void tapi_data_diff_free(tapi_data_diff *d);

/* ---- quality checks ----------------------------------------------- */

/**
 * How much a check tolerates. A negative field is not applied.
 *
 * - max_count: violations allowed;
 * - max_fraction: violations / rows checked allowed;
 * - max_stat: for distribution checks, the statistic (TVD or KS).
 */
typedef struct tapi_data_threshold {
    int64_t     max_count;
    double      max_fraction;
    double      max_stat;
} tapi_data_threshold;

/** No violation allowed. */
extern const tapi_data_threshold TAPI_DATA_NONE_ALLOWED;

/** A check's outcome. Initialise with TAPI_DATA_CHECK_INIT. */
typedef struct tapi_data_check {
    char                   *kind;
    char                   *target;     /**< column(s) checked */
    uint64_t                checked;
    uint64_t                violations;
    double                  fraction;
    double                  stat;       /**< distribution checks */
    te_bool                 passed;
    tapi_data_threshold     thr;
    char                   *summary;    /**< stable, verdict-ready */
    tapi_data_kv            kv;
    te_string               report;
} tapi_data_check;

#define TAPI_DATA_CHECK_INIT { .report = TE_STRING_INIT }

/**
 * Run a check of any kind; the named functions below are this with the
 * kind's own options set.
 *
 * @param ref   Reference dataset (reference, distribution), or NULL.
 */
extern te_errno tapi_data_check_run(tapi_data *ds, tapi_data *ref,
                                    const char *kind, const char *target,
                                    const char *params,
                                    const tapi_data_threshold *thr,
                                    tapi_data_check *res);

/**
 * Schema: columns, types, nullability, unexpected columns, values.
 * params: mode=strict|compatible, allow_extra, coerce, data=0 to skip
 * the data pass. Violations are the error-severity findings.
 */
extern te_errno tapi_data_check_schema(tapi_data *ds, const char *schema,
                                       const char *params,
                                       const tapi_data_threshold *thr,
                                       tapi_data_check *res);

/** NULLs in a column. */
extern te_errno tapi_data_check_nulls(tapi_data *ds, const char *column,
                                      const char *params,
                                      const tapi_data_threshold *thr,
                                      tapi_data_check *res);

/** Duplicate keys over one or more columns ("a,b"). */
extern te_errno tapi_data_check_unique(tapi_data *ds, const char *columns,
                                       const char *params,
                                       const tapi_data_threshold *thr,
                                       tapi_data_check *res);

/** Values outside [min, max] (either may be NULL), in column type. */
extern te_errno tapi_data_check_range(tapi_data *ds, const char *column,
                                      const char *min, const char *max,
                                      const char *params,
                                      const tapi_data_threshold *thr,
                                      tapi_data_check *res);

/**
 * Values not matching a POSIX extended regular expression (full match),
 * or a named pattern when @p regex starts with "@" ("@email", "@uuid",
 * "@ipv4", "@date", "@digits", "@alnum"). params: invert=1 to flag
 * values that DO match.
 */
extern te_errno tapi_data_check_pattern(tapi_data *ds, const char *column,
                                        const char *regex,
                                        const char *params,
                                        const tapi_data_threshold *thr,
                                        tapi_data_check *res);

/**
 * Freshness over an event-time column. params: ingest (ingestion-time
 * column), now (ISO timestamp; default the agent clock), skew_us,
 * max_age_us, max_delay_us, interval_us (gap detection), id (duplicate
 * event ids), seq (sequence gaps).
 */
extern te_errno tapi_data_check_freshness(tapi_data *ds, const char *event,
                                          const char *params,
                                          const tapi_data_threshold *thr,
                                          tapi_data_check *res);

/**
 * A column's distribution: against expected shares (params
 * freq=a:0.5|b:0.3|c:0.2, statistic TVD) or against the same column of a
 * reference dataset (@p ref, statistic KS or TVD). Use thr->max_stat.
 */
extern te_errno tapi_data_check_distribution(tapi_data *ds,
                                             const char *column,
                                             tapi_data *ref,
                                             const char *params,
                                             const tapi_data_threshold *thr,
                                             tapi_data_check *res);

/** Values of @p column absent from @p ref_column of @p ref. */
extern te_errno tapi_data_check_reference(tapi_data *ds, const char *column,
                                          tapi_data *ref,
                                          const char *ref_column,
                                          const char *params,
                                          const tapi_data_threshold *thr,
                                          tapi_data_check *res);

/**
 * Possible sensitive data (e-mail, phone, card numbers with a Luhn
 * check, IBAN with mod-97, IPv4, US SSN, JWT, AWS keys, and telling
 * column names). These are pattern matches: report them as possible
 * findings, never as established privacy violations.
 */
extern te_errno tapi_data_check_pii(tapi_data *ds, const char *columns,
                                    const char *params,
                                    const tapi_data_threshold *thr,
                                    tapi_data_check *res);

/**
 * Log the check and, when it failed, emit an ERROR verdict with its
 * summary. The verdict text holds no pointer, path or time, so TRC can
 * match it run after run.
 *
 * @return Whether the check passed.
 */
extern te_bool tapi_data_check_verdict(const tapi_data_check *res);
extern void tapi_data_check_free(tapi_data_check *res);

/* ---- pipelines: generation, conversion, reconciliation ------------ */

/** What tapi_data_generate() wrote. */
typedef struct tapi_data_gen {
    uint64_t        rows;
    uint64_t        written;
    uint64_t        duplicates;
    uint64_t        malformed;
    uint64_t        bytes;
    char            digest[17];
    char            digest_unordered[17];
    char           *schema;         /**< to reopen the file exactly */
    tapi_data_kv    kv;
} tapi_data_gen;

/**
 * Write a deterministic synthetic dataset on the agent.
 *
 * @param spec    One column per line: "name:type:generator[:k=v,...]".
 *                Generators: seq, uniform, normal, bool, string
 *                (pattern=..., len_min/len_max), uuid, time, enum
 *                (values=a|b, weights=3|1), corr (of=col,k,b,noise),
 *                const (value=). Any column: null=p, boundary=p.
 * @param params  rows, seed, dups=p, malformed=p, and writer options.
 */
extern te_errno tapi_data_generate(rcf_rpc_server *rpcs, const char *path,
                                   const char *format, const char *spec,
                                   const char *params, tapi_data_gen *out);
extern void tapi_data_gen_free(tapi_data_gen *g);

/** Content digest: the same rows give the same digest in any format. */
typedef struct tapi_data_digest_info {
    uint64_t    rows;
    char        digest[17];
    char        unordered[17];
} tapi_data_digest_info;

extern te_errno tapi_data_digest(tapi_data *ds, const char *params,
                                 tapi_data_digest_info *out);

/** Write a dataset in another format; result in @p out. */
extern te_errno tapi_data_convert(tapi_data *ds, const char *path,
                                  const char *format, const char *params,
                                  tapi_data_kv *out);

/**
 * Group and aggregate into a new dataset file. params: group_by=a,b
 * and aggs=count:*,sum:x,min:x,max:x,avg:x.
 */
extern te_errno tapi_data_aggregate(tapi_data *ds, const char *path,
                                    const char *format, const char *params,
                                    tapi_data_kv *out);

/** Source-to-target reconciliation. */
typedef struct tapi_data_recon {
    uint64_t        rows_src;
    uint64_t        rows_dst;
    uint64_t        keys_matched;
    uint64_t        keys_lost;      /**< source keys absent from target */
    uint64_t        keys_phantom;   /**< target keys with no source */
    uint64_t        dup_src;
    uint64_t        dup_dst;
    tapi_data_kv    kv;
    te_string       report;
} tapi_data_recon;

#define TAPI_DATA_RECON_INIT { .report = TE_STRING_INIT }

/** params: keys, sums (columns summed on both sides), map (src:dst). */
extern te_errno tapi_data_reconcile(tapi_data *src, tapi_data *dst,
                                    const char *params,
                                    tapi_data_recon *out);
extern void tapi_data_recon_free(tapi_data_recon *r);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_DATA_H__ */
