/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief RPC for dataset validation
 *
 * The RPCs of rpcs_data, a thin layer over ta_data (see ta_data.h).
 * Add this file to the rpcxdr definitions of the engine platform and of
 * the agent platform:
 *
 *   TE_LIB_PARMS([rpcxdr], [<platform>], [],
 *                [--with-rpcdefs=tarpc_job.x.m4,../ta_data/data_rpc.x.m4])
 *
 * Every RPC has the same shape: dataset handles, a check kind, a path
 * and format, options and a spec, all as text; back come a return code,
 * a new handle, a feature bitmask and three strings - the result
 * ("key=value" lines), the report (records of tab-separated,
 * percent-escaped fields) and the reason of a failure. XDR has no
 * portable floating-point type; real numbers travel inside the text,
 * printed so that they read back exactly. Datasets stay on the agent:
 * only bounded samples and reports cross the wire.
 *
 * Field names avoid the RPC language's keywords ("version", "program",
 * "string").
 */

/* data_available(): is the dataset engine in this agent; features bitmask (Parquet = 1, SQLite = 2) and library versions in result. */
struct tarpc_data_available_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_available_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_open(): open path (format, params, spec = schema) and keep it on the agent; handle out, schema lines in result. */
struct tarpc_data_open_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_open_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_close(): release the dataset ha. */
struct tarpc_data_close_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_close_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_schema(): the schema of ha: name, type, nullable, source. */
struct tarpc_data_schema_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_schema_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_count(): stream ha: rows, malformed/mismatched records, NULLs per column. */
struct tarpc_data_count_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_count_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_sample(): a bounded sample of ha: JSON Lines in report. */
struct tarpc_data_sample_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_sample_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_profile(): statistics of ha, one record per column in result. */
struct tarpc_data_profile_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_profile_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_compare(): compare ha (expected) with hb (actual); differences in report. */
struct tarpc_data_compare_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_compare_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_check(): quality check kind on ha (hb: reference dataset, or 0). */
struct tarpc_data_check_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_check_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_generate(): write a deterministic synthetic dataset to path from spec. */
struct tarpc_data_generate_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_generate_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_convert(): write ha to path in format. */
struct tarpc_data_convert_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_convert_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_aggregate(): group ha and write the aggregates to path. */
struct tarpc_data_aggregate_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_aggregate_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_reconcile(): source ha against target hb: row counts, key coverage, sums. */
struct tarpc_data_reconcile_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_reconcile_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_digest(): content digest of ha, order-sensitive and order-insensitive. */
struct tarpc_data_digest_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_digest_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

/* data_stats(): the RPC server's open datasets and memory (rss, maxrss). */
struct tarpc_data_stats_in {
    struct tarpc_in_arg common;

    tarpc_int       ha;
    tarpc_int       hb;
    string          kind<>;
    string          path<>;
    string          format<>;
    string          params<>;
    string          spec<>;
};

struct tarpc_data_stats_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_uint      features;
    string          result<>;
    string          report<>;
    string          errmsg<>;
};

program data
{
    version ver0
    {
        RPC_DEF(data_available)
        RPC_DEF(data_open)
        RPC_DEF(data_close)
        RPC_DEF(data_schema)
        RPC_DEF(data_count)
        RPC_DEF(data_sample)
        RPC_DEF(data_profile)
        RPC_DEF(data_compare)
        RPC_DEF(data_check)
        RPC_DEF(data_generate)
        RPC_DEF(data_convert)
        RPC_DEF(data_aggregate)
        RPC_DEF(data_reconcile)
        RPC_DEF(data_digest)
        RPC_DEF(data_stats)
    } = 1;
} = 46;
