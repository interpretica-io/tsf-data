/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset operations for the RPC server
 *
 * The TE-typed face of the dataset engine. The engine itself is the
 * plain-C core in ta_data_core.h (with the Parquet C++ unit behind it);
 * this layer keeps the registry of opened datasets - an RPC client
 * holds an integer handle, the dataset stays on the agent next to its
 * file - and maps the core's return codes to te_errno.
 *
 * Every operation is "text in, text out": options as "key=value" lines,
 * a result as "key=value" lines, a report as newline-separated records
 * of tab-separated, percent-escaped fields. tapi_data on the engine
 * parses them. Nothing of the data crosses the RPC except bounded
 * samples and reports.
 */

#ifndef __TA_DATA_H__
#define __TA_DATA_H__

#include "te_defs.h"
#include "te_errno.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Operations of ta_data_rpc(). Wire values: the RPCs map onto them. */
typedef enum ta_data_op {
    TA_DATA_OP_AVAILABLE = 0,
    TA_DATA_OP_OPEN,
    TA_DATA_OP_CLOSE,
    TA_DATA_OP_SCHEMA,
    TA_DATA_OP_COUNT,
    TA_DATA_OP_SAMPLE,
    TA_DATA_OP_PROFILE,
    TA_DATA_OP_COMPARE,
    TA_DATA_OP_CHECK,
    TA_DATA_OP_GENERATE,
    TA_DATA_OP_CONVERT,
    TA_DATA_OP_AGGREGATE,
    TA_DATA_OP_RECONCILE,
    TA_DATA_OP_DIGEST,
    TA_DATA_OP_STATS,
} ta_data_op;

/**
 * Run one operation.
 *
 * @param op        What to do.
 * @param ha        First dataset handle (most operations).
 * @param hb        Second dataset handle (compare, reconcile, a check's
 *                  reference), or 0.
 * @param kind      Check kind (check), else "".
 * @param path      File to open / write (open, generate, convert,
 *                  aggregate).
 * @param format    Its format, or "" for the extension.
 * @param params    Options, "key=value" lines.
 * @param spec      Schema (open) or generator spec (generate).
 * @param handle    New handle (open).
 * @param features  Feature bits (available).
 * @param result    Result lines (heap, never NULL; caller frees).
 * @param report    Report records (heap, never NULL; caller frees).
 * @param errmsg    Failure reason (heap, never NULL; caller frees).
 */
extern te_errno ta_data_rpc(ta_data_op op, int ha, int hb, const char *kind,
                            const char *path, const char *format,
                            const char *params, const char *spec, int *handle,
                            unsigned int *features, char **result,
                            char **report, char **errmsg);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TA_DATA_H__ */
