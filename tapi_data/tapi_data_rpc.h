/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Dataset TAPI: RPC client layer
 *
 * One call per data_* RPC (see ta_data/data_rpc.x.m4). Every RPC has
 * the same text-in, text-out shape, so they share the argument and
 * result structures below and one entry point that names the RPC.
 * Tests use tapi_data.h; this is the layer beneath it.
 */

#ifndef __TAPI_DATA_RPC_H__
#define __TAPI_DATA_RPC_H__

#include "te_defs.h"
#include "te_errno.h"
#include "te_string.h"
#include "rcf_rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/** The data_* RPCs. */
typedef enum tapi_data_rpc_op {
    TAPI_DATA_RPC_AVAILABLE = 0,
    TAPI_DATA_RPC_OPEN,
    TAPI_DATA_RPC_CLOSE,
    TAPI_DATA_RPC_SCHEMA,
    TAPI_DATA_RPC_COUNT,
    TAPI_DATA_RPC_SAMPLE,
    TAPI_DATA_RPC_PROFILE,
    TAPI_DATA_RPC_COMPARE,
    TAPI_DATA_RPC_CHECK,
    TAPI_DATA_RPC_GENERATE,
    TAPI_DATA_RPC_CONVERT,
    TAPI_DATA_RPC_AGGREGATE,
    TAPI_DATA_RPC_RECONCILE,
    TAPI_DATA_RPC_DIGEST,
    TAPI_DATA_RPC_STATS,
} tapi_data_rpc_op;

/** Arguments of a data_* RPC; NULL strings travel as "". */
typedef struct tapi_data_rpc_args {
    int         ha;
    int         hb;
    const char *kind;
    const char *path;
    const char *format;
    const char *params;
    const char *spec;
} tapi_data_rpc_args;

/** Results of a data_* RPC. Initialise with TAPI_DATA_RPC_RES_INIT. */
typedef struct tapi_data_rpc_res {
    int             handle;
    unsigned int    features;
    te_string       result;
    te_string       report;
    te_string       errmsg;
} tapi_data_rpc_res;

#define TAPI_DATA_RPC_RES_INIT \
    { 0, 0, TE_STRING_INIT, TE_STRING_INIT, TE_STRING_INIT }

/** Free the strings of a result. */
extern void tapi_data_rpc_res_free(tapi_data_rpc_res *res);

/**
 * Call a data_* RPC. The RPC's own failure is returned, not jumped on:
 * a dataset that does not parse is a finding for the test to judge.
 *
 * @param timeout_ms  Timeout of this call, 0 for the TAPI default.
 */
extern te_errno rpc_data_call(rcf_rpc_server *rpcs, tapi_data_rpc_op op,
                              const tapi_data_rpc_args *args,
                              unsigned int timeout_ms,
                              tapi_data_rpc_res *res);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_DATA_RPC_H__ */
