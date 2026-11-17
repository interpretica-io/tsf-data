/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Dataset TAPI: RPC client layer
 *
 * The rcf_rpc_call() boilerplate behind tapi_data. The RPCs share one
 * shape, so one macro stamps out the call for each; rpc_data_call()
 * picks it. An RPC transport failure is mapped to TE_ECORRUPTED.
 */

#define TE_LGR_USER     "TAPI DATA RPC"

#include "te_config.h"

#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "te_string.h"
#include "logger_api.h"
#include "tapi_rpc_internal.h"
#include "tarpc.h"

#include "tapi_data_rpc.h"

#define CHECK_RPC_ERRNO_UNCHANGED(_func, _var) \
    CHECK_RETVAL_VAR_ERR_COND(_func, _var, false,                    \
                              TE_RC(TE_TAPI, TE_ECORRUPTED), false)

/* See description in tapi_data_rpc.h */
void
tapi_data_rpc_res_free(tapi_data_rpc_res *res)
{
    te_string_free(&res->result);
    te_string_free(&res->report);
    te_string_free(&res->errmsg);
}

static char *
s(const char *v)
{
    return (char *)(v != NULL ? v : "");
}

static void
take(te_string *dst, const char *src)
{
    if (src != NULL && *src != '\0')
        te_string_append(dst, "%s", src);
}

/*
 * One client function per RPC. The in/out structures of the RPCs are
 * distinct types with the same fields, hence a macro rather than one
 * function.
 */
#define DATA_RPC(_name) \
static te_errno                                                         \
call_##_name(rcf_rpc_server *rpcs, const tapi_data_rpc_args *a,         \
             tapi_data_rpc_res *r)                                      \
{                                                                       \
    tarpc_##_name##_in in;                                              \
    tarpc_##_name##_out out;                                            \
                                                                        \
    memset(&in, 0, sizeof(in));                                         \
    memset(&out, 0, sizeof(out));                                       \
    in.ha = a->ha;                                                      \
    in.hb = a->hb;                                                      \
    in.kind = s(a->kind);                                               \
    in.path = s(a->path);                                               \
    in.format = s(a->format);                                           \
    in.params = s(a->params);                                           \
    in.spec = s(a->spec);                                               \
                                                                        \
    rcf_rpc_call(rpcs, #_name, &in, &out);                              \
    CHECK_RPC_ERRNO_UNCHANGED(_name, out.retval);                       \
    TAPI_RPC_LOG(rpcs, _name, "ha=%d hb=%d %s%s%s", "%r handle=%d",     \
                 a->ha, a->hb, s(a->kind), *s(a->kind) != '\0' ? " " : "",\
                 s(a->path), out.retval, out.handle);                   \
    r->handle = out.handle;                                             \
    r->features = out.features;                                         \
    take(&r->result, out.result);                                       \
    take(&r->report, out.report);                                       \
    take(&r->errmsg, out.errmsg);                                       \
                                                                        \
    RETVAL_TE_ERRNO(_name, out.retval);                                 \
}

DATA_RPC(data_available)
DATA_RPC(data_open)
DATA_RPC(data_close)
DATA_RPC(data_schema)
DATA_RPC(data_count)
DATA_RPC(data_sample)
DATA_RPC(data_profile)
DATA_RPC(data_compare)
DATA_RPC(data_check)
DATA_RPC(data_generate)
DATA_RPC(data_convert)
DATA_RPC(data_aggregate)
DATA_RPC(data_reconcile)
DATA_RPC(data_digest)
DATA_RPC(data_stats)

/** Default timeout of a dataset RPC: they stream whole datasets. */
#define TAPI_DATA_RPC_TIMEOUT_MS (30 * 60 * 1000)

/* See description in tapi_data_rpc.h */
te_errno
rpc_data_call(rcf_rpc_server *rpcs, tapi_data_rpc_op op,
              const tapi_data_rpc_args *args, unsigned int timeout_ms,
              tapi_data_rpc_res *res)
{
    rpcs->timeout = timeout_ms != 0 ? timeout_ms : TAPI_DATA_RPC_TIMEOUT_MS;
    /* A dataset that fails to parse is the test's to judge, not a
     * reason to jump to cleanup. */
    RPC_AWAIT_ERROR(rpcs);
    switch (op)
    {
        case TAPI_DATA_RPC_AVAILABLE:
            return call_data_available(rpcs, args, res);
        case TAPI_DATA_RPC_OPEN:
            return call_data_open(rpcs, args, res);
        case TAPI_DATA_RPC_CLOSE:
            return call_data_close(rpcs, args, res);
        case TAPI_DATA_RPC_SCHEMA:
            return call_data_schema(rpcs, args, res);
        case TAPI_DATA_RPC_COUNT:
            return call_data_count(rpcs, args, res);
        case TAPI_DATA_RPC_SAMPLE:
            return call_data_sample(rpcs, args, res);
        case TAPI_DATA_RPC_PROFILE:
            return call_data_profile(rpcs, args, res);
        case TAPI_DATA_RPC_COMPARE:
            return call_data_compare(rpcs, args, res);
        case TAPI_DATA_RPC_CHECK:
            return call_data_check(rpcs, args, res);
        case TAPI_DATA_RPC_GENERATE:
            return call_data_generate(rpcs, args, res);
        case TAPI_DATA_RPC_CONVERT:
            return call_data_convert(rpcs, args, res);
        case TAPI_DATA_RPC_AGGREGATE:
            return call_data_aggregate(rpcs, args, res);
        case TAPI_DATA_RPC_RECONCILE:
            return call_data_reconcile(rpcs, args, res);
        case TAPI_DATA_RPC_DIGEST:
            return call_data_digest(rpcs, args, res);
        case TAPI_DATA_RPC_STATS:
            return call_data_stats(rpcs, args, res);
        default:
            return TE_RC(TE_TAPI, TE_EINVAL);
    }
}
