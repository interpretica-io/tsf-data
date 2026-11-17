/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Dataset RPC server library
 *
 * The data_* RPCs (see data_rpc.x.m4) on top of ta_data. Every RPC has
 * the same shape, so each is one plain C function that names its
 * operation and the TARPC_FUNC_STATIC() wrapper after it, which moves
 * the strings ta_data_rpc() returns straight into the out struct (XDR
 * frees them after sending). The dataset work happens on the agent, in
 * the RPC server process.
 */

#define TE_LGR_USER     "RPC DATA"

#include "te_config.h"

#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "rpc_server.h"

#include "ta_data.h"

/* ---- available ----------------------------------------------------- */

static te_errno
data_available(int ha, int hb, const char *kind, const char *path,
               const char *format, const char *params, const char *spec,
               int *handle, unsigned int *features, char **result,
               char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_AVAILABLE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_available, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- open ---------------------------------------------------------- */

static te_errno
data_open(int ha, int hb, const char *kind, const char *path,
          const char *format, const char *params, const char *spec,
          int *handle, unsigned int *features, char **result,
          char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_OPEN, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_open, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- close --------------------------------------------------------- */

static te_errno
data_close(int ha, int hb, const char *kind, const char *path,
           const char *format, const char *params, const char *spec,
           int *handle, unsigned int *features, char **result,
           char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_CLOSE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_close, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- schema -------------------------------------------------------- */

static te_errno
data_schema(int ha, int hb, const char *kind, const char *path,
            const char *format, const char *params, const char *spec,
            int *handle, unsigned int *features, char **result,
            char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_SCHEMA, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_schema, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- count --------------------------------------------------------- */

static te_errno
data_count(int ha, int hb, const char *kind, const char *path,
           const char *format, const char *params, const char *spec,
           int *handle, unsigned int *features, char **result,
           char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_COUNT, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_count, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- sample -------------------------------------------------------- */

static te_errno
data_sample(int ha, int hb, const char *kind, const char *path,
            const char *format, const char *params, const char *spec,
            int *handle, unsigned int *features, char **result,
            char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_SAMPLE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_sample, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- profile ------------------------------------------------------- */

static te_errno
data_profile(int ha, int hb, const char *kind, const char *path,
             const char *format, const char *params, const char *spec,
             int *handle, unsigned int *features, char **result,
             char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_PROFILE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_profile, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- compare ------------------------------------------------------- */

static te_errno
data_compare(int ha, int hb, const char *kind, const char *path,
             const char *format, const char *params, const char *spec,
             int *handle, unsigned int *features, char **result,
             char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_COMPARE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_compare, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- check --------------------------------------------------------- */

static te_errno
data_check(int ha, int hb, const char *kind, const char *path,
           const char *format, const char *params, const char *spec,
           int *handle, unsigned int *features, char **result,
           char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_CHECK, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_check, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- generate ------------------------------------------------------ */

static te_errno
data_generate(int ha, int hb, const char *kind, const char *path,
              const char *format, const char *params, const char *spec,
              int *handle, unsigned int *features, char **result,
              char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_GENERATE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_generate, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- convert ------------------------------------------------------- */

static te_errno
data_convert(int ha, int hb, const char *kind, const char *path,
             const char *format, const char *params, const char *spec,
             int *handle, unsigned int *features, char **result,
             char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_CONVERT, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_convert, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- aggregate ----------------------------------------------------- */

static te_errno
data_aggregate(int ha, int hb, const char *kind, const char *path,
               const char *format, const char *params, const char *spec,
               int *handle, unsigned int *features, char **result,
               char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_AGGREGATE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_aggregate, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- reconcile ----------------------------------------------------- */

static te_errno
data_reconcile(int ha, int hb, const char *kind, const char *path,
               const char *format, const char *params, const char *spec,
               int *handle, unsigned int *features, char **result,
               char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_RECONCILE, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_reconcile, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- digest -------------------------------------------------------- */

static te_errno
data_digest(int ha, int hb, const char *kind, const char *path,
            const char *format, const char *params, const char *spec,
            int *handle, unsigned int *features, char **result,
            char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_DIGEST, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_digest, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})

/* ---- stats --------------------------------------------------------- */

static te_errno
data_stats(int ha, int hb, const char *kind, const char *path,
           const char *format, const char *params, const char *spec,
           int *handle, unsigned int *features, char **result,
           char **report, char **errmsg)
{
    return ta_data_rpc(TA_DATA_OP_STATS, ha, hb, kind, path, format, params,
                       spec, handle, features, result, report, errmsg);
}

TARPC_FUNC_STATIC(data_stats, {},
{
    int handle = 0;
    unsigned int features = 0;

    MAKE_CALL(out->retval = func(in->ha, in->hb, in->kind, in->path,
                                 in->format, in->params, in->spec, &handle,
                                 &features, &out->result, &out->report,
                                 &out->errmsg));
    out->handle = handle;
    out->features = features;
    out->common.errno_changed = false;
})
