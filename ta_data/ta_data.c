/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset operations: handle registry and TE adapter
 *
 * Opened datasets live here, in the RPC server process, keyed by small
 * integer handles. RPCs are synchronous, but the registry is guarded by
 * a mutex all the same, as several RPC server threads may share the
 * process.
 */

#define TE_LGR_USER     "TA DATA"

#include "te_config.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <unistd.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"

#include "ta_data.h"
#include "ta_data_core.h"

/** At most this many datasets open at once in one RPC server. */
#define TA_DATA_MAX_OPEN 256

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static ta_data_ds *open_ds[TA_DATA_MAX_OPEN + 1];
static unsigned int n_open;
static uint64_t n_ops;

static te_errno
map_rc(int rc)
{
    switch (rc)
    {
        case TA_DATA_OK: return 0;
        case TA_DATA_EINVAL: return TE_RC(TE_TA_UNIX, TE_EINVAL);
        case TA_DATA_ENOENT: return TE_RC(TE_TA_UNIX, TE_ENOENT);
        case TA_DATA_EIO: return TE_RC(TE_TA_UNIX, TE_EIO);
        case TA_DATA_ECORRUPT: return TE_RC(TE_TA_UNIX, TE_ECORRUPTED);
        case TA_DATA_EILSEQ: return TE_RC(TE_TA_UNIX, TE_EILSEQ);
        case TA_DATA_E2BIG: return TE_RC(TE_TA_UNIX, TE_E2BIG);
        case TA_DATA_ENOMEM: return TE_RC(TE_TA_UNIX, TE_ENOMEM);
        case TA_DATA_ENOTSUP: return TE_RC(TE_TA_UNIX, TE_EOPNOTSUPP);
        default: return TE_RC(TE_TA_UNIX, TE_EFAIL);
    }
}

static char *
nn(char *s)
{
    return s != NULL ? s : strdup("");
}

/* Look a handle up; the registry lock must be held. */
static ta_data_ds *
lookup(int h)
{
    if (h < 1 || h > TA_DATA_MAX_OPEN)
        return NULL;
    return open_ds[h];
}

/* Process memory, for the suite's bounded-memory tests. */
static void
stats(char **result)
{
    struct rusage ru;
    long rss_pages = 0;
    long size_pages = 0;
    FILE *f = fopen("/proc/self/statm", "r");
    char buf[256];

    if (f != NULL)
    {
        if (fscanf(f, "%ld %ld", &size_pages, &rss_pages) != 2)
            rss_pages = 0;
        fclose(f);
    }
    getrusage(RUSAGE_SELF, &ru);
    snprintf(buf, sizeof(buf),
             "open=%u\nops=%llu\nrss_kb=%ld\nmaxrss_kb=%ld\nvsz_kb=%ld\n",
             n_open, (unsigned long long)n_ops,
             rss_pages * (sysconf(_SC_PAGESIZE) / 1024), ru.ru_maxrss,
             size_pages * (sysconf(_SC_PAGESIZE) / 1024));
    *result = strdup(buf);
}

/* See description in ta_data.h */
te_errno
ta_data_rpc(ta_data_op op, int ha, int hb, const char *kind, const char *path,
            const char *format, const char *params, const char *spec,
            int *handle, unsigned int *features, char **result,
            char **report, char **errmsg)
{
    ta_data_ds *a = NULL;
    ta_data_ds *b = NULL;
    char *res = NULL;
    char *rep = NULL;
    char *err = NULL;
    int rc = TA_DATA_OK;
    int i;

    pthread_mutex_lock(&lock);
    n_ops++;
    if (op != TA_DATA_OP_AVAILABLE && op != TA_DATA_OP_OPEN &&
        op != TA_DATA_OP_GENERATE && op != TA_DATA_OP_STATS)
    {
        a = lookup(ha);
        if (a == NULL)
        {
            rc = TA_DATA_EINVAL;
            err = strdup("no such dataset handle");
            goto out;
        }
        if (op == TA_DATA_OP_COMPARE || op == TA_DATA_OP_RECONCILE ||
            (op == TA_DATA_OP_CHECK && hb != 0))
        {
            b = lookup(hb);
            if (b == NULL)
            {
                rc = TA_DATA_EINVAL;
                err = strdup("no such second dataset handle");
                goto out;
            }
        }
    }

    switch (op)
    {
        case TA_DATA_OP_AVAILABLE:
            rc = ta_data_features(features, &res);
            break;

        case TA_DATA_OP_OPEN:
        {
            ta_data_ds *ds = NULL;

            for (i = 1; i <= TA_DATA_MAX_OPEN && open_ds[i] != NULL; i++)
                ;
            if (i > TA_DATA_MAX_OPEN)
            {
                rc = TA_DATA_E2BIG;
                err = strdup("too many datasets open");
                break;
            }
            rc = ta_data_ds_open(path, format, params, spec, &ds, &err);
            if (rc != 0)
                break;
            open_ds[i] = ds;
            n_open++;
            *handle = i;
            rc = ta_data_ds_schema(ds, &res);
            break;
        }

        case TA_DATA_OP_CLOSE:
            ta_data_ds_free(a);
            open_ds[ha] = NULL;
            n_open--;
            break;

        case TA_DATA_OP_SCHEMA:
            rc = ta_data_ds_schema(a, &res);
            break;

        case TA_DATA_OP_COUNT:
            rc = ta_data_count(a, params, &res, &err);
            break;

        case TA_DATA_OP_SAMPLE:
            rc = ta_data_sample(a, params, &res, &rep, &err);
            break;

        case TA_DATA_OP_PROFILE:
            rc = ta_data_profile(a, params, &res, &err);
            break;

        case TA_DATA_OP_COMPARE:
            rc = ta_data_compare(a, b, params, &res, &rep, &err);
            break;

        case TA_DATA_OP_CHECK:
            rc = ta_data_check(a, b, kind, params, &res, &rep, &err);
            break;

        case TA_DATA_OP_GENERATE:
            rc = ta_data_generate(path, format, params, spec, &res, &err);
            break;

        case TA_DATA_OP_CONVERT:
            rc = ta_data_convert(a, path, format, params, &res, &err);
            break;

        case TA_DATA_OP_AGGREGATE:
            rc = ta_data_aggregate(a, path, format, params, &res, &err);
            break;

        case TA_DATA_OP_RECONCILE:
            rc = ta_data_reconcile(a, b, params, &res, &rep, &err);
            break;

        case TA_DATA_OP_DIGEST:
            rc = ta_data_digest(a, params, &res, &err);
            break;

        case TA_DATA_OP_STATS:
            stats(&res);
            break;

        default:
            rc = TA_DATA_EINVAL;
            err = strdup("unknown operation");
            break;
    }

out:
    pthread_mutex_unlock(&lock);
    if (rc != 0)
        ERROR("data op %d failed: %s: %s", op, ta_data_strerror(rc),
              err == NULL ? "" : err);
    *result = nn(res);
    *report = nn(rep);
    *errmsg = nn(err);
    return map_rc(rc);
}
