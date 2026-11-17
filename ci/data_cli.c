/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/*
 * data_cli: drive the tsf-data core (ta_data_core.h) from a shell, with
 * no Test Environment around it. The module's CI builds it with the
 * sanitizers and runs ci/core-test.sh over it; it is also handy to try
 * an option by hand. Not part of the TE build.
 *
 *   data_cli schema|count|profile|sample|digest A PARAMS
 *   data_cli compare A B PARAMS
 *   data_cli check KIND A PARAMS [REF]
 *   data_cli generate OUT SPEC PARAMS
 *   data_cli convert|aggregate A OUT PARAMS
 *   data_cli reconcile A B PARAMS
 *   data_cli features
 *
 * PARAMS is "k=v;k=v" (';' becomes a newline). A dataset is opened with
 * the same PARAMS; TD_SCHEMA_A / TD_SCHEMA_B give explicit schemas,
 * TD_FORMAT_A / TD_FORMAT_B explicit formats. The result is printed,
 * then the report after a "--" line. Exit status: the core's code.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_data_core.h"

static char *
nl(const char *s)
{
    char *p = strdup(s == NULL ? "" : s);
    char *q;

    for (q = p; *q != '\0'; q++)
    {
        if (*q == ';')
            *q = '\n';
    }
    return p;
}

static int
open_ds(const char *path, const char *params, int which, ta_data_ds **ds)
{
    char *err = NULL;
    int rc = ta_data_ds_open(path,
                             getenv(which == 0 ? "TD_FORMAT_A" : "TD_FORMAT_B"),
                             params,
                             getenv(which == 0 ? "TD_SCHEMA_A" : "TD_SCHEMA_B"),
                             ds, &err);

    if (rc != 0)
        fprintf(stderr, "open %s: %s (%s)\n", path, ta_data_strerror(rc),
                err == NULL ? "" : err);
    free(err);
    return rc;
}

static int
done(int rc, char *res, char *rep, char *err)
{
    if (rc == 0)
    {
        fputs(res == NULL ? "" : res, stdout);
        if (rep != NULL)
        {
            fputs("--\n", stdout);
            fputs(rep, stdout);
        }
    }
    else
        fprintf(stderr, "error: %s: %s\n", ta_data_strerror(rc),
                err == NULL ? "" : err);
    free(res);
    free(rep);
    free(err);
    return rc;
}

int
main(int argc, char **argv)
{
    const char *op = argc > 1 ? argv[1] : "";
    ta_data_ds *a = NULL;
    ta_data_ds *b = NULL;
    char *res = NULL;
    char *rep = NULL;
    char *err = NULL;
    char *params;
    int rc;

    if (strcmp(op, "features") == 0)
    {
        unsigned int f = 0;

        ta_data_features(&f, &res);
        printf("features=%u\nversions=%s\n", f, res);
        free(res);
        return 0;
    }
    if (argc < 4)
    {
        fprintf(stderr, "usage: see the header of ci/data_cli.c\n");
        return 2;
    }
    if (strcmp(op, "generate") == 0)
    {
        char *spec = strdup(argv[3]);
        char *q;

        for (q = spec; *q != '\0'; q++)
        {
            if (*q == ';')
                *q = '\n';
        }
        params = nl(argc > 4 ? argv[4] : "");
        rc = ta_data_generate(argv[2], getenv("TD_FORMAT_OUT"), params, spec,
                              &res, &err);
        free(spec);
        free(params);
        return done(rc, res, NULL, err);
    }
    if (strcmp(op, "check") == 0)
    {
        params = nl(argv[4]);
        rc = open_ds(argv[3], params, 0, &a);
        if (rc == 0 && argc > 5)
            rc = open_ds(argv[5], "", 1, &b);
        if (rc == 0)
            rc = ta_data_check(a, b, argv[2], params, &res, &rep, &err);
        ta_data_ds_free(a);
        ta_data_ds_free(b);
        free(params);
        return done(rc, res, rep, err);
    }
    if (strcmp(op, "compare") == 0 || strcmp(op, "reconcile") == 0 ||
        strcmp(op, "convert") == 0 || strcmp(op, "aggregate") == 0)
    {
        params = nl(argc > 4 ? argv[4] : "");
        rc = open_ds(argv[2], params, 0, &a);
        if (rc == 0 && (op[0] == 'c' && op[2] == 'm'))
            rc = open_ds(argv[3], params, 1, &b);
        else if (rc == 0 && op[0] == 'r')
            rc = open_ds(argv[3], params, 1, &b);
        if (rc == 0)
        {
            if (strcmp(op, "compare") == 0)
                rc = ta_data_compare(a, b, params, &res, &rep, &err);
            else if (strcmp(op, "reconcile") == 0)
                rc = ta_data_reconcile(a, b, params, &res, &rep, &err);
            else if (strcmp(op, "convert") == 0)
                rc = ta_data_convert(a, argv[3], getenv("TD_FORMAT_OUT"),
                                     params, &res, &err);
            else
                rc = ta_data_aggregate(a, argv[3], getenv("TD_FORMAT_OUT"),
                                       params, &res, &err);
        }
        ta_data_ds_free(a);
        ta_data_ds_free(b);
        free(params);
        return done(rc, res, rep, err);
    }
    params = nl(argv[3]);
    rc = open_ds(argv[2], params, 0, &a);
    if (rc == 0)
    {
        if (strcmp(op, "schema") == 0)
            rc = ta_data_ds_schema(a, &res);
        else if (strcmp(op, "count") == 0)
            rc = ta_data_count(a, params, &res, &err);
        else if (strcmp(op, "profile") == 0)
            rc = ta_data_profile(a, params, &res, &err);
        else if (strcmp(op, "sample") == 0)
            rc = ta_data_sample(a, params, &res, &rep, &err);
        else if (strcmp(op, "digest") == 0)
            rc = ta_data_digest(a, params, &res, &err);
        else
        {
            fprintf(stderr, "unknown operation '%s'\n", op);
            rc = 2;
        }
    }
    ta_data_ds_free(a);
    free(params);
    return done(rc, res, rep, err);
}
