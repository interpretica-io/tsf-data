/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: Parquet seam without Arrow C++
 *
 * Built in place of ta_data_parquet.cpp when Arrow/Parquet C++ is not
 * found: every call reports TA_DATA_ENOTSUP, ta_data_features() leaves
 * the Parquet bit clear and the suite's Parquet tests skip.
 */

#include <stdlib.h>
#include <string.h>

#include "ta_data_core.h"

static int
none(char **err)
{
    if (err != NULL && *err == NULL)
    {
        const char *m = "Parquet (Arrow C++) is not compiled in";

        *err = malloc(strlen(m) + 1);
        if (*err != NULL)
            strcpy(*err, m);
    }
    return TA_DATA_ENOTSUP;
}

int ta_data_pq_supported(void) { return 0; }
const char *ta_data_pq_version(void) { return ""; }

int
ta_data_pq_open(const char *path, int batch_rows, void **h, int *ncols,
                char **err)
{
    (void)path; (void)batch_rows; (void)h; (void)ncols;
    return none(err);
}

int
ta_data_pq_column(void *h, int i, ta_data_pq_col *col)
{
    (void)h; (void)i; (void)col;
    return TA_DATA_ENOTSUP;
}

int
ta_data_pq_num_rows(void *h, int64_t *rows)
{
    (void)h; (void)rows;
    return TA_DATA_ENOTSUP;
}

int
ta_data_pq_next(void *h, ta_data_cell *cells, char **err)
{
    (void)h; (void)cells;
    return none(err);
}

void ta_data_pq_close(void *h) { (void)h; }

int
ta_data_pq_wopen(const char *path, int ncols, const ta_data_pq_col *cols,
                 int64_t group_rows, void **h, char **err)
{
    (void)path; (void)ncols; (void)cols; (void)group_rows; (void)h;
    return none(err);
}

int
ta_data_pq_write(void *h, const ta_data_cell *cells, char **err)
{
    (void)h; (void)cells;
    return none(err);
}

int
ta_data_pq_wclose(void *h, char **err)
{
    (void)h;
    return none(err);
}
