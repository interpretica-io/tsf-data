#!/bin/bash
# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# Build the tsf-data core and ci/data_cli without Test Environment, with
# AddressSanitizer and UndefinedBehaviorSanitizer. Arrow/Parquet C++ is
# used when pkg-config finds it (the real ta_data_parquet.cpp), else the
# ENOTSUP stub; SQLite likewise. Output: ${OUT:-build-ci}/data_cli.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-build-ci}"
SAN="${SAN--fsanitize=address,undefined -fno-omit-frame-pointer}"
mkdir -p "$OUT"
CFLAGS="-std=c11 -D_GNU_SOURCE -D_DEFAULT_SOURCE -O1 -g -Wall -Wextra -Werror \
        -Wno-unused-parameter $SAN $(pkg-config --cflags jansson)"
LIBS="$(pkg-config --libs jansson) -lm"
if pkg-config --exists sqlite3; then
    CFLAGS="$CFLAGS -DTA_DATA_HAVE_SQLITE $(pkg-config --cflags sqlite3)"
    LIBS="$LIBS $(pkg-config --libs sqlite3)"
fi
objs=()
for f in ta_data/ta_data_{util,type,reader,ds,group,profile,compare,check,ops}.c ci/data_cli.c; do
    o="$OUT/$(basename "${f%.c}").o"
    cc $CFLAGS -Ita_data -c "$f" -o "$o"
    objs+=("$o")
done
LINK=cc
if pkg-config --exists arrow parquet; then
    c++ -std=c++20 -O1 -g -Wall -Wextra -Werror -Wno-unused-parameter $SAN \
        $(pkg-config --cflags arrow parquet) -Ita_data \
        -c ta_data/ta_data_parquet.cpp -o "$OUT/ta_data_parquet.o"
    objs+=("$OUT/ta_data_parquet.o")
    LIBS="$LIBS $(pkg-config --libs arrow parquet)"
    LINK=c++
else
    cc $CFLAGS -c ta_data/ta_data_parquet_none.c -o "$OUT/ta_data_parquet_none.o"
    objs+=("$OUT/ta_data_parquet_none.o")
fi
$LINK $SAN "${objs[@]}" $LIBS -o "$OUT/data_cli"
echo "built $OUT/data_cli"
