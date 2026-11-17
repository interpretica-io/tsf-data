# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# External TE library (see TE_EXT_REPO): agent-side dataset validation.
# The engine is plain C over jansson (JSON, JSON Lines), SQLite (query
# results, optional) and POSIX regex, plus one C++ unit over Apache
# Arrow/Parquet C++ (optional) behind a C ABI, the tsf-cv approach. It
# is linked and called in-process in the agent's RPC server: nothing is
# spawned and no data leaves the agent but bounded samples and reports.

headers += files(
    'ta_data.h',
)

sources += files(
    'ta_data.c',
    'ta_data_util.c',
    'ta_data_type.c',
    'ta_data_reader.c',
    'ta_data_ds.c',
    'ta_data_group.c',
    'ta_data_profile.c',
    'ta_data_compare.c',
    'ta_data_check.c',
    'ta_data_ops.c',
)

te_libs += [
    'logger_core',
    'tools',
]

cc = meson.get_compiler('c')

deps += [ dependency('jansson'), cc.find_library('m') ]
deps += [ dependency('threads') ]

data_defs = []

# SQLite: the built-in SQL query source. Optional; without it an
# "sqlite" dataset reports TE_EOPNOTSUPP and the SQL tests skip.
sqlite_dep = dependency('sqlite3', required: false)
if sqlite_dep.found()
    deps += [ sqlite_dep ]
    data_defs += '-DTA_DATA_HAVE_SQLITE'
endif

# Apache Parquet over Arrow C++. Optional; without it the C stub answers
# TE_EOPNOTSUPP for every Parquet call and the Parquet tests skip.
arrow_dep = dependency('arrow', required: false)
parquet_dep = dependency('parquet', required: false)
if arrow_dep.found() and parquet_dep.found()
    add_languages('cpp', required: true)
    # Recent Arrow C++ headers need C++20 (std::span, <bit>), and the TE
    # library target only takes C arguments: build the C++ unit as its
    # own static library with its own standard and fold its objects into
    # libta_data.a.
    ta_data_pq = static_library('ta_data_pq', 'ta_data_parquet.cpp',
                                dependencies: [ arrow_dep, parquet_dep ],
                                override_options: [ 'cpp_std=c++20' ],
                                pic: true)
    deps += declare_dependency(link_whole: ta_data_pq)
    deps += [ arrow_dep, parquet_dep ]
    # The agent application is linked with the C compiler driver, which
    # does not pull in the C++ runtime on its own.
    deps += declare_dependency(link_args: ['-lstdc++'])
else
    sources += files('ta_data_parquet_none.c')
endif

if data_defs.length() > 0
    deps += declare_dependency(compile_args: data_defs)
endif
