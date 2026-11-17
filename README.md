# tsf-data

Dataset validation for Test Environment (TE) suites, packaged as an
external TE repository (consumed with the `TE_EXT_REPO` builder
directive). It reads a dataset where it lives, on a Test Agent, and
profiles it, compares it with another, asserts data-quality rules over
it, generates deterministic synthetic data and reconciles a pipeline's
input with its output. It does not run pipelines; a test triggers them
through the module that drives the system under test. Only bounded
samples and reports reach the engine, so a test can validate a
multi-gigabyte dataset that the engine never holds.

Three libraries:

- `ta_data`: agent side. A plain-C streaming engine (no TE header in
  it, so it builds and tests on its own, see `ci/`) over jansson, SQLite
  and POSIX regex, plus one C++ unit (`ta_data_parquet.cpp`) that reaches
  Apache Arrow/Parquet C++ through a C ABI, the approach of tsf-cv.
  `ta_data.c`, the TE glue, keeps the opened datasets in the RPC
  server, keyed by handle.
- `rpcs_data`: the `data_*` RPCs (RPC program 46). Each takes its
  options as text and returns a result and a report as text.
- `tapi_data`: engine side. `tapi_data.h` is what tests use;
  `tapi_data_rpc.h` is the layer beneath it.

## Sources

| Source | Status | Notes |
|---|---|---|
| CSV / TSV | yes | RFC 4180 quoting, embedded newlines, BOM, `header`, `delimiter`, `null` token |
| JSON | yes | a top-level array of records, split element by element, never loaded whole |
| JSON Lines | yes | |
| Apache Parquet | yes, with Arrow C++ | every integer width, float16/32/64, decimal128/256, dictionary, date32/64, timestamp of any unit, list, struct, map |
| SQLite query results | yes, with libsqlite3 | `query=` or `table=`; types from the declared column types |
| HTTP API, MQTT | through tsf-http, tsf-mqtt | those modules put the payload in a file on the agent; tsf-data opens the file |
| PostgreSQL, MariaDB | through tsf-sql | no direct adapter yet: export the result on the agent, then open it |
| Arrow IPC, S3 | planned | |

Every source maps onto one logical model: bool, int (64-bit), float
(double), decimal(p,s) with p <= 18, string, binary, date, timestamp
(microseconds, UTC), array<T> and object{name:T[!],...}. Nested values
travel as canonical JSON. A Parquet decimal wider than 18 digits is read
as its exact text.

## Operations

| Call | What it answers |
|---|---|
| `tapi_data_open` / `tapi_data_close` | Open a dataset on the agent with an explicit or inferred schema |
| `tapi_data_schema` | Column names, types, nullability, whether inferred |
| `tapi_data_count` | Rows, malformed records, values that do not fit, coercions, NULLs per column |
| `tapi_data_sample` | A bounded sample (head or reservoir), as JSON Lines |
| `tapi_data_profile` | Per column: count, NULLs, distinct, min/max, mean, standard deviations, exact sum, quantiles, top values, string lengths, time ranges |
| `tapi_data_compare` | Two datasets: exact, unordered, key-based, schema-only or distribution comparison |
| `tapi_data_check_*` | schema, nulls, unique, range, pattern, freshness, distribution, reference, pii |
| `tapi_data_generate` | A deterministic synthetic dataset from a spec and a seed |
| `tapi_data_convert` | The same rows in another format |
| `tapi_data_aggregate` | GROUP BY with count/sum/min/max/avg, written as a dataset |
| `tapi_data_reconcile` | Source against target: row counts, lost and phantom keys, duplicates, sums |
| `tapi_data_digest` | A content digest of the rows, the same in every format |

```c
tapi_data *orders = NULL;
tapi_data *customers = NULL;
tapi_data_check chk = TAPI_DATA_CHECK_INIT;

if (!tapi_data_available(pco))
    TEST_SKIP("The agent has no dataset engine");
CHECK_RC(tapi_data_open(pco, "/data/orders.parquet", NULL, NULL, NULL,
                        &orders));
CHECK_RC(tapi_data_open(pco, "/data/customers.csv", NULL, NULL, NULL,
                        &customers));

/* No order may reference an unknown customer. */
CHECK_RC(tapi_data_check_reference(orders, "customer_id", customers, NULL,
                                   NULL, &TAPI_DATA_NONE_ALLOWED, &chk));
if (!tapi_data_check_verdict(&chk))
    TEST_STOP;
```

Options are one string of `key=value` lines (`;` also separates them in
the TAPI, as a shorthand). A dataset's own options (given at open) apply
to every operation on it; an operation's options override them for that
call.

## Checks and verdicts

Each check returns what it counted and a bounded, sorted sample of the
offending records. The TAPI then decides pass or fail against a
`tapi_data_threshold`: the violations allowed, the fraction allowed, or
the largest statistic allowed for distribution checks.
`tapi_data_check_verdict()` logs the result and, on failure, emits an
ERROR verdict. Its text holds counts and thresholds and no path or
time, so TRC matches it run after run.

- **schema**: required and unexpected columns, types, nullability,
  nested structures, then every value. `mode=compatible` accepts
  backward-compatible evolution (added nullable columns, widened types)
  and reports it as information; `coerce=1` converts values that only fit
  by coercion and reports how many.
- **nulls**, **unique** (one or more columns), **range** (in the
  column's type: numbers, dates, timestamps, strings), **pattern** (POSIX
  extended, full match; `@email`, `@uuid`, `@ipv4`... built in).
- **freshness**: age of the latest event against `now` (the agent's
  clock by default), ingestion delay (`ingest=` column, event and
  ingestion time kept apart), gaps of more than `interval_us`,
  out-of-order events, duplicate event ids, missing sequence numbers.
  `skew_us` is the clock-synchronisation allowance applied to all of
  them.
- **distribution**: against expected shares (TVD, chi-square) or against
  the same column of a reference dataset (KS for numbers, TVD otherwise).
- **reference**: values with no match in a reference dataset.
- **pii**: possible sensitive data: e-mail, phone, card numbers (Luhn
  checked), IBAN (mod-97 checked), IPv4, US SSN, JWT, AWS keys, and
  column names that suggest them. These are pattern matches and are
  reported as possible findings, never as established privacy
  violations.

## Exact and approximate

Profiling is exact while its structures fit `mem_limit` (default
256 MiB per operation). A structure that does not fit is replaced by its
approximate counterpart and the result says so: `distinct_algo` is
`exact-hash64` or `hll-p14` (standard error 0.81%), `q_algo` is
`exact-linear` or `histogram-4096` (error at most one bin, reported in
`q_err`), `top_algo` is `exact` or `misra-gries-<k>` (undercount bound in
`top_err`). `mode=exact` refuses with `TE_E2BIG` instead of
approximating; `mode=approx` never tries to be exact.

## Streaming and memory

Every operation streams. Operations that must match records across a
whole dataset (unordered and key-based comparison, uniqueness,
references, aggregation, duplicate ids) hash-partition the records into
spill files on disk and load one partition at a time; a partition that
does not fit is split again. The spill files are created with
`mkstemp()` (mode 0600) in `tmpdir` and unlinked at once, so nothing is
left behind even if the RPC server is killed mid-operation. `mem_limit`
bounds the working set of an operation (hash tables, sketches,
partitions); fixed I/O buffers and Arrow's row-group buffers come on
top. Reports are bounded (`max_report`) and sorted by (kind, row), and
`report_digest` covers every difference found, so a comparison reports
the same thing whatever the partitioning.

## Errors

Every failure comes back as a `te_errno` and a reason that names the
record and the column where there is one (`tapi_data_last_error()`):
`TE_EILSEQ` for a malformed record or a value that does not fit its
column (unless `on_error=count`, which counts them and goes on),
`TE_ECORRUPTED` for input that cannot be a dataset (a truncated Parquet
file, an empty CSV), `TE_ENOENT`, `TE_E2BIG` (memory limit),
`TE_EOPNOTSUPP` (a feature not compiled in).

## Synthetic data

`tapi_data_generate()` writes rows from a spec, one column per line
(`name:type:generator[:k=v,...]`): `seq`, `uniform`, `normal`, `bool`,
`string` (`pattern=[A-Z]{3}-\d{4}`), `uuid`, `time`, `enum`
(`values=a|b`, `weights=3|1`), `corr` (`of=x,k=2,b=1,noise=0.1`),
`const`; any column takes `null=p` and `boundary=p` (INT64 extremes,
empty and long strings, leap days...). Dataset-wide, `dups=p` repeats
earlier rows and `malformed=p` writes broken records (CSV, JSON, JSON
Lines). The same spec and seed give the same file byte for byte; each
column has its own random stream, so adding a column leaves the others
unchanged.

## Building

The agent needs libjansson (required) and, optionally, libsqlite3 and
Arrow/Parquet C++ with a C++20 compiler; without them the SQL and Parquet
calls return `TE_EOPNOTSUPP` and `tapi_data_supports()` says so. In a
suite's `builder.conf`:

```
TE_EXT_REPO_USE([tsf_data], [], [ta_data rpcs_data tapi_data])
TE_LIB_PARMS([rpcxdr], [], [],
             [--with-rpcdefs=tarpc_job.x.m4,../ta_data/data_rpc.x.m4])
TE_TA_APP([ta_rpcprovider], [], [linux], [ta_rpcprovider], [], [],
          [ta_data rpcs_data ...stock RPC server libraries...],
          [\${EXT_SOURCES}/build.sh --extra-deps='libpcre2-8 jansson sqlite3 arrow parquet stdcxx'],
          [ta_rpcs], [])
```

`stdcxx` is a one-line pkg-config file for `-lstdc++` (the agent
application is linked with the C driver). tsf-data-ts is the companion
suite and a working example.

## Tests

`ci/core-test.sh` builds the core and `ci/data_cli` (a shell driver of
the core) with AddressSanitizer and UndefinedBehaviorSanitizer and checks
it against facts computed by Python's standard library over the suite's
fixtures. tsf-data-ts is the full suite.

## License

MIT, see LICENSE.
