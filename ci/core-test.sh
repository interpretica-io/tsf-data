#!/bin/bash
# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# Contract tests of the tsf-data core, without Test Environment: build
# ci/data_cli with the sanitizers (ci/build-core.sh) and check it against
# the facts the companion suite's generator computes with Python's own
# csv/json/statistics/sqlite3 (and pyarrow when installed). Exits non-zero
# on the first broken contract. DATAGEN points at datagen.py (default: a
# tsf-data-ts checkout beside this one).
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-build-ci}"
case "$OUT" in /*) ;; *) OUT="$PWD/$OUT" ;; esac
OUT="$OUT" ./ci/build-core.sh
C="$OUT/data_cli"
DATAGEN="$(cd "$(dirname "${DATAGEN:-../tsf-data-ts/tsf-data-ts/lib/py/datagen.py}")" && pwd)/datagen.py"
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
python3 "$DATAGEN" "$W" >/dev/null
cd "$W"
fact() { grep "^$1=" oracle.txt | cut -d= -f2-; }
val() { grep "^$1=" | head -1 | cut -d= -f2-; }
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok: $*"; }
SCH="$(fact orders.schema)"
export TD_SCHEMA_A="$SCH"

# Every format carries the same rows.
d0=$("$C" digest orders.csv '' | val digest)
for f in orders.json orders.jsonl; do
    [ "$("$C" digest "$f" '' | val digest)" = "$d0" ] || fail "digest $f"
done
[ "$("$C" digest orders.db 'query=SELECT * FROM orders' | val digest)" = "$d0" ] \
    || fail "digest sqlite"
if [ -n "$(fact pyarrow)" ] && "$C" features | grep -q 'arrow='; then
    [ "$(TD_SCHEMA_A= "$C" digest orders.parquet '' | val digest)" = "$d0" ] \
        || fail "digest parquet (pyarrow)"
fi
ok "formats carry the same rows"

# Checks count exactly the planted defects.
chk() { "$C" check "$1" "$2" "$3" ${4:-} | val violations; }
[ "$(chk nulls orders.csv column=email)" = "$(fact orders.null_email)" ] || fail nulls
[ "$(chk unique orders.csv columns=order_id)" = "$(fact orders.dups)" ] || fail unique
[ "$(chk range orders.csv 'column=amount
min=0')" = "$(fact orders.neg_amount)" ] || fail range
[ "$(chk pattern orders.csv 'column=email
pattern=email')" = "$(fact orders.bad_email)" ] || fail pattern
[ "$(TD_SCHEMA_A= chk reference orders.csv column=customer_id customers.csv)" \
    = "$(fact orders.orphans)" ] || fail reference
ok "checks count the planted defects"

# Profile agrees with Python's statistics.
p=$("$C" profile orders.csv 'columns=amount
mode=exact' | grep '^column=amount')
mean=$(echo "$p" | tr '\t' '\n' | val mean)
python3 -c "import sys; a,b=float('$mean'),float('$(fact amount.mean)'); sys.exit(abs(a-b) > 1e-9*abs(b))" \
    || fail "mean $mean"
ok "profile matches the reference"

# Corruption is refused or counted.
"$C" count bad_fields.csv '' >/dev/null 2>&1 && fail "bad_fields accepted"
[ "$(TD_SCHEMA_A= "$C" count bad_fields.csv on_error=count | val malformed)" = 2 ] \
    || fail "bad_fields count"
TD_SCHEMA_A= "$C" count empty.csv '' >/dev/null 2>&1 && fail "empty accepted"
ok "corruption refused or counted"

# Comparison is independent of the partitioning.
export TD_SCHEMA_B="$SCH"
r1=$("$C" compare orders_base.csv orders_b.csv 'mode=key
keys=order_id' | val report_digest)
r2=$("$C" compare orders_base.csv orders_b.csv "mode=key
keys=order_id
mem_limit=16k
tmpdir=$W" | val report_digest)
[ "$r1" = "$r2" ] || fail "report changes with partitioning"
[ -z "$(ls -A "$W" | grep ta_data_spill || true)" ] || fail "spill files left"
ok "comparison reproducible, nothing spilled left"

# Generation is deterministic.
g1=$("$C" generate g1.csv 'id:int:seq;u:string:uuid' 'rows=1000
seed=5' | val digest)
g2=$("$C" generate g2.csv 'id:int:seq;u:string:uuid' 'rows=1000
seed=5' | val digest)
[ "$g1" = "$g2" ] && cmp -s g1.csv g2.csv || fail "generation not deterministic"
ok "generation deterministic"
echo "all core contracts hold"
