#!/bin/sh
# Run a pipeline on every fixture and compare with expected/.
# Usage: tools/fixtures.sh FIXTURES_DIR CMD...   (CMD gets DATA_DIR OUT_DIR as its last two args and env)
set -u
FX="$1"; shift
HERE=$(cd "$(dirname "$0")" && pwd)
PY="${PY:-python3}"
pass=0; fail=0
for case in $(ls "$FX"); do
  [ -d "$FX/$case/input" ] || continue
  out=$(mktemp -d)
  if DATA_DIR="$FX/$case/input" OUTPUT_DIR="$out" "$@" "$FX/$case/input" "$out" >"$out.log" 2>&1 \
     && "$PY" -I "$HERE/compare.py" "$out" "$FX/$case/expected" -q >"$out.cmp" 2>&1; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); echo "FAIL $case"; tail -20 "$out.log"; cat "$out.cmp" 2>/dev/null
  fi
  rm -rf "$out" "$out.log" "$out.cmp"
done
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
