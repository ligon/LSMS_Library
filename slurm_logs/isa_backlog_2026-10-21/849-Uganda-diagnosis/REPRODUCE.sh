#!/bin/bash
# Read-only source verification by default. The commands action prints the
# original private command record. Replay only in NEW physical private roots,
# adapting all runner/guard roots together; never overwrite frozen evidence.
set -eu
ROOT=/local/job40255479/worktrees/isa-Uganda-849
test "$(git -C "$ROOT" rev-parse HEAD)" = f8d236694c26fe9e709c5548f02c70f74ea72c33
test -z "$(GIT_OPTIONAL_LOCKS=0 git -C "$ROOT" status --porcelain)"
if [ "${1:-verify}" = verify ]; then
    printf '%s\n' 'Frozen f8 source pin and clean state verified.'
    exit 0
fi
test "$1" = commands
cat <<'COMMANDS'
P=/local/job40255479/control/isa-backlog-20261021/Uganda/849
# Runner asserts exact checkout cwd/import/countries-root/data-root and sets
# private config/DVC/tmp/XDG, threads/build/Make1, socket/write guard+unshare-rn.
# Terminal MD5/SHA/size/mode/mtime/physical-copy gate precedes source decoding:
bash "$P/run.sh" preparation "$P/prepare.py"
bash "$P/run.sh" baseline "$P/baseline.py" cold
bash "$P/run.sh" baseline "$P/baseline.py" warm
bash "$P/run.sh" baseline "$P/default-warm.py"
# Pure helper scenario, deep-copied colmap parameter only; existing country
# helper dictionary lookup runs in this same frozen checkout's Uganda/_ cwd.
bash "$P/run.sh" scenario "$P/oracle.py"
bash "$P/run.sh" baseline "$P/measure.py"
bash "$P/run.sh" scenario "$P/source-counts.py"
bash "$P/run.sh" scenario "$P/collision-audit.py"
bash "$P/run.sh" scenario "$P/source-identity.py"
bash "$P/run.sh" scenario "$P/key-audit.py"
bash "$P/run.sh" baseline "$P/supplement.py"
# Existing pure factor/screen tests; real baseline case separately attempted:
bash "$P/run.sh" tests -m pytest -q -n 2 --dist=loadfile -p no:cacheprovider -o addopts= --basetemp="$P/pytest-private" tests/test_crop_kg_factor.py tests/test_shipped_factors.py -k 'not test_uganda_harvest_kg_baseline'
bash "$P/run.sh" baseline -m pytest -q -n 0 -p no:cacheprovider -o addopts= --basetemp="$P/pytest-baseline-test-private" tests/test_crop_kg_factor.py::test_uganda_harvest_kg_baseline
# Second command genuinely skips at the existing credential collection gate;
# complete cold/warm actual API assertions ran separately above, no bypass.
COMMANDS
