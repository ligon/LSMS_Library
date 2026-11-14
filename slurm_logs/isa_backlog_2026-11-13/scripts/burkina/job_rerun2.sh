#!/bin/bash
# Rerun the two non-Burkina whole-suite failures on cand and base, fresh private roots.
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $B/tools/burst-env.sh burkina "$1" "$2" || exit 9
export PATH="$(dirname "$PY"):$PATH"
T="tests/test_sample.py::TestSample::test_covers_all_waves tests/test_gh932_safe_ci_failure_locations.py"
for k in 1 2; do s=SRC_$k; d=DATA_$k; lab=$([ $k = 1 ] && echo cand || echo base)
  ( cd ${!s} && PYTHONPATH=${!s} LSMS_DATA_DIR=${!d} $PY -m pytest -q -p no:cacheprovider -rfE $T > $OUT/rerun2-$lab.txt 2>&1 ); echo "$lab exit $?"; tail -1 $OUT/rerun2-$lab.txt
done
