#!/bin/bash
# Red team #1716 (A): run candidate A's private_ci_check.py --public-pins around the
# REAL windows_build_check.py child, cold, exactly as ci.yml's Windows step does
# (Linux stand-in).  Wrapper stdout/stderr is the public surface, so it is saved.
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
A=72885852cfc9ab765759c044c6b23489f94f3d7a
source $BATCH/tools/burst-env.sh ci-redteam $A || exit 9
export PATH="$(dirname "$PY"):$PATH" PYTHONUNBUFFERED=1
J=$SLURM_JOB_ID
NEW="Uganda:shocks=14459:aac42079f006,Uganda:assets=116808:9202ed542a14"
OLD="Uganda:shocks=14457:0ab2ea4c79dd,Uganda:assets=116808:9202ed542a14"
cd "$SRC_1"
wrap() {  # wrap <label> <expect> <stage> [extra wrapper args]
  local lab=$1 exp=$2 stg=$3; shift 3
  echo "== $lab (EXPECT=${exp:-<unset>}, stage=$stg) root holds $(find "$DATA_1" -mindepth 1 | wc -l) entries"
  local t0=$SECONDS
  if [ -n "$exp" ]; then export EXPECT="$exp"; else unset EXPECT; fi
  PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" scripts/private_ci_check.py --stage "$stg" "$@" -- \
      python scripts/windows_build_check.py Uganda:shocks Uganda:assets \
      > $OUT/wrap-$lab-$J.stdout 2> $OUT/wrap-$lab-$J.stderr
  echo "wrapper exit $?  ($((SECONDS - t0)) s)"
  echo "-- stdout:"; cat $OUT/wrap-$lab-$J.stdout
  echo "-- stderr bytes: $(wc -c < $OUT/wrap-$lab-$J.stderr)"
}
wrap new-cold "$NEW" windows-script-build --public-pins
wrap old-warm "$OLD" windows-script-build --public-pins
wrap none-warm "" windows-script-build --public-pins
wrap refused-stage "$NEW" data-tests --public-pins
wrap nopins-warm "$NEW" windows-script-build
echo "== the unwrapped child, warm, RESULT/MISMATCH lines only (cross-check of what the wrapper parsed)"
EXPECT="$OLD" PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" scripts/windows_build_check.py Uganda:shocks Uganda:assets \
    > $JOBLOCAL/child.log 2>&1; echo "child exit $?"
grep -nE '^(RESULT|MISMATCH|RAISED)' $JOBLOCAL/child.log
echo "lines containing RESULT anywhere: $(grep -c RESULT $JOBLOCAL/child.log)"
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
