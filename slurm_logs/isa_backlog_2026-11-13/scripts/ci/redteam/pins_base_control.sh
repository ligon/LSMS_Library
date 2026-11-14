#!/bin/bash
# Red team #1716 (A): re-measure the windows-script-build pins on Linux, cold.
#   base f138a3d61 twice (two separate empty roots), control cbcf85f8f once.
# Then warm-dump per-row hashes (job-local) and compare base vs control by wave.
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
R=$BATCH/reports/ci-redteam
BASE=f138a3d61c203f892148592675a7ef72e160a322
CTRL=cbcf85f8f091d651f620fba18865bd87dbd51613
source $BATCH/tools/burst-env.sh ci-redteam $BASE $BASE $CTRL || exit 9
export PATH="$(dirname "$PY"):$PATH" PYTHONUNBUFFERED=1
unset EXPECT
J=$SLURM_JOB_ID
echo "SRC_1=$SRC_1 SRC_2=$SRC_2 SRC_3=$SRC_3"
run() {  # run <src> <data> <label>
  echo "== $3 at $(git -C "$1" rev-parse HEAD): root $2 holds $(find "$2" -mindepth 1 | wc -l) entries [$(find "$2" -mindepth 1 | head -5 | tr '\n' ' ')]"
  local t0=$SECONDS
  ( cd "$1" && PYTHONPATH=$1 LSMS_DATA_DIR=$2 "$PY" scripts/windows_build_check.py Uganda:shocks Uganda:assets \
      > $JOBLOCAL/build-$3.log 2>&1; echo "exit $?" >> $JOBLOCAL/build-$3.log )
  grep -E '^(RESULT|MISMATCH|RAISED|MAKEVARS|exit )' $JOBLOCAL/build-$3.log | tee $OUT/pins-$3-$J.txt
  echo "   ($((SECONDS - t0)) s)"
}
run "$SRC_1" "$DATA_1" base-r1
run "$SRC_1" "$DATA_2" base-r2
run "$SRC_3" "$DATA_3" ctrl
echo "== warm dumps"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $R/scripts/dump_uganda.py $JOBLOCAL/dump-base ) 2>&1 | grep -E '^WARM|Error|Traceback'
( cd "$SRC_3" && PYTHONPATH=$SRC_3 LSMS_DATA_DIR=$DATA_3 "$PY" $R/scripts/dump_uganda.py $JOBLOCAL/dump-ctrl ) 2>&1 | grep -E '^WARM|Error|Traceback'
echo "== base vs control"
"$PY" -I $R/scripts/compare_uganda.py $JOBLOCAL/dump-base $JOBLOCAL/dump-ctrl
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries $SRC_3/lsms_library/countries -name '*.parquet' | wc -l)"
