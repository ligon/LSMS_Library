#!/bin/bash
# Cold base-vs-candidate build of every Burkina table.  Usage: job_measure.sh <cand-sha> <base-sha> <tag>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina
source $B/tools/burst-env.sh burkina "$1" "$2" || exit 9
TAG=${3:-m1}
export PATH="$(dirname "$PY"):$PATH"
FRC=$JOBLOCAL/frames-cand; FRB=$JOBLOCAL/frames-base; mkdir -p $FRC $FRB; chmod 700 $FRC $FRB
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/measure.py cand $FRC $OUT/measure-$TAG-cand.json > $S/logs/measure-$TAG-cand-$SLURM_JOB_ID.log 2>&1 ) &
P1=$!
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/scripts/measure.py base $FRB $OUT/measure-$TAG-base.json > $S/logs/measure-$TAG-base-$SLURM_JOB_ID.log 2>&1 ) &
P2=$!
wait $P1; echo "cand exit $?"; wait $P2; echo "base exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/compare.py $FRB $FRC $OUT/measure-$TAG-base.json $OUT/measure-$TAG-cand.json $OUT/compare-$TAG.json ) > $S/logs/compare-$TAG-$SLURM_JOB_ID.log 2>&1
echo "compare exit $?"
echo "in-tree parquets cand: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l) base: $(find $SRC_2/lsms_library/countries -name '*.parquet' | wc -l)"
