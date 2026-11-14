#!/bin/bash
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
source $B/tools/burst-env.sh burkina-redteam "$1" "$2" || exit 9
J=$SLURM_JOB_ID
FRC=$JOBLOCAL/f-cand; FRB=$JOBLOCAL/f-base; mkdir -p $FRC $FRB
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/fu.py build cand $FRC $OUT/fu-cand.json > $S/logs/fu-cand-$J.log 2>&1 ) &
P1=$!
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/scripts/fu.py build base $FRB $OUT/fu-base.json > $S/logs/fu-base-$J.log 2>&1 ) &
P2=$!
wait $P1; echo "cand $?"; wait $P2; echo "base $?"
cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" $S/scripts/fu.py cmp $FRB $FRC $OUT/fu-cmp.json > $S/logs/fu-cmp-$J.log 2>&1
echo "cmp $?"
