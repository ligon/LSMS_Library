#!/bin/bash
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina
source $B/tools/burst-env.sh burkina "$1" "$2" || exit 9
export PATH="$(dirname "$PY"):$PATH"
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $S/scripts/feature_2018.py $OUT/feature2018-cand.json ) &
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $S/scripts/feature_2018.py $OUT/feature2018-base.json ) &
wait; echo done
