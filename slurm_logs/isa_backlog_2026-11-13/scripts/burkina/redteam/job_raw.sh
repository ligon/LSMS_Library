#!/bin/bash
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
source $B/tools/burst-env.sh burkina-redteam "$1" || exit 9
cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/raw_census.py $OUT/raw_census2.json > $S/logs/raw-census2-$SLURM_JOB_ID.log 2>&1
echo "exit $?"
