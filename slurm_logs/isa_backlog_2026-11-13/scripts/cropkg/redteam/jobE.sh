#!/bin/bash
# Red team GH #1559 job E: rerun the Nigeria analysis (fixed script).  sbatch ... jobE.sh CAND NIGERIA
set -uo pipefail
CAND=$1; NG=$2
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg-redteam "$CAND" "$NG" || exit 9
J=$SLURM_JOB_ID; JL=/local/job$J; FR_N=$JL/private/frames-ng; mkdir -p "$FR_N"
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$RT/scripts/rt_build.py" "$FR_N" nigeria "$OUT/E-build-ng-$J.json" Nigeria > /dev/null 2>&1 ); echo "build exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$RT/scripts/rt_nigeria.py" "$FR_N/Nigeria_crop_production.pkl" "$OUT/E-nigeria-$J.json" ); echo "exit $?"
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo done
