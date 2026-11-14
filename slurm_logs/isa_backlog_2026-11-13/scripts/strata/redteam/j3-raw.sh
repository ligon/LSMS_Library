#!/bin/bash
#SBATCH -J srt-raw
# Red team, strata stream: raw headers, sweeps and served-vs-raw (candidate only).
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" || exit 1
O=$OUT/j3; mkdir -p $O
echo "start $(date -Is)"
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/raw_check.py $O) > $O/raw.log 2>&1
echo "exit $?"; tail -5 $O/raw.log
echo "end $(date -Is)"
