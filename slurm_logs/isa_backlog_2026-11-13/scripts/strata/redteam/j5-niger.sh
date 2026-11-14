#!/bin/bash
#SBATCH -J srt-niger
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" || exit 1
mkdir -p $OUT/j5
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/niger1415.py) > $OUT/j5/niger1415.txt 2>&1
echo "exit $?" >> $OUT/j5/niger1415.txt
