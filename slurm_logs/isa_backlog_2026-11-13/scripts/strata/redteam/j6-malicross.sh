#!/bin/bash
#SBATCH -J srt-mcross
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" || exit 1
mkdir -p $OUT/j6
(cd "$SRC_1" && PYTHONPATH=$SRC_1 $PY $RT/scripts/mali_cross.py $OUT/j2/cand/Mali__sample__all.pkl) > $OUT/j6/mali_cross.txt 2>&1
echo "exit $?" >> $OUT/j6/mali_cross.txt
