#!/bin/bash
#SBATCH -J mwrt-3
# Red team, malawi: frame compare only (reads job1 frames).
CAND=af81784630ef1ad650a705f4f9c85dde98912797
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND || exit 9
O=$OUT/job3; mkdir -p $O
( cd $SRC_1 && env PYTHONPATH=$SRC_1 $PY $RT/scripts/compare.py $OUT/job1/frames $O/compare.json > $O/compare.out 2>&1; echo "compare rc=$? $(date -Is)" )
echo "JOB DONE $(date -Is)"
