#!/bin/bash
#SBATCH -J mwrt-5
CAND=af81784630ef1ad650a705f4f9c85dde98912797
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND || exit 9
O=$OUT/job5; mkdir -p $O
( cd $SRC_1 && env PYTHONPATH=$SRC_1 $PY $RT/scripts/last.py $OUT/job1/frames $OUT/job2/oracle/oracle_cowpea_lines.csv > $O/last.out 2>&1; echo "last rc=$?" )
