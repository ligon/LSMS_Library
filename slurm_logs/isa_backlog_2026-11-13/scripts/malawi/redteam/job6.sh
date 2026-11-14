#!/bin/bash
#SBATCH -J mwrt-6
CAND=af81784630ef1ad650a705f4f9c85dde98912797
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND || exit 9
O=$OUT/job6; mkdir -p $O
( cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/season2010.py > $O/season2010.out 2>&1; echo "s2010 rc=$?" )
