#!/bin/bash
#SBATCH -J rt-benin-raw2
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $CAND || exit 1
O=$OUT/raw2; mkdir -p $O
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_raw2.py $SRC_1 $O > $O/log.txt 2>&1; echo "exit $?" >> $O/log.txt )
echo ALLDONE > $O/ALLDONE
