#!/bin/bash
#SBATCH -J rt-benin-compare2
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $CAND || exit 1
M=$OUT/main
cd $SRC_1; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
$PY $RT/scripts/rt_routes_compare.py $OUT $M/raw/derived_crosswalk.json $OUT/routes_compare.json > $OUT/routes_compare.txt 2>&1
echo "exit $?" >> $OUT/routes_compare.txt
echo ALLDONE > $OUT/COMPARE2_DONE
