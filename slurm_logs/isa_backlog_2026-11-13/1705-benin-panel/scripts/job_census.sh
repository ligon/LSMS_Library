#!/bin/bash
#SBATCH -J benin-census
# usage: job_census.sh <cand-sha> <base-sha>
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 $2 || exit 1
O=$OUT/census-${1:0:12}; mkdir -p $O
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-cand.json) > $O/log.txt 2>&1
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json) >> $O/log.txt 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json >> $O/log.txt 2>&1
echo "exit $?" >> $O/log.txt
