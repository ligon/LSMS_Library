#!/bin/bash
#SBATCH -J benin-build
# usage: job_build.sh <label> <sha>   -- cold build of every Benin table in a private empty root
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
LABEL=$1; SHA=$2
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
O=$OUT/build-$LABEL-${SHA:0:12}; mkdir -p $O
cd "$SRC_1"
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
echo "data root $DATA_1; empty: $(ls -A $DATA_1 | wc -l)" > $O/log-cold.txt
"$PY" $BATCH/reports/benin/scripts/build_side.py $LABEL $O cold >> $O/log-cold.txt 2>&1
echo "exit $?" >> $O/log-cold.txt
"$PY" $BATCH/reports/benin/scripts/build_side.py $LABEL $O warm > $O/log-warm.txt 2>&1
echo "exit $?" >> $O/log-warm.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt
git status --short > $O/git_status.txt
