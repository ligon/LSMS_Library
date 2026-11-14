#!/bin/bash
#SBATCH -J mqrt-build
# usage: job_build.sh <Country>
set -uo pipefail
C=${1:?country}
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
CAND=fc40960d27b800045984735a38c68cf91c9b2194
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND $BASE || exit 1
O=$OUT/build-$C; mkdir -p $O/cand $O/base
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 3000 $PY $RT/scripts/rt_build.py $O/cand $C ) > $O/cand.log 2>&1 &
P1=$!
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 timeout 3000 $PY $RT/scripts/rt_build.py $O/base $C ) > $O/base.log 2>&1 &
P2=$!
wait $P1; echo "cand build exit $?"; wait $P2; echo "base build exit $?"
tail -2 $O/cand.log $O/base.log
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_compare.py $O/base $O/cand $C $O/compare.json ) > $O/compare.log 2>&1
echo "compare exit $?"; cat $O/compare.log | tail -5
echo "in-tree parquets:"; find $SRC_1/lsms_library/countries $SRC_2/lsms_library/countries -name '*.parquet' | head
