#!/bin/bash
#SBATCH -J rt-benin-delta
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
C2=2aa1e428736450e5190992019c1b5eaa0b71b5ba
BASE=f138a3d61c203f892148592675a7ef72e160a322
C1=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $C2 $BASE $C1 || exit 1
O=$OUT/delta; mkdir -p $O/cand2 $O/census
M=$OUT/main
cd $SRC_1; export PYTHONPATH=$SRC_1
# census (config only) in parallel with the cold build
( LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census/census-cand2.json > $O/census/c2.log 2>&1 ) &
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census/census-base.json > $O/census/b.log 2>&1 ) &
( LSMS_DATA_DIR=$DATA_3 $PY $RT/scripts/rt_delta.py xw $SRC_1 $SRC_3 $O > $O/xw.log 2>&1; echo "exit $?" >> $O/xw.log ) &
( LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_build.py cold cand2 $O/cand2 > $O/cand2/log-cold.txt 2>&1; echo "exit $?" >> $O/cand2/log-cold.txt
  LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_build.py warm cand2 $O/cand2 > $O/cand2/log-warm.txt 2>&1; echo "exit $?" >> $O/cand2/log-warm.txt ) &
wait
$PY $BATCH/tools/hashdiff.py $O/census/census-base.json $O/census/census-cand2.json $O/census/hashdiff.json > $O/census/hashdiff.txt 2>&1
$PY $RT/scripts/rt_compare.py $M/base $O/cand2 $O/xw2.json $O/compare.json > $O/compare.txt 2>&1
LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_delta.py checks $O $M/base $M/cand $O/cand2 $O/xw2.json > $O/checks.log 2>&1; echo "exit $?" >> $O/checks.log
# regenerate the crosswalk in this job-local clone
( cd $SRC_1/lsms_library/countries/Benin/_ && PATH=$(dirname $PY):$PATH LSMS_DATA_DIR=$DATA_3 make panel-ids > $O/make_panel_ids.txt 2>&1; echo "exit $?" >> $O/make_panel_ids.txt )
{ echo "== git status"; git -C $SRC_1 status --short; find $SRC_1/lsms_library/countries -name '*.parquet'; } > $O/regen_status.txt 2>&1
echo ALLDONE > $O/ALLDONE
