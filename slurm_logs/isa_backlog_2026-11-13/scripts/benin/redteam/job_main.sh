#!/bin/bash
#SBATCH -J rt-benin-main
# Cold base vs candidate (separate private empty roots), fresh-process warm reads,
# raw-file checks, comparison, and the candidate read over a base-built root.
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh benin-redteam $CAND $BASE || exit 1
O=$OUT/main; mkdir -p $O/cand $O/base $O/raw $O/cand_on_baseroot
JL=/local/job$SLURM_JOB_ID
DATA_R=$JL/private/data-raw; mkdir -p $DATA_R
echo "roots: cand $DATA_1 ($(ls -A $DATA_1 | wc -l) entries) base $DATA_2 ($(ls -A $DATA_2 | wc -l) entries)" > $O/roots.txt
ident "$SRC_1" "$DATA_R" >> $O/roots.txt 2>&1

( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_R $PY $RT/scripts/rt_raw.py $SRC_1 $O/raw > $O/raw/log.txt 2>&1; echo "exit $?" >> $O/raw/log.txt ) &
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_build.py cold cand $O/cand > $O/cand/log-cold.txt 2>&1; echo "exit $?" >> $O/cand/log-cold.txt
  PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_build.py warm cand $O/cand > $O/cand/log-warm.txt 2>&1; echo "exit $?" >> $O/cand/log-warm.txt ) &
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $RT/scripts/rt_build.py cold base $O/base > $O/base/log-cold.txt 2>&1; echo "exit $?" >> $O/base/log-cold.txt
  PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $RT/scripts/rt_build.py warm base $O/base > $O/base/log-warm.txt 2>&1; echo "exit $?" >> $O/base/log-warm.txt ) &
wait

# compare base (mapped through MY crosswalk, derived from the raw covers) with the candidate
( cd $SRC_1 && $PY $RT/scripts/rt_compare.py $O/base $O/cand $O/raw/derived_crosswalk.json $O/compare.json > $O/compare.txt 2>&1 )

# candidate code over a COPY of the base-built root: must equal the candidate cold build
DATA_X=$JL/private/data-baseroot; cp -a $DATA_2 $DATA_X
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_X $PY $RT/scripts/rt_build.py cold cand_on_baseroot $O/cand_on_baseroot > $O/cand_on_baseroot/log.txt 2>&1; echo "exit $?" >> $O/cand_on_baseroot/log.txt )
echo '{"2018-19": {}, "2021-22": {}}' > $O/empty_xw.json
( cd $SRC_1 && $PY $RT/scripts/rt_compare.py $O/cand $O/cand_on_baseroot $O/empty_xw.json $O/compare_cand_on_baseroot.json > $O/compare_cand_on_baseroot.txt 2>&1 )

for s in 1 2; do v=SRC_$s; ( cd ${!v} && git status --short > $O/git_status_$s.txt; find lsms_library/countries -name '*.parquet' > $O/intree_parquets_$s.txt ); done
echo ALLDONE > $O/ALLDONE
