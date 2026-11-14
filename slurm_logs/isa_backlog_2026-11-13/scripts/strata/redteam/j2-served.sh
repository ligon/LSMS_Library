#!/bin/bash
#SBATCH -J srt-served
# Red team, strata stream: served frames cold, base vs candidate, separate
# private roots, both sides in parallel; then compare.
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" "$BASE" || exit 1
O=$OUT/j2; mkdir -p $O/cand $O/base
echo "start $(date -Is)"
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/served_side.py $O/cand > $O/side-cand.log 2>&1) &
(cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $RT/scripts/served_side.py $O/base > $O/side-base.log 2>&1) &
wait
echo "sides done $(date -Is)"
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/served_compare.py $O/cand $O/base $O/compare.json) > $O/compare.txt 2>&1
cat $O/compare.txt
echo "end $(date -Is)"
