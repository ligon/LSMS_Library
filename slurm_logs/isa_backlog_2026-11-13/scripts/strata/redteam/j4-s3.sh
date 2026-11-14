#!/bin/bash
#SBATCH -J srt-s3
# Red team, strata stream: Mali 2014-15 call-order experiment (both sides, fresh
# roots), then the requires_s3 tests of the touched test files on the candidate.
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" "$BASE" || exit 1
O=$OUT/j4; mkdir -p $O
mkdir -p $DATA_1-order $DATA_2-order
echo "start $(date -Is)"
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1-order $PY $RT/scripts/mali_order.py) > $O/mali-order-cand.txt 2>&1 &
(cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2-order $PY $RT/scripts/mali_order.py) > $O/mali-order-base.txt 2>&1 &
wait
echo "order done $(date -Is)"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 6000 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider \
  -q -rfEs -m requires_s3 tests/test_sampling.py tests/test_gh1494_mali_eaci_namespace.py tests/test_gh812_ehcvm_strata.py \
  > $O/pytest-s3-cand.txt 2>&1
echo "cand s3 exit $?" >> $O/pytest-s3-cand.txt
grep -E '^(FAILED|ERROR) ' $O/pytest-s3-cand.txt | sed -E 's/^(FAILED|ERROR) //; s/ - .*//' | sort -u > $O/failed-s3-cand.txt
if [ -s $O/failed-s3-cand.txt ]; then
  cd "$SRC_2"
  PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 timeout 3000 $PY -m pytest -p no:cacheprovider -q -rfE \
     $(cat $O/failed-s3-cand.txt | tr '\n' ' ') > $O/pytest-s3-failed-on-base.txt 2>&1
  echo "base rerun exit $?" >> $O/pytest-s3-failed-on-base.txt
fi
echo "end $(date -Is)"
