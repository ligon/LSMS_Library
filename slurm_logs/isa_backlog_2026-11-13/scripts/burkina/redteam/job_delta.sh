#!/bin/bash
# Delta red team: base vs new candidate, cold; hash census; strict grain; targeted tests.
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
source $B/tools/burst-env.sh burkina-redteam "$1" "$2" "$1" || exit 9
export PATH="$(dirname "$PY"):$PATH"
J=$SLURM_JOB_ID
FRC=$JOBLOCAL/d-cand; FRB=$JOBLOCAL/d-base; mkdir -p $FRC $FRB; chmod 700 $FRC $FRB
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/delta.py cand $FRC $OUT/delta-cand.json > $S/logs/delta-cand-$J.log 2>&1 ) &
P1=$!
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/scripts/delta.py base $FRB $OUT/delta-base.json > $S/logs/delta-base-$J.log 2>&1 ) &
P2=$!
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_3 "$PY" $B/tools/hash_census.py $OUT/census-cand2.json > $S/logs/hash-cand2-$J.log 2>&1
  cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_3 "$PY" $B/tools/hash_census.py $OUT/census-base2.json > $S/logs/hash-base2-$J.log 2>&1
  "$PY" $B/tools/hashdiff.py $OUT/census-base2.json $OUT/census-cand2.json $OUT/hashdiff2.json > $S/logs/hashdiff2-$J.log 2>&1 ) &
P3=$!
wait $P1; echo "cand $?"; wait $P2; echo "base $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/delta_cmp.py $FRB $FRC $OUT/delta-cand.json $OUT/delta-cmp.json ) > $S/logs/delta-cmp-$J.log 2>&1
echo "cmp $?"
for arm in cand base; do
  if [ $arm = cand ]; then SRC=$SRC_1; DATA=$DATA_1; FR=$FRC; else SRC=$SRC_2; DATA=$DATA_2; FR=$FRB; fi
  ( cd "$SRC" && PYTHONPATH=$SRC LSMS_DATA_DIR=$DATA LSMS_GRAIN_STRICT=1 "$PY" $S/scripts/strict.py grain $FR $OUT/strict2-grain-$arm.json ) > $S/logs/strict2-grain-$arm-$J.log 2>&1
done
cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n 6 --dist=loadfile -q -p no:cacheprovider -rfEs \
  tests/test_burkina_faso_2021_crop_production.py tests/test_burkina_faso_2014_passages.py \
  tests/test_gh824_burkina_unit_sold.py tests/test_derivations.py tests/test_schema_consistency.py \
  tests/test_build_transform_hash.py tests/test_gh824_mali_ehcvm_crop_production.py \
  tests/test_gh1442_burkina_section_c_values.py tests/test_feature_canonical_index.py > $OUT/pytest-delta.txt 2>&1
echo "pytest $?"; tail -3 $OUT/pytest-delta.txt
wait $P3; echo "hash $?"
echo "in-tree parquets cand: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
