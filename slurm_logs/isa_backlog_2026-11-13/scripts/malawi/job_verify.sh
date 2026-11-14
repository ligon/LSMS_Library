#!/bin/bash
#SBATCH -J mw-verify
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --mem=96G
#SBATCH -t 4:00:00
#SBATCH --exclude=n0035.savio4,n0034.savio4,n0046.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
# Cold base-vs-candidate builds in separate private roots, compare, sensitivity,
# tests on both arms, hash census.  usage: sbatch --export=ALL,CAND=<sha>,TAG=<name> job_verify.sh
CAND=${CAND:?}; TAG=${TAG:?}
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
REP=$BATCH/reports/malawi
source $BATCH/tools/burst-env.sh malawi $CAND $BASE || exit 9
V=$OUT/verify-$TAG; FR=$V/frames; mkdir -p $FR
echo "cand $SRC_1 $DATA_1 | base $SRC_2 $DATA_2 | $(date -Is)"
arm() { local which=$1; shift; if [ $which = cand ]; then cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; else cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$@"; fi; }

# 0. fast synthetic gate on the candidate (no data)
arm cand $PY -m pytest -q -p no:cacheprovider -m "not requires_s3" tests/test_gh1228_malawi_cowpea_label.py tests/test_malawi_sale_basis.py tests/test_gh1190_selected_farm_carrier.py tests/test_derivations.py tests/test_1111_malawi_applied_kg.py > $V/pytest-gate.txt 2>&1
GATE=$?; echo "gate rc=$GATE"; tail -3 $V/pytest-gate.txt
[ $GATE -eq 0 ] || { echo "GATE FAILED, stopping"; exit 1; }

# 1. cold builds, both arms in parallel
( arm cand $PY $REP/scripts/build.py $SRC_1 cand $FR > $FR/cand.build.log 2>&1; echo "build cand rc=$?" ) &
( arm base $PY $REP/scripts/build.py $SRC_2 base $FR > $FR/base.build.log 2>&1; echo "build base rc=$?" ) &
wait
echo "builds done $(date -Is)"

# 2. compare, 3. sensitivity
arm cand $PY $REP/scripts/compare.py $FR base cand $V/compare.json > $V/compare.log 2>&1; echo "compare rc=$?"; cat $V/compare.log | cut -c1-400
arm cand $PY $REP/scripts/sens.py $SRC_1 $FR cand $V/sens.json > $V/sens.log 2>&1; echo "sens rc=$?"; tail -3 $V/sens.log
arm cand $PY $REP/scripts/diag4.py $SRC_1 $V/diag4 > $V/diag4.log 2>&1; echo "diag4 rc=$?"; grep -E "DIAG4|Error" $V/diag4.log | tail -3

# 4. tests, both arms on their warm roots
FILES="$(cd $SRC_1 && ls tests/test_*malawi*.py | tr '\n' ' ') tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_867_nitrogen_content.py tests/test_863_transforms.py tests/test_crop_kg_factor.py tests/test_feature_canonical_index.py tests/test_gh1460_unknown_plot.py tests/test_gh1475_country_crop_transforms.py tests/test_gh1190_selected_farm_carrier.py tests/test_quantity_screen.py tests/test_shipped_factors.py tests/test_null_index_guard.py tests/test_categorical_mapping_resolution.py tests/test_no_build_input_in_contents_org.py tests/test_coverage_matrix.py tests/test_1234_map_index_unit_sentinel.py tests/test_food_kg_inference.py tests/test_962_unit_sentinel.py tests/test_gh1482_1485_quantity_diagnostics.py tests/test_gh1205_uganda_parcel_prefix.py tests/test_gh1493_near_zero_screen.py tests/test_niger_npk_grade.py tests/test_gh637_tanzania_unit_grain.py tests/test_niger_crop_production_condition.py tests/test_use_answer_convention.py tests/test_uganda_sold_unit.py"
echo "$FILES" | tr ' ' '\n' | grep . > $V/pytest_files.txt
( arm cand $PY -m pytest -n 7 --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $V/pytest-cand.txt 2>&1; echo "pytest cand rc=$?" ) &
( arm base $PY -m pytest -n 7 --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $V/pytest-base.txt 2>&1; echo "pytest base rc=$?" ) &
wait
tail -2 $V/pytest-cand.txt $V/pytest-base.txt
( arm cand $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $V/pytest-table-structure-cand.txt 2>&1; echo "ts cand rc=$?" ) &
( arm base $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $V/pytest-table-structure-base.txt 2>&1; echo "ts base rc=$?" ) &
wait
tail -2 $V/pytest-table-structure-cand.txt $V/pytest-table-structure-base.txt

# 5. hash census
arm cand $PY $BATCH/tools/hash_census.py $V/census-cand.json > $V/census-cand.log 2>&1; echo "census cand rc=$?"
arm base $PY $BATCH/tools/hash_census.py $V/census-base.json > $V/census-base.log 2>&1; echo "census base rc=$?"
$PY $BATCH/tools/hashdiff.py $V/census-base.json $V/census-cand.json $V/hashdiff.json; echo "hashdiff rc=$?"

for s in $SRC_1 $SRC_2; do echo "in-tree parquets $s: $(find $s/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $s status --porcelain | head -5; done
echo "JOB DONE $(date -Is)"
