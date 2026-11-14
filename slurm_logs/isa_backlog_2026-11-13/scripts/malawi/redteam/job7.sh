#!/bin/bash
#SBATCH -J mwrt-7
# Red team, malawi DELTA: cold base-vs-new-candidate builds, compare, delta checks,
# #1712 probe, hash census + closure parts, tests.
CAND=9ca3518a35e6c688577283121baeb49a939b9e0f
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND $BASE || exit 9
O=$OUT/job7; mkdir -p $O/frames
arm() { local which=$1; shift; if [ $which = cand ]; then cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; else cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$@"; fi; }
( arm cand $PY $RT/scripts/build_arm.py $SRC_1 cand $O/frames > $O/frames/cand.out 2>&1; echo "build cand rc=$? $(date -Is)" ) &
( arm base $PY $RT/scripts/build_arm.py $SRC_2 base $O/frames > $O/frames/base.out 2>&1; echo "build base rc=$? $(date -Is)" ) &
( arm cand $PY $RT/scripts/probe_1712.py $SRC_1 > $O/probe_1712.out 2>&1; echo "probe rc=$?"
  arm cand $PY $BATCH/tools/hash_census.py $O/census-cand.json > $O/census-cand.log 2>&1; echo "census cand rc=$?"
  arm base $PY $BATCH/tools/hash_census.py $O/census-base.json > $O/census-base.log 2>&1; echo "census base rc=$?"
  $PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json; echo "hashdiff rc=$?"
  arm cand $PY $RT/scripts/fwimp_parts.py $SRC_1 $O/fwimp-cand.json > $O/fwimp-cand.log 2>&1; echo "fwimp cand rc=$?"
  arm base $PY $RT/scripts/fwimp_parts.py $SRC_2 $O/fwimp-base.json > $O/fwimp-base.log 2>&1; echo "fwimp base rc=$?" ) &
wait
echo "phase 1 done $(date -Is)"
( arm cand $PY $RT/scripts/compare.py $O/frames $O/compare.json > $O/compare.out 2>&1; echo "compare rc=$?" ) &
( arm cand $PY $RT/scripts/delta_check.py $SRC_1 $O/frames $O/delta.json > $O/delta.out 2>&1; echo "delta rc=$?" ) &
TF="tests/test_gh1228_malawi_cowpea_label.py tests/test_malawi_cassava_recode.py tests/test_gh1190_selected_farm_carrier.py tests/test_1111_malawi_applied_kg.py tests/test_malawi_sale_basis.py tests/test_gh1457_malawi_farm_level_sales.py tests/test_build_transform_hash.py tests/test_derivations.py tests/test_schema_consistency.py tests/test_gh1174_malawi_input_use.py tests/test_gh1190_malawi_seed_purchase_binding.py tests/test_gh1298_malawi_bounded_observations.py tests/test_gh637_malawi_tail.py tests/test_gh1476_malawi_unknown_plot.py"
( arm cand $PY -m pytest -n 6 --dist=loadfile -q -p no:cacheprovider -rfEs $TF > $O/pytest-cand.txt 2>&1; echo "pytest cand rc=$?" ) &
wait
( arm cand $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $O/pytest-ts-cand.txt 2>&1; echo "ts cand rc=$?" )
for f in $O/pytest-cand.txt $O/pytest-ts-cand.txt; do echo "== $f"; tail -n 3 $f; done
for s in $SRC_1 $SRC_2; do echo "in-tree parquets $s: $(find $s/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $s status --porcelain | head -5; done
echo "JOB DONE $(date -Is)"
