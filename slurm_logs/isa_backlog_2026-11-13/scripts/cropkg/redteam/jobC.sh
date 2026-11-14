#!/bin/bash
# Red team GH #1559 job C: the screen on the Nigeria candidate (3d34550d5).
# sbatch ... jobC.sh CAND NIGERIA
set -uo pipefail
CAND=$1; NG=$2
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh cropkg-redteam "$CAND" "$NG" || exit 9
J=$SLURM_JOB_ID; JL=/local/job$J
FR_N=$JL/private/frames-ng; FR_O=$JL/private/frames-overlay; mkdir -p "$FR_N" "$FR_O"
# overlay: Nigeria candidate + the cropkg screen's two library files and its test file
OV=$JL/src-overlay
git clone --quiet --shared --no-checkout "$SRC_2" "$OV" && git -C "$OV" checkout --quiet --detach "$NG"
for f in lsms_library/quantity_audit.py lsms_library/_build_registry.py tests/test_gh1559_survey_kg_price_screen.py; do
  cp "$SRC_1/$f" "$OV/$f"
done
git -C "$OV" status --short
echo "Nigeria touches quantity_audit/_build_registry? $(git -C "$OV" diff --stat f138a3d61c203f892148592675a7ef72e160a322 $NG -- lsms_library/quantity_audit.py lsms_library/_build_registry.py | wc -l)"
( cd "$OV" && PYTHONPATH=$OV LSMS_DATA_DIR=$DATA_2 "$PY" -c "import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith('$OV/'), lsms_library.__file__; from lsms_library import quantity_audit as Q; print('overlay ok', hasattr(Q,'audit_survey_kg_prices'))" ) || exit 7
echo "=== census ng vs overlay $(date)"
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $BATCH/tools/hash_census.py $OUT/C-census-ng-$J.json )
( cd "$OV" && PYTHONPATH=$OV LSMS_DATA_DIR=$DATA_2 "$PY" $BATCH/tools/hash_census.py $OUT/C-census-overlay-$J.json )
"$PY" $BATCH/tools/hashdiff.py $OUT/C-census-ng-$J.json $OUT/C-census-overlay-$J.json $OUT/C-hashdiff-$J.json
echo "=== cold build Nigeria (Nigeria code) $(date)"
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$RT/scripts/rt_build.py" "$FR_N" nigeria "$OUT/C-build-ng-$J.json" Nigeria ); echo "exit $?"
echo "=== warm read Nigeria (overlay) $(date)"
( cd "$OV" && PYTHONPATH=$OV LSMS_DATA_DIR=$DATA_2 "$PY" "$RT/scripts/rt_build.py" "$FR_O" overlay "$OUT/C-build-overlay-$J.json" Nigeria ); echo "exit $?"
echo "=== compare $(date)"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$RT/scripts/rt_compare.py" "$FR_N" "$FR_O" "$OUT/C-build-ng-$J.json" "$OUT/C-build-overlay-$J.json" "$OUT/C-compare-$J.json" > /dev/null ); echo "exit $?"
echo "=== analysis (cand code on the Nigeria frame) $(date)"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$RT/scripts/rt_nigeria.py" "$FR_N/Nigeria_crop_production.pkl" "$OUT/C-nigeria-$J.json" ); echo "exit $?"
echo "=== overlay tests $(date)"
TESTS=$( cd "$OV" && ls tests/test_gh1559_survey_kg_price_screen.py tests/test_quantity_screen.py tests/test_build_transform_hash.py tests/test_gh1433_nigeria_crop_lines.py tests/test_gh843_nigeria_household_crop_sales.py tests/test_gh844_nigeria_harvest_value.py tests/test_nigeria_kg_factor.py tests/test_nigeria_sold_unit.py tests/test_gh1502_nigeria_food_conversion.py tests/test_nigeria_safe_harvest.py 2>/dev/null )
( cd "$OV" && PYTHONPATH=$OV LSMS_DATA_DIR=$DATA_2 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -rs -p no:cacheprovider $TESTS > $OUT/C-pytest-overlay-$J.txt 2>&1 ); echo "pytest exit $?"; tail -12 $OUT/C-pytest-overlay-$J.txt
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
for s in "$SRC_1" "$SRC_2" "$OV"; do echo "in-tree parquet $s: $(find "$s/lsms_library/countries" -name '*.parquet' | wc -l)"; done
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo "done $(date)"
