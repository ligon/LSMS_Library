# usage (after common.sh):  run_tests.sh LABEL   -- runs the selection in cwd's checkout
LABEL="$1"
cd "$SRC"
FILES=$(ls tests/test_malawi_*.py tests/test_gh1275*.py tests/test_gh1210_invalid_food_waves.py tests/test_gh970_no_json_make.py tests/test_id_walk.py tests/test_panel_ids_persistence.py tests/test_build_transform_hash.py tests/test_gh1190_selected_farm_carrier.py tests/test_1111_malawi_applied_kg.py tests/test_gh1174_malawi_input_use.py tests/test_gh1457_malawi_farm_level_sales.py tests/test_gh1190_malawi_seed_purchase_binding.py tests/test_gh1174_malawi_nonseed_deletion.py tests/test_gh1287_malawi_metadata_compilers.py tests/test_gh1288_malawi_source_literals.py tests/test_gh218_retired_food_route.py tests/test_recall_period.py tests/test_recall_record.py tests/test_derivations.py tests/test_failure_is_not_absence.py 2>/dev/null || true)
mkdir -p "$E/tests"
echo "$FILES" > "$E/tests/files_$LABEL.txt"
set +e
"$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfEs --durations=25 --junitxml="$E/tests/junit_$LABEL.xml" $FILES > "$E/tests/pytest_$LABEL.log" 2>&1
echo "pytest $LABEL exit $?" | tee -a "$E/tests/exit_codes.txt"
"$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfEs --junitxml="$E/tests/junit_sample_$LABEL.xml" tests/test_sample.py -k Malawi > "$E/tests/pytest_sample_$LABEL.log" 2>&1
echo "pytest-sample $LABEL exit $?" | tee -a "$E/tests/exit_codes.txt"
find "$SRC/lsms_library/countries" -name '*.parquet' > "$E/tests/intree_parquets_$LABEL.txt"
set -e
