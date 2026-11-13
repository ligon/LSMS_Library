#!/bin/bash
# Historical commands, run from the named checkout through the guarded runner.
# The private root contains detailed source rows/IDs/warnings; do not publish it.
# prepare.py refuses existing blobs and cold refuses existing country L2.
# To repeat cold preparation, physically copy the scripts/offline guard into a
# NEW private root, update run.sh P to it, and run prepare.py successfully FIRST.
# Never clear, restamp, link, or reuse a warm comparator as a cold baseline.
set -euC
P=/local/job40255479/control/isa-backlog-20261021/Nigeria/1095/implementation/final-verification
FIRST=/local/job40255479/control/isa-backlog-20261021/Nigeria/1095/implementation
bash "$P/run.sh" candidate "$P/prepare.py" > "$P/prepare.log" 2>&1
# Both source roots were completely verified before these dependent launches.
# Each phase below completed successfully before dependent phases ran.
# Hash census/tests/API audit below were executed in FIRST on identical runtime
# code before the isolation-only rerun; copied hash artifacts retain their origin.
bash "$P/run.sh" candidate "$P/build.py" candidate cold > "$P/candidate-cold.log" 2>&1
bash "$P/run.sh" baseline "$P/build.py" baseline cold > "$P/baseline-cold.log" 2>&1
bash "$P/run.sh" candidate "$P/build.py" candidate warm > "$P/candidate-warm.log" 2>&1
bash "$P/run.sh" baseline "$P/build.py" baseline warm > "$P/baseline-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/source_inventory.py" > "$P/source-inventory.log" 2>&1
bash "$P/run.sh" candidate "$P/native_and_compare.py" > "$P/native-comparison-exact-drop-masks.log" 2>&1
bash "$P/run.sh" candidate "$P/feature_probe.py" candidate > "$P/candidate-feature.log" 2>&1
bash "$P/run.sh" baseline "$P/feature_probe.py" baseline > "$P/baseline-feature.log" 2>&1
bash "$FIRST/run.sh" candidate "$FIRST/hash_inventory.py" "$FIRST/candidate-hashes.json" > "$FIRST/candidate-hashes.log" 2>&1
bash "$FIRST/run.sh" baseline "$FIRST/hash_inventory.py" "$FIRST/baseline-hashes.json" > "$FIRST/baseline-hashes.log" 2>&1
bash "$FIRST/run.sh" candidate -m pytest -q -p no:cacheprovider --basetemp="$FIRST/pytest-focused" \
  tests/test_nigeria_community_units.py tests/test_gh1047_nigeria_unit_codes.py \
  tests/test_nigeria_cluster_identity.py::test_community_prices_is_post_harvest_only \
  tests/test_nigeria_cluster_identity.py::test_community_prices_metric_sizes_are_per_unit \
  tests/test_nigeria_cluster_identity.py::test_community_prices_unrecorded_package_sizes_dropped \
  -k 'not no_food_unit_is_float_stringified and not no_crop_row_has_a_nan_unit and not no_plot_input_has_a_nan_unit' > "$FIRST/tests-focused.log" 2>&1
# Private copy of the SAME production tests binds the immutable f8 helper/config.
# The executed initial baseline command had pytest's default private cache;
# the cache-disabled form below was also run (ten failures) and keeps test caches private.
if bash "$FIRST/run.sh" baseline -m pytest -q -p no:cacheprovider --basetemp="$FIRST/pytest-baseline-controls" \
  "$FIRST/test_nigeria_community_units.py" -k recorded_c8_unit_survives_actual_builder > "$FIRST/baseline-ten-controls-repeat.log" 2>&1; then
  echo 'Expected ten baseline failures; baseline unexpectedly passed' >&2
  exit 1
else
  baseline_status=$?
  [ "$baseline_status" -eq 1 ] || exit "$baseline_status"
fi
rg -q '^10 failed, 35 deselected in ' "$FIRST/baseline-ten-controls-repeat.log"
bash "$FIRST/run.sh" candidate -c 'import os,pathlib,lsms_library,subprocess; from lsms_library.paths import countries_root; p=pathlib.Path(os.environ["TASK_ROOT"]).resolve(); assert pathlib.Path.cwd()==p and pathlib.Path(lsms_library.__file__).resolve().is_relative_to(p) and countries_root().resolve()==p/"lsms_library/countries"; subprocess.run(["make","api-audit","POETRY=/local/job40255479/control/isa-backlog-20261021/Nigeria/1095/implementation/image-poetry.sh"],check=True)' > "$FIRST/api-audit-final.log" 2>&1
# Actual census artifacts were physically copied unchanged into the final root.
cp "$FIRST/candidate-hashes.json" "$P/candidate-hashes.json"
cp "$FIRST/baseline-hashes.json" "$P/baseline-hashes.json"
bash "$P/run.sh" candidate "$P/complete_checks.py" candidate > "$P/candidate-complete-checks-final.log" 2>&1
bash "$P/run.sh" baseline "$P/complete_checks.py" baseline > "$P/baseline-complete-checks-final.log" 2>&1
# Run only after commits: snapshot full tracked content/modes/head/status, then
# verify the frozen candidate plus all four protected trees remain exact.
bash "$P/run.sh" candidate "$P/freeze.py" freeze
bash "$P/run.sh" candidate "$P/freeze.py" verify
bash "$P/run.sh" candidate "$P/quiescence.py"
