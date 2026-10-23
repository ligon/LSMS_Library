#!/bin/bash
# Commands executed for this bounded review, in dependency order.
# Cold phases require the original empty private L2 roots; replay in a new
# physical review root after adapting the scripts' P paths, never over evidence.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/integration/nigeria-thirteen-review
PY=/local/job40255479/venv_img/bin/python
"$PY" "$P/snapshot.py" before
"$PY" "$P/composition.py"
bash "$P/run.sh" candidate "$P/prepare.py"
bash "$P/run.sh" baseline "$P/build.py" baseline cold > "$P/baseline-cold.log" 2>&1
bash "$P/run.sh" candidate "$P/build.py" candidate cold > "$P/candidate-cold.log" 2>&1
bash "$P/run.sh" baseline "$P/build.py" baseline warm > "$P/baseline-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/build.py" candidate warm > "$P/candidate-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/source-proof.py" > "$P/source-proof.log" 2>&1
bash "$P/run.sh" baseline "$P/feature_probe.py" baseline > "$P/baseline-feature.log" 2>&1
bash "$P/run.sh" baseline "$P/reference-differences.py" > "$P/reference-differences.log" 2>&1
bash "$P/run.sh" candidate -m pytest -q -n 2 --dist=loadfile -p no:cacheprovider \
  --basetemp="$P/pytest-focused" \
  tests/test_nigeria_community_units.py \
  tests/test_gh1047_nigeria_unit_codes.py \
  tests/test_nigeria_cluster_identity.py::test_community_prices_is_post_harvest_only \
  tests/test_nigeria_cluster_identity.py::test_community_prices_metric_sizes_are_per_unit \
  tests/test_nigeria_cluster_identity.py::test_community_prices_unrecorded_package_sizes_dropped \
  -k 'not no_food_unit_is_float_stringified and not no_crop_row_has_a_nan_unit and not no_plot_input_has_a_nan_unit' \
  > "$P/tests-focused.log" 2>&1
bash "$P/run.sh" candidate "$P/feature_probe.py" candidate > "$P/candidate-feature.log" 2>&1
REVIEW_TAG=baseline bash "$P/run.sh" baseline "$P/strict-warm.py" > "$P/baseline-strict-warm.log" 2>&1
REVIEW_TAG=candidate bash "$P/run.sh" candidate "$P/strict-warm.py" > "$P/candidate-strict-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/final-proof.py" > "$P/final-proof.log" 2>&1
"$PY" "$P/snapshot.py" after > "$P/snapshot-after.log" 2>&1
"$PY" "$P/quiescence.py"
"$PY" "$P/assemble-review.py"
