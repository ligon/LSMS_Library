#!/bin/bash
# Historical executed command forms; NOT an invitation to overwrite frozen
# evidence. A new review needs new physical roots, corresponding launcher/
# guard paths, and separately approved DVC metadata targets. Preparation is
# one-shot and refuses existing parquets. Preserve every original stop log.
# Actual15 4227e3a802c7349b11f41ec70a924d712159e587
# Actual16 d527249ba9d3eaebf190a075e39043e36d2dbb85
set -eu
review_root=/local/job40255479/control/isa-backlog-20261021/integration/framework-sixteen-review
stage_root=/local/job40255479/control/isa-backlog-20261021/integration/framework-sixteen-Ethiopia-prereq
python_image=/local/job40255479/venv_img/bin/python

bash "$review_root/run.sh" candidate "$review_root/protect.py" before
bash "$review_root/run.sh" candidate "$review_root/boundary.py"
# Initial complete gate stopped at42/60 and copied nothing. Root then
# explicitly approved the separate held-source prerequisite extension.
# The corrected preparer inspects the actual sample.py source bindings.
bash "$stage_root/run.sh" prereq "$stage_root/prepare.py"
# All20 sidecar-MD5/SHA/size/mode/mtime physical-copy gates completed first.
bash "$stage_root/run.sh" prereq "$stage_root/build.py"
# The initial build completed all4 tables, then stopped on a mistaken
# sample=household_characteristics expectation. Preserve that log/frames.
# The corrected fresh-process verifier used the measured25914 sample rows.
bash "$stage_root/run.sh" prereq "$stage_root/build.py" verify
bash "$stage_root/run.sh" prereq "$stage_root/freeze.py"
bash "$review_root/run.sh" candidate "$review_root/stage_receipt.py" before
bash "$review_root/run.sh" candidate "$review_root/prepare.py"
# Preparation must exit0 with120 physical copies/current60-per-root gates
# BEFORE any following test/API/fault/scanner command.

bash "$review_root/run.sh" candidate -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider -p pytest_identity --basetemp="$review_root/pytest-candidate" --junitxml="$review_root/candidate-tests-private.xml" tests/test_1081_derived_categorical_errors.py tests/test_categorical_mapping_empty_table.py tests/test_categorical_mapping_resolution.py tests/test_categorical_mapping_value_required.py tests/test_food_prices_units_kwarg.py tests/test_categorical_key_resolver.py tests/test_gh1077_nonfinite_label_keys.py
set +e
bash "$review_root/run.sh" baseline -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider -p pytest_identity --rootdir=/local/job40255479/worktrees/isa-baseline-fifteen-framework --basetemp="$review_root/pytest-baseline" --junitxml="$review_root/baseline-tests-private.xml" "$review_root/test_1081_current.py"
negative_status=$?
set -e
test "$negative_status" -eq 1
# Expected73-case counterfactual:42fail/31pass, zero errors/skips.

bash "$review_root/run.sh" baseline "$review_root/healthy.py" initial
bash "$review_root/run.sh" baseline "$review_root/probe_baseline.py"
bash "$review_root/run.sh" baseline "$review_root/probe_valuation.py"
bash "$review_root/run.sh" baseline "$review_root/feature_probe.py"
bash "$review_root/run.sh" baseline "$review_root/healthy.py" warm
bash "$review_root/scanner.sh" baseline
bash "$review_root/run.sh" baseline "$review_root/cache_metadata.py"

bash "$review_root/run.sh" candidate "$review_root/probe_context.py"
bash "$review_root/run.sh" candidate "$review_root/adversarial.py"
bash "$review_root/run.sh" candidate "$review_root/probe_valuation.py"
bash "$review_root/run.sh" candidate "$review_root/healthy.py" initial
bash "$review_root/run.sh" candidate "$review_root/healthy.py" warm
bash "$review_root/run.sh" candidate "$review_root/feature_probe.py"
bash "$review_root/scanner.sh" candidate
bash "$review_root/run.sh" candidate "$review_root/cache_metadata.py"
bash "$review_root/run.sh" candidate scripts/api_surface_audit.py --strict
# All actual commands redirected full stdout/stderr to corresponding PRIVATE
# logs. Independent baseline/candidate roots may run concurrently only after
# terminal preparation; each data root has exactly one consumer at a time.
bash "$review_root/run.sh" candidate "$review_root/finish_checks.py"
bash "$review_root/run.sh" candidate "$review_root/stage_receipt.py" after
bash "$review_root/run.sh" candidate "$review_root/protect.py" after
"$python_image" "$review_root/write_report.py"
# Every preceding consumer must have exited before final release.
"$python_image" "$review_root/release_review.py"
