#!/bin/bash
# Recorded successful commands in dependency order. Do not replay over evidence.
# A new cold replay needs new physical roots, adapted P paths and a root-prepared
# ignored DVC tmp target with matching exact metadata; never clear these roots.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/integration/malawi-fourteen-review
PY=/local/job40255479/venv_img/bin/python
"$PY" "$P/snapshot.py" before > "$P/snapshot-before.log" 2>&1
"$PY" "$P/composition.py" > "$P/composition.log" 2>&1
# Original prep attempts ran prepare.py and stopped before dependent probes.
# Their exact source/log copies remain prepare-initial-failed.*, 
# prepare-empty-root-assert-failed.* and prepare-reference-name-failed.*.
bash "$P/run.sh" candidate "$P/cache-search.py" > "$P/cache-search.log" 2>&1
bash "$P/run.sh" candidate "$P/prepare.py" > "$P/preparation.log" 2>&1
bash "$P/run.sh" candidate "$P/prepare_feature.py" > "$P/feature-preparation.log" 2>&1
bash "$P/run.sh" base "$P/runtime.py" cold > "$P/base-cold.log" 2>&1
bash "$P/run.sh" candidate "$P/runtime.py" cold > "$P/candidate-cold.log" 2>&1
bash "$P/run.sh" base-feature "$P/feature_cold.py" cold > "$P/base-feature-cold.log" 2>&1
bash "$P/run.sh" candidate-feature "$P/feature_cold.py" cold > "$P/candidate-feature-cold.log" 2>&1
bash "$P/run.sh" base "$P/runtime.py" warm > "$P/base-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/runtime.py" warm > "$P/candidate-warm.log" 2>&1
bash "$P/run.sh" base-feature "$P/feature_cold.py" warm > "$P/base-feature-warm.log" 2>&1
bash "$P/run.sh" candidate-feature "$P/feature_cold.py" warm > "$P/candidate-feature-warm.log" 2>&1
# test_runner.py records the complete vector and requires129 passes/zero skip.
# pytest -n2 --dist=loadfile -q -p no:cacheprovider -p pytest_identity
# tests/test_1111_malawi_applied_kg.py tests/test_1109_malawi_input_quantity.py
# tests/test_867_nitrogen_content.py tests/test_863_transforms.py
# tests/test_plot_id_canonical.py -k 'not warm'
bash "$P/run.sh" candidate "$P/test_runner.py" > "$P/tests.log" 2>&1
bash "$P/run.sh" candidate "$P/source_oracle.py" > "$P/source-oracle.log" 2>&1
bash "$P/run.sh" base "$P/run_scan.py" > "$P/base-scan.log" 2>&1
bash "$P/run.sh" candidate "$P/run_scan.py" > "$P/candidate-scan.log" 2>&1
bash "$P/run.sh" candidate "$P/compare_scan.py" > "$P/scanner-comparison.log" 2>&1
bash "$P/run.sh" candidate "$P/final_proof.py" > "$P/final-proof.log" 2>&1
"$PY" "$P/snapshot.py" after > "$P/snapshot-after.log" 2>&1
"$PY" "$P/quiescence.py"
"$PY" "$P/assemble_review.py"
