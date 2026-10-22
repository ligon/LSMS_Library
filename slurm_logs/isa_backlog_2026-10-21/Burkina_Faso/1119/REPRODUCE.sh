#!/bin/bash
# Recorded commands; frozen author root must not be rerun in place.
# A reviewer prepares a NEW root and adapts exact source/data identities.
P=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1119
# run.sh supplies mounted image, exact cwd/import/countries/config/data,
# private temp/XDG/DVC, threads1, socket/write guards and unshare-rn.
# Preparation must complete terminally before any source decode/API build.
bash "$P/run.sh" candidate prepare "$P/prepare.py"
bash "$P/run.sh" base hashes-before "$P/hash-inventory.py" "$P/base/hashes-before-private.json"
bash "$P/run.sh" base grounding "$P/source-grounding.py"
# Apply exactly two Preferred cells and expected test values in new writer.
bash "$P/run.sh" candidate hashes-prebuild "$P/hash-inventory.py" "$P/candidate/hashes-prebuild-private.json"
bash "$P/run.sh" candidate hash-scope "$P/hash-scope.py"
bash "$P/run.sh" candidate focused-unit -m pytest -q -p no:cacheprovider tests/test_burkina_faso_seed_crop_labels.py tests/test_burkina_faso_plot_inputs_identity.py -k 'not every_real_native_line and not source21_is_unlabelled'
# Expected parent exit1:5failed/4passed, no fixture errors.
bash "$P/run.sh" base negative -m pytest -q -p no:cacheprovider "$P/test-parent-negative.py"
for side in base candidate; do
 bash "$P/run.sh" "$side" cold "$P/build.py"
 bash "$P/run.sh" "$side" warm "$P/build.py"
 bash "$P/run.sh" "$side" feature "$P/feature-scan.py"
 bash "$P/run.sh" "$side" source-check "$P/built-source-check.py"
done
# Original compare.py stopped on default-currency expected construction.
# v2 uses saved actual operands only and does not replay APIs.
bash "$P/run.sh" candidate compare-v2 "$P/compare-v2.py"
bash "$P/run.sh" candidate seed-support -m pytest -q -p no:cacheprovider tests/test_gh1093_declared_unit_consumers.py tests/test_gh1159_pooled_reducers.py
# Supplemental wave hashes were measured AFTER builds, not before.
for side in base candidate; do bash "$P/run.sh" "$side" wave-hashes-v2 "$P/wave-hashes-v2.py"; done
bash "$P/run.sh" candidate api-audit "$P/api-audit.py"
# Complete command/checker SHA256 pins and private manifests: READY.json.
# No broad suite, acquisition, DVC CLI, source/cache bypass or shared mutation.
