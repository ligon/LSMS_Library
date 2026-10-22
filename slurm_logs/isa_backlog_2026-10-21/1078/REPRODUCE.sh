#!/bin/bash
# Exact command record. Default action verifies immutable source identities only.
# Full frames/sources and the isolated runner are private. Replay data-producing
# commands in a NEW private root with matching adapted runner/guard paths;
# do not overwrite frozen evidence or bypass its complete preparation gates.
set -eu
TASK_ROOT=/local/job40255479/worktrees/isa-framework-1078-fix
BASE=/local/job40255479/worktrees/isa-Uganda-1078
P=/local/job40255479/control/isa-backlog-20261021/framework1078-fix
RUNTIME=a4c92ccf6a40a8bf75cbb6c0bf2933cd1e69ec7b
export GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1
test "$(git -C "$BASE" rev-parse HEAD)" = f8d236694c26fe9e709c5548f02c70f74ea72c33
test -z "$(git -C "$TASK_ROOT" diff "$RUNTIME" -- lsms_library tests)"
test -z "$(git -C "$BASE" status --porcelain)"
if [ "${1:-verify}" = verify ]; then
    printf '%s\n' 'Baseline pin and runtime/test boundary verified. See commands action for original commands.'
    exit 0
fi
test "$1" = commands
cat <<'COMMANDS'
# run.sh pins cwd/PYTHONPATH/countries-root/data-root; unshare -rn + socket/write
# guards; mounted Python; numeric/build/Make threads1; private config/temp/DVC/XDG.
P=/local/job40255479/control/isa-backlog-20261021/framework1078-fix
# Metadata-only enumeration preceded all source builds, separate exact sources:
bash "$P/run.sh" baseline hashes "$P/hash-inventory.py"
bash "$P/run.sh" candidate hashes "$P/hash-inventory.py"
bash "$P/run.sh" baseline api-wave-hashes "$P/api-wave-hashes.py"
bash "$P/run.sh" candidate api-wave-hashes "$P/api-wave-hashes.py"
# prepare.py requires new empty roots, physically copies and MD5/SHA-gates held
# inputs and the genuine corrupt root, then writes COMPLETE_BEFORE... receipt.
bash "$P/run.sh" candidate preparation "$P/prepare.py"
# Focused cohort; output was redirected to focused-private.log:
bash "$P/run.sh" candidate focused -m pytest -q -n 2 --dist=loadfile -p no:cacheprovider -o addopts= --basetemp="$P/pytest-focused" tests/test_gh1078_food_producer_provenance.py tests/test_build_transform_hash.py tests/test_wave_getattr.py
# Ordinary healing; no clear, stamping, replacement or freshness bypass:
bash "$P/run.sh" candidate healing "$P/healing.py" cold
bash "$P/run.sh" candidate healing "$P/healing.py" warm
# Separate processes and new objects, identical default arguments:
bash "$P/run.sh" baseline cold "$P/frames.py" cold
bash "$P/run.sh" candidate cold "$P/frames.py" cold
# Initial strict warm attempts failed on Niger u dtype on both heads; retained.
bash "$P/run.sh" baseline cold "$P/frames.py" warm
bash "$P/run.sh" candidate cold "$P/frames.py" warm
# Continued enumeration preserves the strict difference without normalization:
bash "$P/run.sh" baseline cold "$P/frames.py" warm-complete
bash "$P/run.sh" candidate cold "$P/frames.py" warm-complete
bash "$P/run.sh" candidate comparison "$P/compare-stages.py"
# stage-scenario.py physically prepares each new private Uganda L1/L2 root:
for scenario in timed-pair market-then-currency currency-then-market; do
    /local/job40255479/venv_img/bin/python "$P/stage-scenario.py" candidate "$scenario"
done
# Actual two-process sample/currency test schedule, then full writer proof:
/local/job40255479/venv_img/bin/python "$P/coordinate.py" candidate
/local/job40255479/venv_img/bin/python "$P/trace-proof.py"
TRACE1078=1 bash "$P/run.sh" candidate market-then-currency "$P/exercise.py" tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH tests/test_currency.py::test_country_currency_index_roundtrip
TRACE1078=1 bash "$P/run.sh" candidate currency-then-market "$P/exercise.py" tests/test_currency.py::test_country_currency_index_roundtrip tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH
# Existing complete phase1/2 Feature harness, both source heads:
bash "$P/run.sh" baseline cold /local/job40255479/worktrees/isa-Uganda-1078/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices --countries Uganda Niger --jobs 1 --out "$P/scanner-baseline-private.jsonl"
bash "$P/run.sh" candidate cold /local/job40255479/worktrees/isa-framework-1078-fix/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices --countries Uganda Niger --jobs 1 --out "$P/scanner-candidate-private.jsonl"
/local/job40255479/venv_img/bin/python "$P/compare-scanner.py"
/local/job40255479/venv_img/bin/python "$P/freeze-check.py"
# Keep every full warning/log/frame/trace private; no author broad suite/corpus run.
COMMANDS
