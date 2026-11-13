# GLOBAL17 bounded review commands

Sue (GPT-6). Frozen candidate7ecf8e95fef567554fe93870ba95abddd1955a86 versus actual16d527249ba9d3eaebf190a075e39043e36d2dbb85.
All evidence roots are frozen. These executed forms are documentation, not authorization to overwrite them.
Replay only in a NEW physical private root with independently prepared exact DVC metadata links, copied/adapted denial policy, mounted Python and the same source pins.
The private commands-private.jsonl records exact cwd/argv/environment; source/checker/operand receipts are manifest-pinned.

```sh
review_root=/local/job40255479/control/isa-backlog-20261021/integration/framework-seventeen-review
python_image=/local/job40255479/venv_img/bin/python
# Historical preparation and metadata gates precede dependent builds:
$python_image "$review_root/setup.py"
$python_image "$review_root/prepare.py"
$python_image "$review_root/runner.py" baseline hashes "$review_root/hash-inventory.py"
$python_image "$review_root/runner.py" candidate hashes "$review_root/hash-inventory.py"
# Hash-scope SUCCESS is required; source gates are terminal SUCCESS first.
$python_image "$review_root/runner.py" baseline cold "$review_root/collect.py" cold
$python_image "$review_root/runner.py" candidate cold "$review_root/collect.py" cold
$python_image "$review_root/runner.py" candidate compare "$review_root/compare.py" cold
$python_image "$review_root/runner.py" baseline cold "$review_root/collect.py" warm
$python_image "$review_root/runner.py" candidate cold "$review_root/collect.py" warm
$python_image "$review_root/runner.py" candidate compare "$review_root/compare.py" warm
# Focused438tests and ONE8-worker full selection; full result remains FAIL13:
$python_image "$review_root/runner.py" candidate focused "$review_root/tests.py"
$python_image "$review_root/runner.py" candidate full "$review_root/tests.py"
$python_image "$review_root/runner.py" baseline net-control "$review_root/net_control.py"
# Source-test race roots have prior physical scenario gates for BOTH layouts:
$python_image "$review_root/coordinate.py" baseline
$python_image "$review_root/coordinate.py" candidate
$python_image "$review_root/runner.py" baseline race-check "$review_root/race_check.py"
$python_image "$review_root/runner.py" candidate race-check "$review_root/race_check.py"
# Only after actual race proofs and physical new bad16-copy SUCCESS:
$python_image "$review_root/prepare_healing.py"
$python_image "$review_root/runner.py" candidate healing "$review_root/heal.py" cold
$python_image "$review_root/runner.py" candidate healing "$review_root/heal.py" warm
$python_image "$review_root/runner.py" candidate healing-delta "$review_root/healing_delta.py"
# Bothactual heads, bothgatedsource-order roots, cold then freshwarm:
TRACE1078=1 FORBID_FDC1078=1 $python_image "$review_root/runner.py" baseline order-forward "$review_root/orders.py" cold
TRACE1078=1 FORBID_FDC1078=1 $python_image "$review_root/runner.py" candidate order-forward "$review_root/orders.py" cold
TRACE1078=1 FORBID_FDC1078=1 $python_image "$review_root/runner.py" baseline order-reverse "$review_root/orders.py" cold
TRACE1078=1 FORBID_FDC1078=1 $python_image "$review_root/runner.py" candidate order-reverse "$review_root/orders.py" cold
# Repeat only each corresponding orders.py warm phase in a new process.
$python_image "$review_root/runner.py" candidate order-check "$review_root/orders_check.py"
# Original source serialorders were actual exercise.py invocations of:
# tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH
# tests/test_currency.py::test_country_currency_index_roundtrip
# in bothorders, distinct physicallygatedcandidate roots, TRACE1078=1.
# Existing full scanner, exactdefaultdeclaredkwargs, jobs1, peractualcheckout:
$python_image "$review_root/runner.py" baseline cold /local/job40255479/worktrees/isa-integration-seventeen-base/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices household_roster --countries Uganda Niger --jobs 1 --out "$review_root/scanner-baseline-private.jsonl"
$python_image "$review_root/runner.py" candidate cold /local/job40255479/worktrees/isa-integration-seventeen/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices household_roster --countries Uganda Niger --jobs 1 --out "$review_root/scanner-candidate-private.jsonl"
$python_image "$review_root/runner.py" candidate scanner-check "$review_root/scanner_check.py"
$python_image "$review_root/runner.py" baseline metadata "$review_root/metadata.py"
$python_image "$review_root/runner.py" candidate metadata "$review_root/metadata.py"
# Artifact/source receipt/freeze checks after all data consumers stop:
$python_image "$review_root/freeze_checks.py"
```

Do not rerun the generator to investigate the tracked doc mtime.
The original full selection,14-failure actual16negative selection, four-case baseline counterpart, all stopped attempts and complete raw diagnostics are preserved privately.
The launcher always uses unshare-rn plus the original denial policy and exact package/cwd/config/data identities, numerical/build/Make1 and explicit loadfile workers; the existing doc-test child PYTHONPATH reset is an explicit exception to full-selection Python guard propagation.
No API/source rebuild, full-suite repeat, normalized finding, new timestamp, stamp, cache bypass or hidden skip is needed to inspect this completed review.
