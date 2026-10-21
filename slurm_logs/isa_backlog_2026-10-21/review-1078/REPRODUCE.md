# Independent GH1078 review commands

Sue (GPT-6). Exact runtime a4c92ccf6a40a8bf75cbb6c0bf2933cd1e69ec7b;
final docs d46795c303e2409ddb8ed5e4c62efca4e87ca347; baseline f8d236694.
All existing evidence roots are frozen. These are original executed commands,
not permission to overwrite their outputs. Replay in a new physical private
root with matching root-prepared DVC metadata links and copied/adapted guards;
retain the same source pins, actual MD5/SHA gates and interpreter identities.
The private launchers record every exact cwd, argument and environment.

```sh
P=/local/job40255479/control/isa-backlog-20261021/review-1078
PY=/local/job40255479/venv_img/bin/python
# ONE full suite, credential detector asserted false before collection:
$PY "$P/run_unit.py"
# Original result 4689pass/4453skip/9isolationfail; never reported suite PASS.
# Separate 36-copy preparation + nine unchanged timestamp-trigger cases:
$PY "$P/run_persistence_private.py"
# Separate exact-source metadata-only inventories:
$PY "$P/runner.py" baseline hashes "$P/hash-inventory.py"
$PY "$P/runner.py" candidate hashes "$P/hash-inventory.py"
$PY "$P/runner.py" baseline api-wave-hashes "$P/api-wave-hashes.py"
$PY "$P/runner.py" candidate api-wave-hashes "$P/api-wave-hashes.py"
# Verify released 1288-file author manifest; independently reconstruct/gate47
# declared held dependencies, empty cold roots, genuine corrupt-root physicalcopy:
$PY "$P/prepare_source.py"
# Hash scope SUCCESS receipt precedes dependent data calls.
$PY "$P/runner.py" baseline cold "$P/collect.py" cold
$PY "$P/runner.py" candidate cold "$P/collect.py" cold
$PY "$P/runner.py" candidate cold "$P/source_oracle.py"
$PY "$P/runner.py" baseline cold "$P/collect.py" warm
$PY "$P/runner.py" candidate cold "$P/collect.py" warm
# Strict Niger u failure is retained by collect.py; both complete frames saved.
$PY "$P/runner.py" candidate healing "$P/healing.py" cold
$PY "$P/runner.py" candidate healing "$P/healing.py" warm
$PY "$P/stage-scenario.py" candidate timed-pair
$PY "$P/coordinate.py" candidate
$PY "$P/trace-proof.py"
$PY "$P/stage-scenario.py" candidate market-then-currency
TRACE1078=1 $PY "$P/runner.py" candidate market-then-currency "$P/exercise.py" tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH tests/test_currency.py::test_country_currency_index_roundtrip
$PY "$P/stage-scenario.py" candidate currency-then-market
TRACE1078=1 $PY "$P/runner.py" candidate currency-then-market "$P/exercise.py" tests/test_currency.py::test_country_currency_index_roundtrip tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH
# Independent real f8 bad-writer counterfactual, same actual operations/tests:
$PY "$P/stage-scenario.py" baseline timed-pair
$PY "$P/coordinate_baseline.py" baseline
# Existing phase1/2 harness; actual default kwargs preserved, jobs1:
$PY "$P/runner.py" baseline cold /local/job40255479/worktrees/isa-review-1078-base/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices --countries Uganda Niger --jobs 1 --out "$P/scanner-baseline-private.jsonl"
$PY "$P/runner.py" candidate cold /local/job40255479/worktrees/isa-review-1078/bench/feature_audit/scan.py --features food_acquired food_expenditures food_quantities food_prices --countries Uganda Niger --jobs 1 --out "$P/scanner-candidate-private.jsonl"
# final_checks.py first preserves an over-strong scanner zero-write assertion
# failure after writing valid baseline-race/serial proofs. scanner_verify.py
# proves217fullrecords+73unchangedfiles+12normal additions with fullattrs/hashes:
$PY "$P/final_checks.py"
$PY "$P/scanner_verify.py"
# Final metadata/source/reference/copy/tree/quiescence receipts, no data APIs:
$PY "$P/freeze_review.py"
```

Unit arguments are in `unit/command-private.json` and XML/logs remain private.
Full source environments/commands are in `source-commands-private.jsonl`.
`runner.py` pins mounted Python, exact package/cwd/countries/data, private config,
reader configuration, XDG/temp/DVC/matplotlib, threads/build/Make1, guards and
`unshare -rn`. All source stages use ordinary caches, no acquisition or bypass.
Only timed-pair roots have two deliberately coordinated consumers; others one.
All consumer processes are stopped at the final freeze. No full suite/corpus
repeat is required to inspect this completed review.
