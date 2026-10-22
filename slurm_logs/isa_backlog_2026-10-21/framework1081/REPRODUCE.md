# #1081 reproduction and freeze

Private runnable scripts/logs: `/local/job40255479/control/isa-backlog-20261021/framework1081-fix/`.
Frozen independent diagnosis and actual f8 negatives: sibling `framework1081-diagnosis/` (never edited). Runtime source head is `ca3e5cc119896983b49f50a6b9dade6a5f825131`; ledger-first head is `12346145ab715c28b604f3cc8cdfba7d82db7072`. Final freeze is documentation-only and is pinned in `freeze-proof.json`.

`run.sh baseline` selects immutable f8 diagnosis checkout; `candidate` selects only the approved writer. Both set exact cwd/PYTHONPATH/countries/data roots, mounted image Python, private configuration/temp/DVC site cache, active socket/write guards plus unshare -rn, numeric/build/Make threads1, no reader credentials and no acquisition/cache-bypass flags. Test workers use loadfile distribution and explicit identity checks.

Recorded commands (stdout/stderr go to matching private logs):

```bash
P=/local/job40255479/control/isa-backlog-20261021/framework1081-fix
PY=/local/job40255479/venv_img/bin/python
bash "$P/run.sh" baseline "$P/hash_probe.py" > "$P/baseline-hashes-private.log" 2>&1
bash "$P/run.sh" candidate "$P/hash_probe.py" > "$P/candidate-hashes-private.log" 2>&1
bash "$P/run.sh" candidate -m pytest -n 2 --dist=loadfile -q \
 -p no:cacheprovider -p pytest_identity \
 --basetemp="$P/pytest-candidate" --junitxml="$P/tests-private.xml" \
 tests/test_1081_derived_categorical_errors.py \
 tests/test_categorical_mapping_empty_table.py \
 tests/test_categorical_mapping_resolution.py \
 tests/test_categorical_mapping_value_required.py \
 tests/test_food_prices_units_kwarg.py tests/test_categorical_key_resolver.py \
 > "$P/tests-private.log" 2>&1
bash "$P/run.sh" candidate "$P/probe_desired.py" > "$P/desired-probes-private.log" 2>&1
bash "$P/run.sh" candidate "$P/probe_valuation.py" > "$P/valuation-private.log" 2>&1
# An additional identical166-case replay also checks Python __context__.
bash "$P/run.sh" candidate "$P/probe_context.py" > "$P/context-probes-private.log" 2>&1

# One-time physical source/reference copies. Asserts destinations absent;
# do not repeat over an existing root, delete evidence or clear a cache.
"$PY" "$P/prepare_cache.py" > "$P/cache-preparation-private.log" 2>&1
bash "$P/run.sh" baseline "$P/healthy.py" initial > "$P/baseline-healthy-initial-private.log" 2>&1
bash "$P/run.sh" candidate "$P/healthy.py" initial > "$P/candidate-healthy-initial-private.log" 2>&1
bash "$P/run.sh" baseline "$P/healthy.py" warm > "$P/baseline-healthy-warm-private.log" 2>&1
bash "$P/run.sh" candidate "$P/healthy.py" warm > "$P/candidate-healthy-warm-private.log" 2>&1
bash "$P/run.sh" baseline "$P/feature_probe.py" > "$P/baseline-feature-probes-v2-private.log" 2>&1
bash "$P/run.sh" candidate "$P/feature_probe.py" > "$P/candidate-feature-probes-v2-private.log" 2>&1
bash "$P/run.sh" candidate scripts/api_surface_audit.py --strict > "$P/api-audit-private.log" 2>&1
bash "$P/run.sh" candidate "$P/validate_runtime.py" > "$P/runtime-validation-private.log" 2>&1
```

The existing f8 73-case baseline is42fail/31pass/0skip/error. `baseline-negative-binding.json` proves current test function/parametrization AST identity to that actually executed immutable draft; only the public docstring/unused import differ. Candidate requires235 passes, with no accepted failures/xfails. Generic and UnitsUnavailable preservation controls retain their original expectations; only the three categorical types change expected outcomes.

Hash probes use only existing metadata APIs and compare all549 keys/39 fingerprints in separate processes; `hash-scope.json` records zero changes. No stash or guessed cost. Healthy data roots start with manifest-verified current-hash source caches, so these are normal read-path checks, not a source-cold rebuild claim. Source/raw quantities, units and scientific factors remain unchanged.

`healthy.py` stores each complete frame privately and checks exact order/dtypes/null masks/axes/attrs. Both Country defaults and explicit unitvalue/Aggregate calls are included; Feature's default currency and explicit currency=None are separate calls/references. Warm runs include new Country/Feature instances and require all60 parquet byte/size/mode/mtime receipts unchanged. No derived parquet is created. `feature_probe.py` uses real Feature and generated Country handlers with private synthetic source errors/healthy frames, retaining existing drop/warning markers rather than patching policy.

`validate_runtime.py` proves the exact nine-line production diff and byte-identical UnitsUnavailable handler, full original source/fixture identity, diagnosis immutability and source/cache receipts. After the documentation-only commit, `freeze_proof.py` checks final HEAD/cleanliness/filesystem-origin identity, runtime/test blob equivalence and subprocess quiescence. All frames/IDs/full warnings, parser traces and logs remain private.

Sue.
