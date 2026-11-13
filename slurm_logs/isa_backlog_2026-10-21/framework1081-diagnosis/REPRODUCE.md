# Read-only diagnosis commands

`run.sh` binds exact frozen f8 cwd/PYTHONPATH/countries root and private data/config/tmp/DVC-site roots; uses mounted image Python, no-network socket guards with unshare, source/reference write guard and numeric/build/Make threads one. It disables no-cache/build-backend overrides and supplies no reader credentials. Actual data-root file count remains zero.

```bash
P=/local/job40255479/control/isa-backlog-20261021/framework1081-diagnosis
PY=/local/job40255479/venv_img/bin/python
# One-time fresh output/source-manifest preparation; never rerun over populated dirs.
"$PY" "$P/prepare.py" > "$P/preparation-private.log" 2>&1
bash "$P/run.sh" "$P/probe.py" > "$P/probe-v3-private.log" 2>&1
bash "$P/run.sh" "$P/probe_valuation.py" > "$P/valuation-private.log" 2>&1
bash "$P/run.sh" -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider \
 --basetemp="$P/pytest-existing" --junitxml="$P/existing-private.xml" \
 tests/test_categorical_mapping_empty_table.py \
 tests/test_categorical_mapping_resolution.py \
 tests/test_categorical_mapping_value_required.py \
 tests/test_food_prices_units_kwarg.py \
 tests/test_categorical_key_resolver.py > "$P/existing-tests-private.log" 2>&1
# Expected baseline exit1:42 failures,31 passes; no source implementation applied.
bash "$P/run.sh" -m pytest -n 0 \
 --rootdir=/local/job40255479/worktrees/isa-framework-1081 \
 -q -p no:cacheprovider --basetemp="$P/pytest-contract-v2" \
 --junitxml="$P/proposed-contract-v2-private.xml" \
 "$P/test_1081_proposed_contract.py" > "$P/proposed-contract-v2-private.log" 2>&1
bash "$P/run.sh" "$P/final_proof.py" > "$P/final-proof-private.log" 2>&1
```

The proposed tests run serial because their draft is outside the repository; no worker is permitted to silently change source identity. The supporting run uses two loadfile workers and LSMS_EXPECTED_ROOT asserts the real checkout. All log and JUnit files remain private. A future approved candidate must pass the proposed contract tests, rather than accept their baseline failures as xfails.

The initial65-case draft run (42fail/23pass) and initial two syntax-failed probe drafts/logs remain preserved; adding eight explicit generic legacy-failure controls produces the final73-case negative set. No tracked file or baseline fixture is changed to make a probe pass.

Sue.
