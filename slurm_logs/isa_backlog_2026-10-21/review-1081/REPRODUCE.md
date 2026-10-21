# Independent #1081 reproduction

Frozen candidate `bfbbf8efe81fdd0dd539928087ad06c6edb5ccb0`; baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`. Runtime `ca3e5cc119896983b49f50a6b9dade6a5f825131`; ledger first `12346145ab715c28b604f3cc8cdfba7d82db7072`.

Private harness root: `/local/job40255479/control/isa-backlog-20261021/review-1081`. Never rerun preparation over these frozen copies or write into an author/reference checkout. A new reviewer must create fresh physical private roots and approved DVC metadata targets. `run.sh` asserts exact source/config identity through `identity.py`, activates socket/write guards plus `unshare -rn`, directs data/config/DVC/XDG/matplotlib/temp state privately and pins numerical/build/Make threads to one.

Executed command forms:

```bash
review_root=/local/job40255479/control/isa-backlog-20261021/review-1081
python_image=/local/job40255479/venv_img/bin/python

# Historical one-time gate, before dependent API/test probes; do not repeat.
bash "$review_root/run.sh" candidate "$review_root/prepare.py" > "$review_root/preparation-private.log" 2>&1
"$python_image" "$review_root/static_proof.py" > "$review_root/static-proof-private.log" 2>&1

# Independent current test file physically copied to private space for f8.
bash "$review_root/run.sh" baseline -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider -p pytest_identity --rootdir=/local/job40255479/worktrees/isa-review-1081-base --basetemp="$review_root/pytest-baseline" --junitxml="$review_root/baseline-tests-private.xml" "$review_root/test_1081_current.py" > "$review_root/baseline-tests-private.log" 2>&1
# Expected nonzero: 42 failures / 31 passes, no skips/errors.

bash "$review_root/run.sh" candidate -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider -p pytest_identity --basetemp="$review_root/pytest-candidate-identity" --junitxml="$review_root/candidate-tests-identity-private.xml" tests/test_1081_derived_categorical_errors.py tests/test_categorical_mapping_empty_table.py tests/test_categorical_mapping_resolution.py tests/test_categorical_mapping_value_required.py tests/test_food_prices_units_kwarg.py tests/test_categorical_key_resolver.py > "$review_root/candidate-tests-identity-private.log" 2>&1

bash "$review_root/run.sh" baseline "$review_root/healthy.py" initial > "$review_root/baseline-healthy-initial-private.log" 2>&1
bash "$review_root/run.sh" candidate "$review_root/probe_context.py" > "$review_root/candidate-context-private.log" 2>&1
bash "$review_root/run.sh" candidate "$review_root/hash_probe.py" > "$review_root/candidate-hashes-private.log" 2>&1
bash "$review_root/baseline-controls.sh"
bash "$review_root/candidate-controls.sh"
bash "$review_root/run.sh" candidate "$review_root/finish.py" > "$review_root/final-proof-private.log" 2>&1
```

The sequential control scripts run baseline166 handler / five valuation / six Feature / full549/39 hash / fresh warm checks, and candidate72 additional cause-context / five valuation / initial+warm17-frame / six Feature / strict API audit checks. No simultaneous healthy data consumer uses the same root. The metadata-only hash inventory performs no table build.

`healthy.py` compares every complete frame to the full immutable author baseline, and candidate to the independent f8 frames; warm processes compare their own initial frames as well. Full logical attrs are checked directly, not by pickle bytes. Feature default currency and explicit `currency=None` are distinct references. Current-hash/coverage gates precede normal data calls, and all60 parquet byte/size/mode/mtime receipts must remain exact.

The initial candidate launcher omission (`LSMS_EXPECTED_ROOT`) and its single identity skip are retained in `candidate-tests-private.*`; only `candidate-tests-identity-private.*` is the terminal235/0skip gate. Extra cross-tree documentation-mode assertion stops and the measured writer0644/detached0600 distinction are preserved in `whole-tree-cross-check-initial-private.log` and `whole-tree-scope.json`. The full-tree SHA/mode/symlink comparison separately confirms that country.py is the sole changed original source path and the ledger-first commit contains only its ledger.

`verdict.json` and `proof.json` record clean frozen source/reference receipts,120 physical copies,457 original diagnosis files,801 author files, zero network/source-write attempts and no remaining review processes. `REVIEW.org` contains the source-safe verdict, exact scope and country-note proposals. Raw frames, IDs, messages and full logs remain private.

Sue
