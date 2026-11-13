# Niger public food unit index changes dtype across ordinary cold and warm reads

Observed under two exact sources, with no implementation change to Niger:

- Baseline: `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
- GH1078 tested runtime: `a4c92ccf6a40a8bf75cbb6c0bf2933cd1e69ec7b`.
- Interpreter: `/local/job40255479/venv_img/bin/python`; actual versions are recorded in the adjacent runtime receipt.

Trigger: prepare a physical private L1 from the 47 receipt-verified held Uganda/Niger prerequisites with empty L2, then call `ll.Country('Niger').food_acquired()` with **kwargs `{}`**, through ordinary reads. Save that public frame. In an independent fresh process and new Country object, make the identical call against the normally built current-hash, full-four-wave cache.

Both calls return 239,877 rows. The `u` MultiIndex level is `StringDtype`, storage `pyarrow`, in both cases. Its cold dtype is `string`, repr `<StringDtype(na_value=<NA>)>`, `na_value` type `NAType`. Its warm dtype is `str`, repr `<StringDtype(na_value=nan)>`, `na_value` type `float`. Default `pd.testing.assert_frame_equal(cold, warm)` fails on that index dtype on **both** source pins. The initial strict failures and frames are preserved privately.

All values, null masks, keys, multiplicities, order, column dtypes, axis names and full attrs otherwise match. Enumerating every frame column and index/column level across the complete 26-frame scope finds no other dtype difference on either head. A diagnostic comparison with only `check_index_type=False` passes; it does not replace or normalize the strict failure. All 26 strict matched-stage baseline/candidate comparisons pass at cold and again at warm. Each fresh warm process leaves all 73 data-root files' bytes, modes and mtimes exact, with normal current hashes and full declared-wave coverage. Uganda healing cold/warm frames remain strictly equal.

Reproduction used `framework1078-fix/run.sh {baseline,candidate} cold framework1078-fix/frames.py cold`, followed in fresh processes by `... frames.py warm` (strict failure), and `... frames.py warm-complete` (enumeration without payload alteration). Exact working-directory, import/config/data identity, source MD5/SHA receipts, private socket/write guards and `unshare -rn` apply. Private complete inputs/frames/logs are under `/local/job40255479/control/isa-backlog-20261021/framework1078-fix/`; safe aggregate proof is `cold-warm-dtype-residual.json`. No source acquisition, stale-cache stamping, cache clearing or freshness bypass was used.

This is a pre-existing strict dtype residual, supported by both-source counterfactuals. GH1078 does not repair it. Root should deduplicate it before filing, and the independent reviewer must verify the scope and preservation claims. No household records or cell keys are included here.

Sue
