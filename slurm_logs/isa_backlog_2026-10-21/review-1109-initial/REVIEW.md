# Independent Malawi #1109 review

Verdict: **NEEDS_FIX** for frozen candidate `c08abb3f1dd03fd211d5071b45369dcbae42857f`, compared with frozen baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## Blocking finding

**P1: the stated source premise is false, and the proposed closeout leaves actual organic measurements unmaterialized.**

Anchors in the frozen review tree:

- `lsms_library/countries/Malawi/_/malawi.py:3010`: "Module D records no organic-fertilizer quantity"; lines 3020-3021 construct missing Quantity and unit for every affirmative use record.
- `.coder/ledger/1109-malawi-organic-quantity.md:28`: the new ledger repeats "Module D records no organic quantity" as an authoritative definition.
- `lsms_library/countries/Malawi/_/CONTENTS.org:3027`: the existing country documentation makes the same claim; the new fixed entry at line 3034 relies on it.

This directly contradicts the source headers and actual cached observations. In all seven Module D files, `ag_d36` asks whether any organic fertilizer was used on the plot; 1 means Yes and 2 means No. The immediately following `ag_d37a` is labelled "How much organic fertilizer did you apply ... (QUANTITY)" and `ag_d37b` is its UNIT. All seven dictionaries label native codes 2=Kilogram, 10=Bucket, 11=Wheelbarrow, 12=Ox cart and 13=Other (Specify), with capitalization varying by wave.

Among the **sources actually loaded by the wave scripts**:

| wave | affirmative organic use | positive observed quantity | missing quantity | observed unit |
|---|---:|---:|---:|---:|
| 2010-11 Full Sample | 1,991 | 1,986 | 5 | 1,986 |
| 2013-14 | 822 | 820 | 2 | 819 |
| 2016-17 XS + Panel | 3,617 | 3,617 | 0 | 3,617 |
| 2019-20 XS + Panel | 4,645 | 4,645 | 0 | 4,645 |
| total | **11,075** | **11,068** | **7** | **11,067** |

No affirmative organic quantity is an observed zero or negative. The redundant 2010-11 Panel file was inspected separately and excluded from these totals because the current wave script uses Full Sample. Source artifacts explicitly identify it.

The omission of these amounts predates this candidate; the min-count overlay itself does not newly delete a recorded amount. Nevertheless, the candidate's source explanation and ledger certification are false, and the advertised correction cannot be assessed as survey-grounded missingness restoration while 11,068 recorded positive amounts are being treated as absent. Synthetic tests begin after the source omission, so their success cannot settle this.

Root accepted the finding and chose to extend the mutable Malawi lane to read the actual reported organic quantities and native units now, retaining the use carrier for the seven missing amounts. That amendment requires a new frozen review. I did not edit source or expand the separate 437-record nonorganic investigation.

Reproducible, aggregate-only source evidence: `source-semantics.json`, `source-amounts.json`, `source-organic-unit-counts.json`; scripts `metadata.py`, `source_amounts.py`, `source_units.py`. They inspect private physical copies of the existing raw blobs, print no raw records, and make no acquisition requests. The source contradiction is reported against the country documentation rather than silently treating that documentation as stale.

## What passes mechanically

The frozen reducer and carrier behave as proposed:

- Independent offline **cold builds of both frozen trees**, followed by exact comparison, retain all **180,184 rows**, identical original index/order/dtypes, and all preexisting columns except the intended 11,075 organic Quantity zero-to-NA changes. `Applied` is nullable boolean, True on organic rows and missing on all **169,109 nonorganic rows**.
- Nitrogen frames are exactly equal: **33,466 plots**, **840235.7795800001 kg N**. The organic refusal warning remains. This numeric equality does not validate the false source-quantity premise.
- Independent cold-to-warm and paired `Feature('plot_inputs')(['Malawi','Uganda'], currency=None)` checks retain **264,400 rows**. Pooled index/order/preexisting dtypes are exact apart from the intended Malawi Quantity correction; Uganda's preexisting frame is exact.
- Seven independent edge scenarios pass: three input-order permutations with distinct null/observed crop and unit keys, observed zero, positive, mixed, negative and all-missing amounts, plus four nullable-boolean max cases (including False/NA and all-NA). Full-key reindexing and nullable max are sound.
- Optional Applied follows the existing Ethiopia declaration and survives the actual Country finalizer. No framework policy is changed.
- Independently recomputed hash impact: **22/22 Malawi change; 0/24 Uganda change**.

The 437-record residual is appropriately labelled diagnosis-first and does not invent nonorganic application flags. Its reported prior rejected-build evidence distinguishes it from unit identity (#1118), a generic reducer sweep (#905), and key-soundness (#637). I independently confirmed the accepted candidate leaves every nonorganic value unchanged; I did not repeat the rejected global reducer experiment or adjudicate those 437 sources.

## Commands and results

All execution used `/local/job40255479/venv_img/bin/python`, checkout-specific PYTHONPATH and LSMS_COUNTRIES_ROOT, asserted realpath import identity, private physical writable L1/L2, numeric/build threads=1, offline socket guard, and at most two loadfile workers. See the saved scripts and logs for exact paths. The common candidate environment was:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=1 \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
LSMS_BUILD_WORKERS=1 \
PYTHONPATH=/local/job40255479/worktrees/isa-review-1109:/local/job40255479/control/isa-backlog-20261021/review-1109 \
LSMS_COUNTRIES_ROOT=/local/job40255479/worktrees/isa-review-1109/lsms_library/countries \
LSMS_DATA_DIR=/local/job40255479/control/isa-backlog-20261021/review-1109/fixed
```

With that environment and mounted image bin first on PATH:

```sh
python -m pytest -p no:cacheprovider -q -n 2 --dist=loadfile tests/test_1109_malawi_input_quantity.py tests/test_867_nitrogen_content.py -k 'not ethiopia_warm'
# 36 passed, 7 warnings, 6.88 seconds; targeted.log
python -m pytest -p no:cacheprovider -q tests/test_feature_canonical_index.py::TestPlotTableDeclarations tests/test_feature_canonical_index.py::TestPlotInputsHarmonize tests/test_feature_canonical_index.py::TestPlotTablesEndToEnd
# 21 passed, 92.60 seconds; assembly.log
python /local/job40255479/control/isa-backlog-20261021/review-1109/edge_checks.py
# 7 independent edge cases passed; edge-checks.json
```

The candidate regression file run under the untouched frozen baseline import/config identity gives **six failures and five passes**, as claimed (`negative_control.py`, `negative-control.log`). This validates the helper-level regression controls, not the real source omission.

The frozen `run_cold.py`, `check_warm_feature.py`, and `compare.py` runners were replayed against both checkouts and new private data roots. The additional independent `pooled_compare.py` checks exact pooled compatibility. Results: `baseline/cold-summary.json`, `fixed/cold-summary.json`, `comparison.json`, `pooled-comparison.json`, `hash-comparison.json`. Raw served pickles and build logs remain only in this private control directory.

## Limits and tree proof

No full suite or new country census was run. The writer's two-country scanner records were inspected but not independently rerun; its 15-warning/zero-failure claim is not an independent validation here. No DVC CLI, shared-cache mutation/promotion, source acquisition, fixture regeneration, git mutation, commit, or GitHub write occurred. The mutable `isa-Malawi` lane was never tested.

`make api-audit` passed all 15 on-disk skills but unexpectedly caused Poetry to create an ignored `.venv` in the frozen worktree. This was disclosed promptly to root; it was neither used nor removed. No dependency installation was requested. All subsequent Python execution used the mounted image.

`initial-tree.json` and `final-tree.json` prove frozen HEAD remained c08abb3f, tracked status remained empty, and the digest over all tracked file paths and bytes remained **0ed183ecc5f939d2609ad9511f6278c49c4ade1c856fe662caf21513a3594731**. There are zero in-tree country parquets. Review reports and scripts are under `/local/job40255479/control/isa-backlog-20261021/review-1109/`.

- Sue
