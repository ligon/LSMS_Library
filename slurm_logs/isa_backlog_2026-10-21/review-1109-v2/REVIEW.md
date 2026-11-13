# Independent Malawi #1109 source-amendment review

**PASS** for frozen clean `de9c2734b8a6bcf1326d0d4285ff9c1db1221767` at `/local/job40255479/worktrees/isa-review-1109-v2`, against the **original** frozen baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
No actionable defect found in the authorized source-backed amendment.
Rejected predecessor c08 is used only for a labelled negative regression control, never as the original baseline.

## Source findings and resolved contradiction

Read Malawi CONTENTS first, including its two live TODO entries and the absence of WAITING/LOGBOOK entries, then the corrected ledger, source amendment, report, reproduction instructions, residual and root's concrete source/finalization decisions.

`Malawi/_/CONTENTS.org:3025` explicitly corrects the historical claim **"Module D records no organic quantity"**. The source actually asks ag_d36 organic use, ag_d37a quantity applied and ag_d37b its unit. The initial review's blocker is resolved rather than silently discarding the contrary documentation.

Relevant idiosyncrasies were checked at the source and public boundaries:

- All seven held Module D headers agree on ag_d36 1=Yes/2=No and the organic-only unit dictionary: 2 Kilogram, 10 Bucket, 11 Wheelbarrow, 12 Ox cart, 13 Other (Specify). The redundant 2010 Panel file is excluded from the six-source row accounting, as required by the actual 2010 builder.
- `Malawi/2016-17/_/plot_inputs.py:40` retains the special restricted read needed for a bad byte in unrelated cross-sectional free text and now includes both organic fields.
- `Malawi/_/malawi.py:2928` uses the organic dictionary; `:3019` preserves the use gate, amount, nullable Applied and native unit. `:3040` qualifies unlisted observed codes so unrelated food aliases cannot reinterpret them. Missing identity alone becomes Unknown.
- `Malawi/_/malawi.py:3277` retains organic-only full-key min_count handling; nonorganic reducers are unchanged.

Reviewer-owned `independent_source_check.py` reads the physically copied source blobs directly, verifies their MD5s against sidecars and dictionaries against metadata, and constructs the expected answers **without calling the candidate organic decoder, assembly function, or Country finalizer**. It reuses only established unchanged ID formatting and panel-map closure, and separately verifies resulting public household/plot identities against original f8. Strict numeric parsing finds no nonempty conversion failure. Every raw and mapped public plot key is unique, so grouping hides no duplicate-source collision.

Every one of the **11,075** organic wave and public rows matches its source amount/unit exactly:

| public native unit | rows |
|---|---:|
| Kilogramme | 5,518 |
| Ox-Cart | 2,155 |
| Bucket | 1,451 |
| Other (Specify) | 1,204 |
| Wheelbarrow | 733 |
| Unknown | 8 |
| Organic unit code 1 | 5 |
| Organic unit code 3 | 1 |

There are **11,068 positive quantities and seven missing quantities**, with no observed source zero. All seven missing-amount affirmative-use rows survive as Applied=True. The six source-qualified codes preserve their unlabelled identities through the actual public finalizer; they have no inferred kg factor or positive nitrogen contribution. Known physical labels survive finalization unchanged. Synthetic controls separately preserve reported zero, mixed numeric/missing groups, absent optional columns and no-use gates.

Evidence: `independent-source-accounting.json` and its reviewer-owned script/log. Only aggregate facts are exported.

## Original-baseline compatibility

A fresh private offline candidate cold build took 74.45 seconds and returns **180,184 rows**. The original f8 cold/warm artifacts from the first independent review were physically copied into this review's private baseline; hashes and distinct inodes were verified. That predecessor baseline was not mutated.

Against original f8:

- All **169,109 nonorganic rows** retain exact preexisting index/order/values/dtypes. All 513 zero-only nonorganic payloads are exact, thereby preserving the 437-record residual subset without adjudicating its meaning.
- Organic index components, order, preexisting dtypes and other fields are exact except the authorized Quantity restoration and unit rekeying. Applied is the only additional optional column, nullable boolean, True for organic use and NA elsewhere.
- Default nitrogen frames **and all attributes** are exact: **33,466 plots**, **840235.7795800001 kg N**; the organic refusal remains explicit.
- Candidate cold/warm Malawi frames are exactly equal. Paired Malawi/Uganda Feature output retains **264,400 rows**. All original Uganda and nonorganic Malawi fields are exact; organic changes remain confined to Quantity/unit plus Applied.
- All **22/22 Malawi** table hashes change and **0/24 Uganda** hashes change. Uganda uses physically copied, footer-validated current release parquets; this is a healthy warm comparator, not a blanket cold-country claim.

Evidence: `comparison-vs-f8.json`, `independent-compatibility.json`, `fixed/cold-summary.json`, `fixed/warm-feature-summary.json`, `hash-comparison.json`, scripts and private logs.

## Tests and scanner

Mounted image Python `/local/job40255479/venv_img/bin/python` was used with exact checkout/cwd/PYTHONPATH/LSMS_COUNTRIES_ROOT assertions, private writable L1/L2, an offline socket guard, PYTHONDONTWRITEBYTECODE=1, PYTHONHASHSEED=1, BLAS/OpenMP/MKL/build threads=1, and at most two loadfile workers.

From the frozen candidate, with those environment variables and LSMS_DATA_DIR pointing to `review-1109-v2/fixed`:

```sh
python -m pytest -p no:cacheprovider -q -n 2 --dist=loadfile \
  tests/test_1109_malawi_input_quantity.py \
  tests/test_867_nitrogen_content.py \
  tests/test_feature_canonical_index.py::TestPlotTableDeclarations \
  tests/test_feature_canonical_index.py::TestPlotInputsHarmonize \
  -k 'not ethiopia_warm'
```

**72 passed, zero skips, 15 expected warnings, 9.11 seconds** (`targeted.log`). The Ethiopia warm case was explicitly excluded from this private Malawi/Uganda seed.

The final 28-case regression file under the untouched rejected **c08** runtime gives **16 failures and 12 compatibility passes** (`negative_control.py`, `c08-negative-control.log`). This labelled negative control confirms the new source assertions fail the rejected implementation.

The frozen cold/compare/warm runners were replayed with the new private candidate root. The independent source and compatibility scripts provide the stronger raw-source and original-f8 checks described above. `scripts/api_surface_audit.py` passed directly under mounted image Python, avoiding Poetry/venv creation.

Phase-1 scanner was independently rerun for plot_inputs and only Malawi/Uganda, using matching frozen runtimes and private roots:

```sh
python bench/feature_audit/scan.py --features plot_inputs \
  --countries Malawi Uganda --phases 1 --jobs 1 --out PRIVATE_SCAN.jsonl
```

Both emit **15 warnings, zero failures/errors**. Every emitted field, fingerprint, kwarg, identity and status is exact except three expected detail changes against **original f8**:

1. Two native-null-unit runtime details fall from 28,724 to **17,649**.
2. The existing constant-column warning detail now also names Applied (its nonmissing values are affirmative).

The owner's report of two changes is correctly scoped to c08, where Applied already existed. It must not be conflated with original-f8 comparison. All remaining native null units are nonorganic, with existing nonmissing Quantity. No broader #1118 completion is claimed.

Evidence: `baseline-scan.jsonl`, `candidate-scan.jsonl`, logs and `scanner-comparison.json`.

## Limits, residual and frozen proof

This is one Malawi feature, source-field extraction and public-boundary review. No full suite, other-country census or phase-2 scanner grid was run. Source free-text Other descriptions, organic nutrient shares, container weights, disputed inorganic units (#1111), product composition (#1106), non-crop markers and remaining #1118 work remain outside the approved amendment.

The 437-record residual remains diagnosis-first and keeps its distinction from #905 reducer work, #1118 unit identity and #637 key soundness. It is preserved by exact comparison of the entire nonorganic set. I did not broaden its source diagnosis or repeat the rejected global reducer experiment.

No tracked edits, git mutation, commit, checkout/switch, GitHub/API write, DVC CLI, dependency/fixture/baseline regeneration, acquisition or shared-cache mutation/promotion occurred. Only the frozen v2 tree, original frozen baseline and labelled rejected-c08 control were executed; the mutable owner lane was never tested.

`initial-tree.json` and `final-tree.json` prove unchanged frozen HEAD de9c2734, empty tracked status, zero in-tree country parquets, no created `.venv`, and identical digest over all tracked paths/bytes:

`4d8a68ad51225a93b2e16f58a59ff846233bbcbb0a2b6da435fbe89c8b035abc`

The original predecessor baseline artifacts remain hash-identical. Reports/scripts and all new private data live only under `/local/job40255479/control/isa-backlog-20261021/review-1109-v2/`.

Sue
