PASS for the Tanzania partial #1118 runtime at `0c8d9645ec35d65eb252fc6abcd7e7c2e777b89b`, compared with the original release baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`. The documentation-only descendant `e75be9c56fa6c318ce32cea5a8be6d448578bed1` resolves the documentation findings below. No unresolved code or data finding blocks this scoped correction.

Independent review by Sue, 2026-10-21. Runtime checkout: `/local/job40255479/worktrees/isa-review-1118-Tanzania`. All review artifacts are private to this directory; this report and the aggregate JSON contain no source identifiers or warning examples.

The full Tanzania `_/CONTENTS.org`, including TODO, WAITING and LOGBOOK, informed the review. Its actual constraints include UPHI back-casting rather than interviewed-household identity in the older multi-round panel, disjoint Extended/Refresh subpanels, 2019 Extended versus 2020 Refresh plus the 545-household urban booster, and the missing 2020 population statement. The live WAITING #114 at `CONTENTS.org:1087` remains a caveat despite the closed issue. Modern plot-input support covers only 2019-20 and 2020-21. Older agricultural feature support is outside this change.

The implementation at `lsms_library/countries/Tanzania/_/tanzania.py:1471` retains the existing duplicate reducers and applies the existing `U_UNKNOWN` sentinel only after reduction at line 1488. This placement preserves later recorded units selected by `first`. The removed statement, "u is meaningless where there is no quantity", contradicted the existing #1041 ruling; `CONTENTS.org:1813` explicitly requires Unknown with or without quantity. Known unit labels remain informative when amount is absent. No source decoder, ID rule, source inclusion rule, conversion factor or shared framework behavior changes.

Two documentation inaccuracies in frozen 0c8 were independently reported and resolved in e75. The original ledger at `.coder/ledger/1118-tanzania-plot-input-units.md:60` said "No 2008-15 edits or rebuilds", while the normal public API necessarily rebuilt the unchanged all-wave sample prerequisite from held covers after the country module hash moved. The descendant qualifies the permitted supporting rebuild without permitting older-wave behavior/configuration edits. The producer report also counted 27 Uganda parquet files where its manifest and the actual comparator contain 26; both occurrences are corrected. The descendant explicitly discloses the Feature attrs delta below. Only the ledger and producer REPORT.org change; `lsms_library` and `tests` git tree objects are identical to 0c8. No data/test rerun or additional hash change is warranted. See [doc-descendant-proof.json](doc-descendant-proof.json).

The native oracle independently read the four held agriculture extracts and their Stata dictionaries. It bound every quantity, unit, purchased flag/quantity and improved flag from source fields and source-slot order, then checked every output row and field. It used original-baseline canonical crop/fertilizer name maps only for the unchanged naming convention; it did not use candidate unit maps. Dictionary labels distinguish total seed quantity/unit (`ag4a_10_1/2`) from purchased seed quantity/unit (`ag4a_10c_1/2`) and previous-season channels. The two 2020 source records with an absent/OTHER total unit and a known purchase unit do not borrow that purchase unit. Fixed fertilizer quantities are explicitly labelled KG in the native dictionaries. No unlisted nonnull total-unit codes or recoverable unit-specification text occur in these held extracts.

| Native and public check | 2019-20 | 2020-21 | Total |
|---|---:|---:|---:|
| Source long rows before unchanged reduction | 1,787 | 8,035 | 9,822 |
| Public Country rows retained | 1,774 | 7,924 | 9,698 |
| Missing unit normalized to Unknown | 86 | 322 | 408 |
| Normalized unit with reported amount | 72 | 317 | 389 |
| No-amount rows retained | 14 | 5 | 19 |

All 19 no-amount rows retain their identities and are Seed rows in the held data. Synthetic regressions additionally test known units with missing quantities across all input types. All three actual missing-first/known-later duplicate groups retain their known units and complete public group frames. All six existing mixed-known-unit Seed/Pulses groups, comprising 14 source members, retain their complete helper/public group frames and attrs. Their incommensurable quantity sum with first unit remains the documented #637 defect (`CONTENTS.org:1824`); it is not corrected or enlarged here. Evidence: [independent-source-oracle.json](independent-source-oracle.json), [source_oracle_review.py](source_oracle_review.py), [independent-native-metadata.json](independent-native-metadata.json). Identifiable group frames stay in private pickle artifacts.

Both original baseline and frozen candidate underwent genuine normal cold Country builds from physically private L1 copies and initially empty Tanzania L2 roots. No `assume_cache_fresh`, LSMS_NO_CACHE, backend override or stamp bypass was used. The normal `Country('Tanzania').plot_inputs()` path rebuilt all six logical sample waves from the three held cover files, including the 2008-15 multi-round sample; current country/sample hashes and all-wave coverage were verified. Cold times were 12.953s baseline and 13.891s candidate. Separate-process ordinary warm reads were 1.393s and 1.740s, respectively, exactly equal to their own cold frames/attrs, with zero parquet bytes or mtimes changed. Script-wave parquets remain hashless by the existing design. See the four `baseline/candidate-cold/warm.json` files and [run_api.py](run_api.py).

The entire candidate Country frame equals the baseline frame with exactly 408 missing `u` cells replaced by Unknown. Index, order, dtypes, attrs and every other field are exact; all 9,698 rows remain, including all 19 missing quantities. The healthy original-f8 Uganda comparator was physically copied from the producer's healthy reference, not an integration #1062 cache. Its normal public read returns 84,216 rows with no missing/blank unit and is exact in all fields/attrs to that reference. All 26 comparator parquets remain byte-identical in both private roots.

Ordinary `Feature('plot_inputs')(['Tanzania', 'Uganda'])` returns 93,914 rows. Both full country parts and the complete assembled frame were checked with the existing canonical aligner, aliases, sentinels and default `currency='index'`. Baseline versus candidate Feature cells, index, order and dtypes are entirely exact. The sole attrs change is `canonical_alignment.promoted.Tanzania.u: 408 -> 0`, because Country now performs the existing normalization before assembly; Uganda u remains 0 and fabricated Tanzania season remains `unknown_season`. All other attrs are exact. See [Feature-before-after.json](Feature-before-after.json) and both `*-feature-compatibility.json` artifacts.

Both baseline and candidate ran the complete scoped audit with phases 1 and 2, countries Tanzania/Uganda and feature plot_inputs, without filtering. Plot_inputs has zero phase-2 kwarg-grid entries. Both scans produce 11 warnings, zero failures and zero errors. All 11 finding identities, kwargs, status, severity, expected fields and fingerprints match. All metrics match. Ten raw warning details are exact; the sole changed detail is the declared canonical-alignment warning reporting Tanzania u promotion 408 -> 0. This is a complete before/after comparison, not a candidate-only warning census.

| Unfiltered finding classification | Baseline warnings | Candidate warnings | Delta |
|---|---:|---:|---|
| Tanzania native null crop runtime | 2 | 2 | Exact |
| Tanzania native null crop diagnostic | 1 | 1 | Exact; 2,830 nonseed rows |
| Tanzania API-joined v versus scheme | 1 | 1 | Exact |
| Uganda pre-finalize null keys | 2 | 2 | Exact |
| Uganda cross-wave cluster identity | 2 | 2 | Exact |
| Uganda API-joined v versus scheme | 1 | 1 | Exact |
| Feature declared canonical alignment | 1 | 1 | Tanzania u fill count 408 -> 0 |
| Tanzania 2020 population metadata caveat | 1 | 1 | Exact |
| Total | 11 | 11 | No new/lost finding |

[finding-delta.json](finding-delta.json) records every matched finding and before/after detail digest without exposing source IDs/examples; [scanner-aggregate.json](scanner-aggregate.json) records the classifications. Full raw scanner JSONL and logs remain private.

The final scoped regression selection passed **246 tests, 6 warnings, zero failures and zero skips**, including all 16 final issue tests. The original-baseline negative control ran those same 16 cases with the original import identity asserted: **14 expected failures and 2 compatibility passes**. A private early-fill mutant failed the placement-sensitive duplicate test: **1 expected failure**. Thus the tests detect both the old behavior and incorrect normalization placement. The producer's historical 243-test selection preceded three added parameters; this independent final run supplies 246 without rewriting that history. The direct API-surface audit also passed. Evidence: `scoped-tests.log`, `baseline-negative.log`, `early-mutant.log`, `api-audit.log`; mutant/module/test copies are outside the tracked tree.

Independent fresh-process hash enumeration covers **all 549 country/table cells and all 39 framework table fingerprints**. Exactly 25 Tanzania country hashes change and 524 cells remain equal; zero other-country hashes and zero framework fingerprints change. See [hash-comparison.json](hash-comparison.json) and both `*-all-hashes.json` artifacts. The doc descendant touches no build input or callable source, so its additional hash/fingerprint scope is zero.

The commands below describe the executed bounded selections. `TASK_TAG` is baseline or candidate, and `TASK_CHECKOUT` is its exact pinned checkout. The cold commands were executed before the private Tanzania L2 existed; those roots are now warm. A new cold replay requires new physical private copies of the seven held sources and the comparator, plus copies of these reviewer scripts into the new private evidence root. Do not remove shared caches or bypass freshness to recreate the cold state.

```sh
TASK_EVIDENCE=/local/job40255479/control/isa-backlog-20261021/review-1118-Tanzania
TASK_PYTHON=/local/job40255479/venv_img/bin/python
TASK_CHECKOUT=/local/job40255479/worktrees/isa-review-1118-Tanzania
TASK_TAG=candidate
cd "$TASK_CHECKOUT"
export PYTHONPATH="$TASK_CHECKOUT:$TASK_EVIDENCE"
export LSMS_COUNTRIES_ROOT="$TASK_CHECKOUT/lsms_library/countries"
export LSMS_DATA_DIR="$TASK_EVIDENCE/$TASK_TAG"
export PATH="/local/job40255479/venv_img/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=1
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1 AWS_EC2_METADATA_DISABLED=true
export LSMS_ISSUES_LOG="$TASK_EVIDENCE/issues-$TASK_TAG.jsonl"
unset LSMS_NO_CACHE LSMS_BUILD_BACKEND

unshare -rn -- "$TASK_PYTHON" "$TASK_EVIDENCE/source_oracle_review.py"
unshare -rn -- "$TASK_PYTHON" "$TASK_EVIDENCE/run_api.py" "$TASK_TAG" cold
unshare -rn -- "$TASK_PYTHON" "$TASK_EVIDENCE/run_api.py" "$TASK_TAG" warm
unshare -rn -- "$TASK_PYTHON" "$TASK_EVIDENCE/feature_and_compatibility.py" "$TASK_TAG"
unshare -rn -- "$TASK_PYTHON" "$TASK_EVIDENCE/all_hashes.py" "$TASK_TAG"
unshare -rn -- "$TASK_PYTHON" bench/feature_audit/scan.py \
  --features plot_inputs --countries Tanzania Uganda --phases 1,2 --jobs 1 \
  --out "$TASK_EVIDENCE/$TASK_TAG-scan.jsonl"
unshare -rn -- "$TASK_PYTHON" -m pytest -p no:cacheprovider -q \
  -n 4 --dist=loadfile -m 'not requires_s3' \
  tests/test_1118_tanzania_plot_input_units.py tests/test_962_unit_sentinel.py \
  tests/test_feature_canonical_index.py tests/test_863_transforms.py \
  tests/test_867_nitrogen_content.py
unshare -rn -- "$TASK_PYTHON" scripts/api_surface_audit.py
```

For the baseline public/hash/scanner controls, `TASK_CHECKOUT=/local/job40255479/worktrees/isa-baseline` and `TASK_TAG=baseline`; rerun `cd` and the corresponding exports. The negative control invoked `negative_control.py` from that baseline cwd/import/config identity, targeting the frozen candidate's test file. The private early-fill control invoked `-m pytest -p no:cacheprovider -q "$TASK_EVIDENCE/test_early_fill_mutant.py::test_duplicate_missing_first_unit_does_not_displace_later_known_unit"` from the frozen candidate. All output was redirected to private logs.

Runner assertions compare real package/config/cwd paths to the exact frozen checkout and private data root. Network isolation used `unshare -rn` plus active `socket.socket.connect` and `connect_ex` guards; it preserved the socket class and tested the guarded methods. Numeric/build threads were one, with at most four loadfile test workers. The seven held blobs were physically private, sidecar-MD5 checked, and remained unchanged in both roots. Comparator copies had distinct inodes and verified SHA256. No acquisition, DVC CLI, shared-cache modification, fixtures/goldens/dependencies, in-tree data or Git/GitHub mutation occurred.

Limits: this is a held-seven-source, two-modern-wave data review and two-country assembly audit, with a complete hash/fingerprint scope check. It is not a new country census or full test suite. The unheld 2020 agriculture questionnaire was not fetched. The six #637 mixed-unit groups, null crop keys, population statement gap and live #114 caveat remain explicit pre-existing residuals. This verdict applies only to the Tanzania portion of #1118.

Final frozen proof: HEAD remains `0c8d9645ec35d65eb252fc6abcd7e7c2e777b89b`; git status is empty; the digest of all tracked path names/bytes equals the initial digest `f6849b56bd30fd428efbd3bfe34161fd1ad2a30c17e78b6be7760bb2977d6c38`. There is no in-tree .venv, parquet or Tanzania raw Stata file. [final-tree.json](final-tree.json) records this proof; [initial-tree.json](initial-tree.json) preserves the initial state. All 26 Uganda parquet hashes and all seven held-source MD5s remain unchanged in each private root.

-- Sue
