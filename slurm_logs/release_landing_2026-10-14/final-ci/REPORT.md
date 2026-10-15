SCOPE DEVIATIONS

None. Read-only monitoring, completed-log analysis, and source-only collection against empty private config/data roots. No source, fixture, original/shared cache, GitHub object, workflow or tag was changed. Raw job logs remain private; this bundle contains aggregate outcomes, source-only analysis/monitor scripts and log digests.

Final manual CI [38746123119](https://github.com/ligon/LSMS_Library/actions/runs/38746123119) **passed** on exact implementation `3914d8804d7b2936d75041cea98bbc9ddb6d4c9f`. The data job completed at 2026-10-15T14:49:31Z; metadata was observed at 2026-10-15T14:49:35.090158+00:00. All five non-skipped job logs were captured with full source identity. The detached monitor exited normally at 2026-10-15T14:49:38.768045+00:00; no log is missing and no monitoring process remains.

| Job | Outcome | Actual terminal evidence |
|---|---|---|
| [data-tests](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460137406) | success | 4927 passed, 2768 skipped, 4 xfailed, 1 xpassed, 1004 warnings in 3204.33s (0:53:24) |
| [unit-tests](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460137477) | success | 3293 passed, 4407 skipped, 60 warnings in 128.74s (0:02:08) |
| [windows-smoke](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460137580) | success | 327 passed, 1 skipped in 11.41s |
| [windows-script-build](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460137762) | success | Uganda:shocks rows=14457 cols=10 waves=8 digest=64a53fa7af55 (62s); Uganda:assets rows=116808 cols=1 waves=8 digest=9202ed542a14 (54s) |
| [dvc-remote-reachability](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460138161) | success | Completed successfully; no pytest summary |
| [dvc-lock-check](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460139629) | skipped | Skipped by workflow condition |
| [docs](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460140000) | skipped | Skipped by workflow condition |
| [refresh-dvc-locks](https://github.com/ligon/LSMS_Library/actions/runs/38746123119/job/116460140181) | skipped | Skipped by workflow condition |

The primary credentialed suite collected 7,700 items and finished with **4,927 passed, 2,768 skipped, 4 xfailed, 1 xpassed, zero failures/errors**, plus 1,004 warnings, in 3,204.33 seconds. Its command was sequential `poetry run pytest --tb=short --timeout=600 --timeout-method=thread --durations=25`, with the controlled build/BLAS profile. This actual successful test run closes the prior three-failure CI gate; it does not replace separate corpus, release, or publishing checks.

## Exact previous failures now pass

Prior corrected run38697245468 at9d5bf200f reported 4,919 passed, 2,768 skipped, 4 xfailed, 1 xpassed and three failures. The final run adds five actual-script assets regressions and turns those three failures into passes. All five new tests passed. The affected structural/invariance test code is unchanged between those source heads; the nutrition fixture was changed separately with explicit user approval, and the country assets compiler was independently reviewed.

| Exact test identity | Actual outcome change | Zero-based position / module item count |
|---|---|---|
| `tests/test_table_structure.py::TestTableStructure::test_no_duplicate_rows[Uganda/assets]` | failed → passed | 2260 / 2784 |
| `tests/test_table_structure.py::TestFeatureSanity::test_feature_is_sane[Uganda/assets]` | failed → passed | 2714 / 2784 |
| `tests/test_uganda_invariance.py::test_parquet_matches_baseline[var/nutrition.parquet]` | failed → passed | 49 / 53 |

This is not inferred only from the overall count. Source-only `pytest --collect-only -q -p no:cacheprovider tests/test_table_structure.py tests/test_uganda_invariance.py` collected2,837 nodes at the final source (2,784 structure,53 invariance). It ran with the worktree as cwd, explicit PYTHONPATH/countries root, empty private config/data roots, LSMS_SKIP_AUTH=1 and CPU16–23/BLAS1; no test body or data API executed. Each affected node's old terminal position is `F` and final position is `.`. The original failure summary independently names those same nodes. The parser checks full module lengths and calibrates against all three original failures. All parsed final module marker streams sum to the exact 7,700-item summary. See `semantic-review.json` and `analyze_terminal.py`; the private collection-log digest is recorded there.

Final structure coverage is153 passed/2,631 skipped; Uganda invariance52 passed/1 skipped. The relevant three nodes are in the passed portion. EHCVM seven-day module163 passed with no skips; currency-denominated units20 passed with no skips. The Ghana unitvalue/units regression took55.74 seconds. These current passes do not establish the historical cause of #941.

## Skip and expected-failure limits

The terminal log supports these module-level skip counts, summing exactly2,768. It did not print a per-node skipped-reason ledger, so these are outcome classifications by module, not inferred causes such as missing cache, missing raw source or credentials. The unchanged overall skip total alone is not a per-node coverage claim.

| Module | Actual skip markers |
|---|---:|
| `tests/test_table_structure.py` | 2631 |
| `tests/test_sample.py` | 85 |
| `tests/test_uganda_api_vs_replication.py` | 15 |
| `tests/test_gh866_stale_country_parquet_not_laundered.py` | 9 |
| `tests/test_863_transforms.py` | 7 |
| `tests/test_uganda_tables.py` | 6 |
| `tests/test_age_dtype_consistency.py` | 4 |
| `tests/test_food_prices_dtype.py` | 2 |
| `tests/test_gh323_benin_togo.py` | 2 |
| `tests/test_panel_attrition.py` | 2 |
| `tests/test_categorical_mapping_resolution.py` | 1 |
| `tests/test_gh809_hashless_country_parquet.py` | 1 |
| `tests/test_nonfood_expenditures_schema.py` | 1 |
| `tests/test_reader_sniff.py` | 1 |
| `tests/test_uganda_invariance.py` | 1 |

The four `xfailed` markers occur in `tests/test_sample.py`; the single `xpassed` marker occurs in `tests/test_pakistan_cluster_features.py`. They are recorded as their actual pytest outcomes, not relabeled as ordinary passes or asserted scientific approvals. This log does not identify their individual node reasons. No skip or assertion was added for this monitoring/analysis task.

## Native Windows and package identity

The installed-wheel native job passed both enforced expected digests: Uganda shocks14,457 rows,10 columns,eight waves,`64a53fa7af55`; Uganda assets116,808 rows,one column,eight waves,`9202ed542a14`. The checker reached its installed-code/config-outside-checkout assertions, enforced conda make/sh/find resolution, and reported126 Make prerequisite parquets with zero find errors. No MISMATCH or RAISED result was emitted. Expected values and source are in `expected-native.json`; checked observations are in `native-validation.json`.

Auxiliary development-push38746073618 and PR38746082964 used the same implementation SHA but skipped credentialed data by workflow conditions. They are not substituted for this successful manual run. Docs, DVC lock and lock-refresh jobs skipped by condition are not counted as passes; root-owned documentation validation is separate.

## Private log identity

| Job / job ID | Bytes | SHA256 |
|---|---:|---|
| data-tests / 116460137406 | 457,055 | `6cfa35d674a21ed1d54f3accc8ecd191dd00f7f28d092fe68ad45baaa4c9ca10` |
| unit-tests / 116460137477 | 85,198 | `40595c364d120e1c15a495edff1756534e626b83708d7abc414e87bc4fef68ee` |
| windows-smoke / 116460137580 | 96,039 | `fab6c1b927dc80ae0a01d44117d4b61741b538119f104537626e58dad97008f4` |
| windows-script-build / 116460137762 | 138,250 | `ddfee3c9f870d1a5aa9ec19a764e79a47d90f005a4af04913491c3d3a44fd58e` |
| dvc-remote-reachability / 116460138161 | 20,824 | `7410a2855d47ab735753913eebaa27927779626506e7787f81fa884a31a443b1` |

All completed logs were fetched only after their jobs became terminal. Raw warning lines can include survey identifiers and are intentionally absent from this aggregate archive. The source-only analyzer reads those private logs and emits counts/identities only. No new dispatch, cancellation, GitHub write or cache operation was performed. The prepared make-release launcher is separate and has not been executed by this agent.
