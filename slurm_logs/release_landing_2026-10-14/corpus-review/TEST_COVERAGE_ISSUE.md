UNFILED DRAFT — no GitHub mutation performed.

Proposed title: Uganda derived-table tests skip successful builds and retain obsolete index/dtype assertions

Proposed body:

A completed cold corpus at `7fa05179c8519f44ea329ba6387f5d6d6441a8ea` successfully returned Uganda `food_expenditures`, `food_prices`, `food_quantities` and `household_characteristics`. Their normal API routes derive from cached food/roster inputs and return without writing a derived country parquet (`country.py:5414-5544`). A later credentialed test selection on a private copy of that corpus at `4f1f044bb9b3079e7f427898f77627ad1503ec5e` still skipped ten tests in three files because their guards require those derived parquets.

| File | Coverage defect |
|---|---|
| `tests/test_food_prices_dtype.py` | Two cache tests skip absent derived parquets. Two API tests execute, but condition their dtype assertions on old wide columns (`market`, `unitvalue_*`, `quantity_*`). Current API frames have `Price` or `Quantity`, so those loops assert nothing about dtype. |
| `tests/test_panel_attrition.py` | Both tests skip because they require `household_characteristics.parquet`, although the API successfully derives this table. |
| `tests/test_uganda_tables.py` | Six tests skip behind derived-cache guards. Index checks still require default `m`; the current API defaults to `market=None`, returns `v`, and attaches `m` when requested. Three food cases carry non-strict migration xfails; household-characteristics assertions are unmarked. |

Recorded successful outputs: expenditure 247,049 x 1 (`Expenditure`, Float64); price 348,181 x 1 (`Price`, Float64); quantity 355,487 x 1 (`Quantity`, float64); household characteristics 24,364 x 15, index `(t, v, i)`. These values identify the reviewed run; they are not proposed universal exact-count assertions.

Evidence: `control/runtime/corpus/Uganda/results.jsonl`, `control/full-suite/cache-coverage-summary.json`, and `logs/cache-coverage/ordinary.xml` (SHA-256 `29d760dd913201b3c1dfddfd50064ca630b6cb13cf47fc7f4fe211a224b9da0b`). Independent artifact/source audit: `control/countries/corpus-review/{REVIEW.md,validation.json,audit_evidence.py}`. The ordinary selection had 15 passes and 18 skips: the other eight skips are separate raw-weight tests on four countries whose delivered sample has no weight column.

Suggested scope and acceptance:

1. Exercise the existing normal derived API using real prerequisite availability in a private cache root; do not manufacture derived parquets merely to pass a guard or change production persistence behavior.
2. Decide each test's intended contract explicitly: default `v` identity versus requested `market='Region'` and `m`. Replace obsolete non-strict migration xfails with discriminating assertions for that contract, preserving row/index/value checks.
3. Assert the canonical `Price` and `Quantity` columns exist before checking their numeric dtype. A controlled object/string result or a missing canonical column must fail; an unrelated wide-column list must not make the test vacuous.
4. Run the affected tests with warm prerequisite caches and no derived parquets. Record executed assertions and remaining source-based skips; retain deliberate absence/credential behavior for environments without data. Verify normal and exceptional cleanup leaves ambient roots untouched.
5. Preserve the Uganda nutrition fixture and its scientific hold. This issue does not change survey numbers, broaden tolerances, or explain the historical #941 CI failure.

Dedup rationale (read-only search, saved in `issue-search.json`): exact test filenames, `panel_attrition`, `m-in-index`, `household_characteristics_has_m_index`, `_has_cached_table`, and combinations of derived/parquet/skip and food_prices/dtype found no issue owning this exact test debt. [#907](https://github.com/ligon/LSMS_Library/issues/907) names these files for credential-skip timing; this persists with credentials and real prerequisite data. [#218](https://github.com/ligon/LSMS_Library/issues/218) owns legacy harmony/script retirement. Closed [#246](https://github.com/ligon/LSMS_Library/issues/246) repaired `test_uganda_api_vs_replication.py` and the obsolete producer, not these remaining test guards/assertions. Cross-link those issues rather than treating this as evidence their completed work failed. [#941](https://github.com/ligon/LSMS_Library/issues/941) remains a distinct historical sequential CI investigation.
