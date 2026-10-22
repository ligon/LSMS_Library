# GH #1006: Uganda runtime-derived API tests

The candidate repairs test debt in exactly three test files. It changes no production code, schema, country configuration, fixture, nutrition/FCT input, baseline or persistence behavior. The ledger was committed first after root approved the source-safe diagnosis and concrete boundary. Runtime validation is pinned to `1eb9bb377157c78f74962c7a9de59807d446b239`, independently based on release `f8d236694c26fe9e709c5548f02c70f74ea72c33`; later commits update only the ledger and safe evidence. This candidate stops for independent review, with no GitHub writes or integration-branch movement.

## What failed before and what is exercised now

The unmodified three-file selection gave 5 passed and 10 skipped on actual warm prerequisites with no derived parquets. Assertion-line tracing found zero dtype predicates in its two passing dtype API tests: their old wide column names were absent from canonical Price/Quantity output. Twelve old missing-column/object/string injections across API and synthetic cache-read surfaces incorrectly passed. Derived-file guards hid both attrition cases and the outdated default-m table assertions; three non-strict migration xfails would further mask those assertions.

The new dtype cases call the ordinary API from food_acquired/sample prerequisites, require canonical Price or Quantity and require a floating dtype. Ten pure controls reject the six malformed payloads and accept both NumPy float64 and nullable Float64. Normal derived calls never need or create a derived parquet. The table cases retain all seven registered/derived API checks and explicitly distinguish default v from requested `market='Region'`; Region must preserve every retained non-geographic key, column payload dtype and measurement, and cannot multiply rows. Registered shocks/income/cluster checks remain nonempty as before. The misleading fallback test's nonempty/i/j checks are folded into the real food-expenditure API case; existing Wave recursion tests still exercise that distinct path. Unused backup/unlink helpers are removed.

Both attrition numerical test bodies and all three numerical/loader helpers are AST-identical to f8. Threshold 50, upper-triangle checks, positive adjacent overlaps and both positive-diagonal loops remain intact. Only source readiness and the false monotonic-diagonal comment change. Both actual matrices match f8 exactly and six injected matrix faults fail the unchanged bodies. There was no newly exposed value/coverage failure to weaken or repin.

## Executed validation

| Selection/control | Result |
|---|---|
| Three changed test files | 25 passed, 0 failed, 0 skipped: 15 real API cases + 10 pure controls |
| Existing units/Wave/market-dedup/2009 market-fallback regressions | 41 passed, 0 failed, 0 skipped |
| Empty private cache | 10 pure passed, 15 data skipped; no payload files acquired |
| Credentialless warm private cache | 10 pure passed, 15 data skipped |
| Tracked canonical dtype helper | Six malformed payloads rejected, four float representations accepted |
| Unchanged attrition bodies | Six matrix faults rejected |
| Tracked default/Region helpers | Eight index/duplication/new-key/dtype/measurement faults rejected |
| API errors after source readiness | Four propagated; none converted into skips |
| Environment present/absent x success/injected exception | Four exact restoration checks passed, root memo cleared |
| Fresh-process public API versus saved f8 references | Nine full frames and attrs exact, plus two exact attrition matrices |

The supporting selection executes the existing 2009 HC household count of 2,975, food count of at least 2,929 and no-missing-market assertions without changing their thresholds. This is bounded held-prerequisite verification, not a full source rebuild or full suite.

The five default references are food_prices, food_quantities, food_expenditures, household_characteristics and income; four additional references request Region on the derived tables. Exact comparisons include values, row/index order, labels, index and payload dtypes, axis metadata and attrs, with no normalization. Default row counts remain 348,181 / 355,487 / 247,049 / 24,364 / 14,740. Region drops 36 / 37 / 31 / 67 rows for prices/quantities/expenditures/HC, respectively. These are observed exclusions from existing geography resolution, not new count pins or a claim that every bare observation has Region.

HC already changes `columns.name` from `sex_age` to None when Region is requested. That mode-to-mode metadata difference is separately reported; each entire mode remains exact to its own f8 reference. The tracked Region assertion compares every retained Series exactly without discarding measurement or dtype changes.

## Country decisions and reported contradictions

Uganda CONTENTS was read first, including live TODO/WAITING/LOGBOOK entries. Relevant idiosyncrasies include changing EA code renderings, synthetic GPS-bin v in 2009, later parish identities, the missing-geography tail and refresh cohorts. No cross-wave stable-v or monotone-population assumption is added. Unknown versus Value, accepted unlabeled native-unit codes and existing off-map drops remain untouched; #1062 routing and the separately reviewed #1077 unit decisions are not imported or overwritten in this f8-based lane.

The documentation contradictions remain explicit evidence:

- CONTENTS:253 says sample geography is "complete for every household"; its own :270 caveat records three missing 2019-20 Regions.
- Its rotating-sample account is qualified by the quoted DDI statement "This same sample was used" in the source-design discussion. Actual attrition diagonals need not increase.
- The old test comment said "monotonically non-decreasing" although both loops only assert positive diagonals. The candidate corrects the comment and preserves the loops.
- Global data_info.yml:56 still declares HC `(t, v, i, m)`, while the current Country default returns `(t,v,i)` and explicit Region returns `(t,m,i)`. No schema expansion is included.
- CONTENTS says the extra unit-wave columns are "unused at runtime"; the separate #1077 review qualifies build decoding versus query-time use. This lane does not edit that country document.
- The historical CONTENTS claim that `test_uganda_tables.py` "DELETES generated parquets" describes backup/unlink helpers that were no longer called by these f8 test bodies. They are removed here; baseline and candidate runs both create/delete zero derived parquets.

Live country caveats about phantom cluster outer-merge rows, absent GPS, null-key crop drops, producer-unconfirmed sales values, panel weights and recall windows remain outside this task. Nutrition/FCT scientific decisions, historical holds and narrow approvals/CPU variants are preserved byte-for-byte.

## Isolation and conservation

The fresh physical private copy uses 170 manifest-pinned f8 Uganda prerequisite parquets and 581 verified held L1 blobs. All six checked country prerequisites are hash-fresh and cover eight waves. No derived parquet or nutrition/FCT cache was staged. Normal API calls, fresh-process comparisons, tests and root-restoration controls write no prerequisite parquet; bytes and mtimes remain unchanged. The empty root gains no payload files. No source acquisition, DVC CLI, shared-cache write/clear/promotion, hash bypass/stamp or in-tree parquet/raw file is used.

Mounted image Python runs with matching cwd/PYTHONPATH/countries-root assertions, private data-root assertions, active connect/connect_ex method guards plus `unshare -rn`, numerical/build/make threads one, and three xdist workers under loadfile. Full frames, IDs, warning examples and raw logs remain private. All 14,877 original tracked paths outside the three tests, including the complete library/config and every fixture, are byte-identical to f8; all 170 prerequisites and 581 raw copies still match both their diagnosis references and quiescent sources. The safe aggregate details and commands are in validation.json and REPRODUCE.md. Final containing-commit identity and clean-tree proof are recorded in the private freeze proof and handoff.

-- Sue
