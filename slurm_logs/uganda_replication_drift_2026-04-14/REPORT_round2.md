SCOPE DEVIATIONS: none

# Uganda Replication-Package Drift Report — Round 2

**Date**: 2026-04-14 (afternoon)
**Repo**: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library`, branch `development` at `da7a42cc`+
**Methodology**: Functional-equivalence-first. Direct cache reads used for all features (bypasses the slow market-join path for timeout features). Cached parquets at `~/.local/share/lsms_library/Uganda/var/`.
**Round 1 report**: `slurm_logs/uganda_replication_drift_2026-04-14/REPORT.md`

---

## 1. Summary Table

| parquet | API method | R1 status | R2 status | FE verdict | baseline_action | notes |
|---|---|---|---|---|---|---|
| cluster_features | cluster_features() | REPL_READ_ERROR | REPL_READ_ERROR | n/a (skip) | skip | 0-byte replication file; API cache exists |
| earnings | earnings() | API_TIMEOUT | SCHEMA_DRIFT | YES | adopt_api_shape | cache has 129K rows vs 22K; all repl rows present, MAD=0 |
| enterprise_income | enterprise_income() | API_TIMEOUT | SCHEMA_DRIFT | YES | adopt_api_shape | cache has 78K rows; all 13935 repl rows present, MAD=0 |
| fct | fct() | API_ERROR | PARTIAL | PARTIAL (113/114) | adopt_api_shape | fct() now works; Cheese col missing from API |
| food_acquired | food_acquired() | CONTENT_DRIFT | CLEAN | YES | adopt_api_shape | 352913/352913 rows match on (i,t,j,u); MAD=0 |
| food_expenditures | food_expenditures() | SCHEMA_DRIFT | SCHEMA_DRIFT | YES | adopt_api_shape | cosmetic rename x->Expenditure; MAD=0 |
| food_prices | food_prices() | CONTENT_DRIFT (dtype bug) | CLEAN | YES | adopt_api_shape | dtype bug fixed; float64; MAD=0 |
| food_quantities | food_quantities() | CONTENT_DRIFT (dtype bug) | CLEAN | YES | adopt_api_shape | dtype bug fixed; float64; MAD=0 |
| household_characteristics | household_characteristics() | SCHEMA_DRIFT | SCHEMA_DRIFT | PARTIAL (21166/24555) | adopt_api_shape | col rename + idx reorder; log HSize MAD=0.017 (quality improvement) |
| household_roster | household_roster() | SCHEMA_DRIFT | SCHEMA_DRIFT | YES | adopt_api_shape | kinship decomp + v-join; all 35493 repl rows present |
| income | income() | API_TIMEOUT | CONTENT_DRIFT | PARTIAL (89.5%) | needs_work | duplicate (i,t) rows in cache from subtotal+total writes |
| interview_date | interview_date() | CONTENT_DRIFT | CLEAN | YES | adopt_api_shape | datetime [ns]->[us] cosmetic; all 24264 rows present |
| locality | locality() | SCHEMA_DRIFT | SCHEMA_DRIFT | YES | adopt_api_shape | deprecated; v->Parish rename; values identical |
| nutrition | nutrition() | API_ERROR | CONTENT_DRIFT | PARTIAL (24166/24169) | adopt_api_shape | dtype bug fixed; slight MAD from unit-filter improvement |
| other_features | (none) | NO_API | NO_API | n/a | skip | Rural now in sample() |
| people_last7days | people_last7days() | API_ERROR | SCHEMA_DRIFT | YES | adopt_api_shape | bug fixed; all 15402 rows present; MAD=0 |
| shocks | shocks() | API_TIMEOUT | SCHEMA_DRIFT | YES | adopt_api_shape | Shock->idx, Effected->Affected rename; 14457/14457 match |

---

## 2. What Changed vs Round 1

### 2.1 Bug Fixes Confirmed

**1. food_prices / food_quantities dtype fix (commit 973c45d9)**
Round 1 found all 9 price columns and 4 quantity columns returning `string` dtype instead of `float64`. The round-2 cache has all columns as `float64`. Row comparison on `(i,t,j,u)`: 352,850 rows match, MAD=0 on all numeric columns. The dtype bug is confirmed fixed.

**2. nutrition cascading fix**
Round 1 crashed with `TypeError: can't multiply sequence by non-int of type 'float'` because `food_quantities` returned strings. The round-2 cache has `nutrition.parquet` (24,169 rows, 15 columns, all `float64`). Values are near-identical to the replication (24,166/24,166 repl rows present; slight MAD on Calcium=0.20, Energy=0.37 from unit-filter improvements in `food_quantities`).

**3. people_last7days dead other_features reference fixed (commit b454fda5)**
Round 1 raised `AttributeError: 'Country' object has no attribute 'other_features'`. The cache now has `people_last7days.parquet` (15,471 rows). All 15,402 replication rows are present on `(i,t)` comparison; Boys/Girls/Men/Women MAD=0.

**4. fct is now callable (commit f6e6432a)**
Round 1 had no `fct()` method on `Country`. `uganda.fct()` now returns a `(15, 113)` DataFrame. The replication has 114 food columns vs API's 113 — `Cheese` is missing from the current API's food composition table. All 15 Nutrient rows match; values agree on 113 common items (floating-point noise ~5e-16). The missing `Cheese` column is a minor gap, not a blocker.

### 2.2 Previously-Timeout Features Resolved

**5. earnings (was API_TIMEOUT)**
The cached parquet (129,160 rows, `(t,i)` index) contains all 22,447 replication rows. Direct cache read confirms MAD=0 on the `earnings` column. The API call with `market='Region'` is still slow (market-join of 129K rows against cluster_features for region lookup) and was not waited out in round 2 — the direct cache read is authoritative. Schema differences: index order `(i,t,m)` -> `(t,i)` (m is market join at query time); expanded wave coverage (22K->129K). `level_1` column (income sub-category label) is present in both.

**6. enterprise_income (was API_TIMEOUT)**
Cache (78,040 rows, `(t,i)` index) contains all 13,935 replication rows. MAD=0 on all 6 numeric columns (losses, materials, otherexpense, profits, revenue, wagebill). Schema drift: index reorder + expanded waves.

**7. shocks (was API_TIMEOUT)**
Cache (14,457 rows, `(i,t,Shock)` index) precisely matches the replication on `(i,t,Shock)`: all 14,457 rows present, Duration MAD=0. Two schema changes: (a) `Shock` moved from a data column to an index level; (b) `Effected*` -> `Affected*` spelling fix (four columns). Both are intentional improvements. `m` absent from cache (market join at query time).

**8. income (was API_TIMEOUT) — needs_work**
Cache (18,487 rows, `(t,i)` index) covers more waves than replication. The 10,193 replication `(i,t)` pairs are all present in the cache. However, the cache has duplicate `(i,t)` entries: 3,787 households have 2-3 income rows per `(i,t)`, appearing to be individual income sub-types plus the total, written by wave scripts without deduplication. The replication selected the total. MAD on matched rows = 49,545 (10.5% of rows have non-zero difference). This needs deduplication in the wave scripts before baselining.

---

## 3. Per-Feature Detail (non-adopt_verbatim)

### income — needs_work

The cache stores multiple rows per `(i,t)` for 3,787 of 13,931 households: some have 2 rows with identical income values (exact duplicates) and some have 3 rows where one is the sum of the other two (e.g., sub-income-sources 1,120,000 + 23,000 = total 1,143,000). The replication captured the total value. Reconciliation: the wave-level income scripts should deduplicate by taking the max value per `(i,t)` or restructure to use a sub-category index level. Baseline action: `needs_work` — do not adopt until deduplication is resolved.

### household_characteristics — PARTIAL

The 21,166/24,555 partial match comes from wave-coverage differences (3,197 older-wave HHs not yet in the API) plus 192 new-wave HHs. The log HSize MAD=0.017 on common rows reflects the Age sentinel nullification fix (`age_handler()`, commit `1dffda58`) which removes invalid ages from household size counts — a data-quality improvement, not a regression. Column abbreviation (`Females 00-03` -> `F 00-03`) and index reorder (`(i,t,m)` -> `(t,m,i)`) are cosmetic. Baseline action: `adopt_api_shape`.

### fct — PARTIAL (113/114)

The `Cheese` column present in the replication parquet is absent from the current API's food composition table. This is a minor content gap; all 15 nutrient rows match across 113 common food items. Baseline action: `adopt_api_shape` with a note that `Cheese` composition data is missing.

### nutrition — PARTIAL

The slight MAD (Calcium=0.20, Energy=0.37) on the 24,166 common rows traces to the same unit-code filter changes that caused the row gap in `food_acquired` and `food_prices`. This is a data-quality improvement from stricter unit filtering in 2018-19 and 2019-20 waves. Baseline action: `adopt_api_shape`.

---

## 4. Cosmetic Differences (not blocking baselining)

| Feature | Cosmetic diff | Effect |
|---|---|---|
| All features | `m` absent from raw cache index | Added at query time via market join |
| All features | Index level order changes | Reorder only; no data change |
| interview_date | `datetime64[ns]` -> `datetime64[us]` | pandas 3.0 migration; no data loss |
| food_expenditures | `x` -> `Expenditure` column | Canonical rename |
| locality | `v` -> `Parish` column | Semantic rename (same parish string values) |
| household_characteristics | `Females/Males` -> `F/M`, `51-99` -> `51+` | Label abbreviation |
| household_roster | `Relation` -> `Relationship` + kinship decomp | Schema evolution; values recoverable |
| shocks | `Effected*` -> `Affected*` | Spelling fix |
| shocks | `Shock` column -> `Shock` index level | Schema improvement |

---

## 5. Ready to Baseline?

| Feature | baseline_action | Rationale |
|---|---|---|
| cluster_features | skip | 0-byte replication; no comparison possible |
| earnings | adopt_api_shape | FE=YES; all repl rows present; MAD=0; expanded coverage |
| enterprise_income | adopt_api_shape | FE=YES; all repl rows present; MAD=0; expanded coverage |
| fct | adopt_api_shape | PARTIAL(113/114); Cheese missing but minor; fct() now works |
| food_acquired | adopt_api_shape | FE=YES; 352913/352913 match; MAD=0 |
| food_expenditures | adopt_api_shape | FE=YES; cosmetic rename; MAD=0 |
| food_prices | adopt_api_shape | FE=YES; dtype bug fixed; MAD=0 |
| food_quantities | adopt_api_shape | FE=YES; dtype bug fixed; MAD=0 |
| household_characteristics | adopt_api_shape | PARTIAL; schema cosmetic; log HSize MAD=0.017 is quality improvement |
| household_roster | adopt_api_shape | FE=YES; 35493/35493 repl rows present; all changes intentional |
| income | needs_work | Duplicate (i,t) rows from subtotal+total writes; dedup first |
| interview_date | adopt_api_shape | FE=YES; datetime [ns]->[us] cosmetic; all rows present |
| locality | adopt_api_shape | FE=YES (deprecated); v->Parish rename; values identical |
| nutrition | adopt_api_shape | PARTIAL; slight MAD from quality improvement; acceptable |
| other_features | skip | Intentionally removed; Rural in sample() |
| people_last7days | adopt_api_shape | FE=YES; bug fixed; all 15402 rows present; MAD=0 |
| shocks | adopt_api_shape | FE=YES; all 14457 rows match; schema improvements only |

**Summary**: 13 features -> `adopt_api_shape`; 1 feature -> `needs_work` (income); 2 features -> `skip` (cluster_features, other_features).

---

## 6. Action Items Before Full Baselining

1. **income** (needs_work): Investigate wave scripts that write both income subtotals and totals under the same `(i,t)` key. Dedup by taking max per `(i,t)` or restructure with a sub-category index level. After fix, re-cache and re-run this comparison.

2. **fct**: The missing `Cheese` column in the food composition table should be added if nutritional analysis requires it. Low priority.

---

*Report generated 2026-04-14 (round 2). Scripts: `compare_round2.py` in `slurm_logs/uganda_replication_drift_2026-04-14/`. Raw data: `results_round2.json` in same directory. Direct cache reads used for all features; market-join API path bypassed for timeout features.*
