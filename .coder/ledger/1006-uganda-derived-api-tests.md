# Prior-Art Ledger -- GH #1006 Uganda derived API tests

Approved task ledger at `.coder/ledger/1006-uganda-derived-api-tests.md`. Root approved the concrete private diagnosis/proposal on 2026-10-21; this ledger is committed before test edits. Runtime/diagnosis pin: `f8d236694c26fe9e709c5548f02c70f74ea72c33`, branch `fix/1006-uganda-derived-api-tests-20261021`. Inherits STANDING.md; references below are checked at this pin.

Search tier: ripgrep + git + AST/assertion-line tracing; existing local tests and live GH #1006 body/comments read. The issue is open with no comments at inspection. No new production estimator, loader or test-package import scheme is needed.

## §1 Task, restated

Repair test coverage in `test_food_prices_dtype.py`, `test_panel_attrition.py` and `test_uganda_tables.py`. Exercise runtime food/roster-derived API tables from actual cached prerequisites rather than nonexistent derived parquets; validate canonical float payloads and distinguish default cluster v from explicitly requested Region market m. Preserve actual attrition/coverage assertions, production behavior, #1062's separate lane, scientific fixtures and source decisions. The approved boundary is test-only; the separate source/runtime decisions remain unchanged.

## §2 Existing machinery

| Machinery | Checked anchor | Existing evidence | Decision |
|---|---|---|---|
| Runtime derived dispatch | country.py:6069 `_FOOD_DERIVED`; :6076 `_ROSTER_DERIVED`; :6220-6389 dispatch | food-prices-units tests; current real API probe | Reuse table membership/source dispatch; no competing production resolver |
| Float regression predicate | pandas `is_float_dtype`, already used at test_food_prices_dtype.py:76,93,111,125 | Actual Price=Float64, Quantity=float64; controlled positives accept both representations | Reuse; add canonical-column presence before predicate |
| Numeric source transforms | transformations.py:3088 `food_quantities_from_acquired`; :3207 `food_prices_from_acquired`; :641 `roster_to_characteristics` | test_food_prices_units_kwarg.py synthetic fixture/tests | Reuse; no persistence or arithmetic changes |
| Default geography / requested market | country.py:6150 default market=None; :3221 `_add_market_index` | test_sample.py:600 HH Region fallback; test_add_market_index_dedup.py:54 | Reuse API behavior; test default and requested modes separately |
| Canonical identities | Country `_finalize_result` :3815, `_join_v_from_sample`; Uganda CONTENTS:45-293 | Real retained keys/measurements; existing Uganda ID/market tests | Reuse; never require v to be stable across waves or nonnull everywhere |
| Attrition | local_tools.py:3766 `panel_attrition`; test_panel_attrition.py:24-79 | Both original real-data test bodies pass when their stale guards are bypassed; six faults rejected | Preserve helper, threshold=50, upper-triangle, adjacent-overlap and positive-diagonal assertions |
| Source-cache availability | `paths.data_root`; current local guard helpers in all three scoped files | Six prerequisite country caches are hash-fresh with eight-wave coverage | Extend existing guards to source tables, not derived outputs or in-tree files |
| Credential/absence policy | tests/conftest.py:41 `aws_creds_available`, marker collection hook | Current held run 5 pass/10 skip; empty and credentialless controls | Reuse import-free requires_s3 markers for real-data cases; no catch-all skip |
| Safe test imports | tests/test_tests_package_not_shadowed.py; tests/conftest.py helper docstring | Both `from conftest ...` and `from tests.conftest ...` are documented traps | Avoid both; pytest markers/fixtures or existing by-file inspection only |
| Scoped cache restoration | test_gh866_stale_country_parquet_not_laundered.py:204; test_cache_hash_invalidation.py:170 | Four present/absent × success/exception private controls pass | Reuse MonkeyPatch context + data_root.cache_clear entry/finally pattern; physical copies only for this work |
| Actual wave recursion regression | tests/test_wave_getattr.py:67 | Direct wave.data_scheme tests exist | Do not claim a successful derived-food call exercises a wave fallback |

## §3 Definitions and documented contradictions

- Per STANDING.md §3/§4 and CLAUDE.md:696, the four derived tables are runtime returns, not registered/persisted outputs. Normal calls leave all four derived parquet names absent in the held reproduction.
- Canonical fields are Price/Quantity/Expenditure, not old wide `market`, `unitvalue_*` or `quantity_*` columns. Current observed floating dtypes include nullable Float64 and NumPy float64. Root's requirement is non-vacuous canonical float checking; do not narrow to only one representation.
- `market=None` returns v on the four derived tables; `market='Region'` replaces v with m. `_add_market_index` uses household sample Region first, cluster metadata as fallback, and drops unresolved m. Region calls need not retain every bare row.
- **CONTRADICTION:** global `data_info.yml:56` still declares `household_characteristics: (t, v, i, m)` under Index Info, while current Country market=None returns `(t,v,i)` and explicit Region returns `(t,m,i)`. Report this; do not alter production schema in #1006.
- **CONTRADICTION:** Uganda CONTENTS:253 says sample v/Region is "complete for every household"; its own :270 qualification records three missing 2019-20 Regions. The 2009 synthetic-v and missing-v tail, wave-scoped/redefined v, rotation and refresh cohorts prohibit blanket geography/monotonic-population assumptions.
- **CONTRADICTION:** current attrition test comment says diagonals "should be monotonically non-decreasing", but its executable assertions only require positive diagonals. Both real diagonals are non-monotone. Keep executable assertions; correct the comment, do not add an unsupported monotonicity requirement.
- The f8 CONTENTS historically says extra unit-wave columns are "unused at runtime". #1077's separately reviewed documentation qualifies build decoding versus query-time use. Do not overwrite/reopen that lane here.
- Region HC currently loses `columns.name='sex_age'`; retained payloads and dtypes are exact. This metadata difference is reported separately, not disguised as an exact whole-frame match or repaired in this test task.

## §4 Invariants

- Only the three test files plus the task ledger/safe evidence are proposed. No production/country config, shared test marker policy, fixture/golden, dependency, baseline or nutrition/FCT arithmetic change.
- Never manufacture a derived parquet, change persistence, manually stamp freshness or use assume_cache_fresh to pass readiness.
- A missing canonical column or object/string payload must fail. Do not condition assertions on membership in a historical optional-column list.
- Preserve every existing attrition value assertion. Newly surfaced failures must be diagnosed, never removed, repinned or tolerance-broadened for green tests.
- Use actual cached food_acquired/household_roster plus sample prerequisites. Registered income/shocks/cluster_features remain real API inputs. No in-tree cache fallback under the current read policy.
- Real-data tests retain explicit requires_s3 policy and precise source-readiness skips. Pure dtype fault controls run without data/credentials; never broadly skip an API exception after prerequisites are present.
- Default-v tests must not assume a cross-wave cluster identity or fill source geography. Explicit Region comparisons preserve every retained non-geographic key, measurement and payload dtype; unresolved geography can remove rows. Do not add equal-count, universal Region or monotonic-diagonal promises.
- Preserve scientific nutrition/fixture decisions as instructed. This task does not reopen historical approval qualifications, CPU fingerprint variants, nominal kg assumptions or #1062's sibling routing.

## §5 Reuse decision

| Need | Decision | Reason |
|---|---|---|
| Derived-source readiness | Extend existing local guard functions using Country's existing derived table dictionaries | No new production discovery/helper module; no fragile test-package imports |
| Canonical dtype validation | Small test-local required-column + is_float_dtype assertion, reused by two API cases and fault controls | Existing predicate is right; conditional old-column loops are the defect |
| Default/Region coverage | Extend current table parameter cases, reuse pandas per-column comparisons and existing market invariants | Existing row/key/value checks can become discriminating without a second geography algorithm |
| Attrition | Reuse unchanged bodies | Real data passes and six faults fail |
| Legacy fallback assertion | Move its nonempty/i/j assertions into the already-covered food-expenditure default case; retire misleading name | Actual recursion coverage exists in test_wave_getattr; a derived call is not a fallback proof |
| Cache cleanup | Remove unused backup/unlink helpers; private external validation roots and proven restoration pattern | No test needs to remove a derived or ambient cache |

## §6 Approved boundary and open questions

Root approved the concrete three-file test-only proposal and validation matrix in the task-private PROPOSAL.md on 2026-10-21. Specifically: retire the two obsolete derived-parquet dtype tests in favor of canonical API cases plus ten pure controls; replace default-m/xfail debt with default-v and explicit Region cases; preserve both attrition bodies and the scientific baseline; merge the redundant fallback assertions into the stronger existing food-expenditure case and remove unused deletion helpers. No global conftest/shared fixture change is proposed.

The prior-art-ledger skill's Phase 1 says "Then stop and surface the ledger to the user before implementing" (`/global/home/users/ligon/.sucoder/skills/prior-art-ledger/SKILL.md`); the root also explicitly requested this approval boundary. The read-only diagnosis surfaced this ledger and stopped; root approval satisfies that handshake. No further boundary question remains before the approved implementation.

-- Sue
