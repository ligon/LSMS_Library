Read-only diagnosis complete for GH #1006 at frozen f8. Proposed boundary: **three test files, a per-task ledger and source-safe evidence only**. No tracked edit has been made. Root approval is requested before implementation.

The live issue body and zero comments are saved in issue-1006.json. Its intended API contracts are confirmed by source and real held data. The current test selection gives **5 passed, 10 skipped, 0 failed**, with credentials available, six current prerequisite caches covering all eight waves and no derived parquet. Runtime assertion tracing proves the two dtype API passes execute only their nonempty assertions: zero dtype predicates run. The other three passes check only nonempty registered shocks/income/cluster tables. No parquet changes during that selection.

| Public derived API | Default shape / payload | Explicit Region rows | Bare rows removed |
|---|---|---:|---:|
| food_prices | 348,181; Price Float64; i,t,v,j,u,s | 348,145 | 36 |
| food_quantities | 355,487; Quantity float64; i,t,v,j,u,s | 355,450 | 37 |
| food_expenditures | 247,049; Expenditure Float64; i,t,v,j,s | 247,018 | 31 |
| household_characteristics | 24,364; t,v,i; 14 integer demographics plus float log HSize | 24,297 | 67 |

These are observations, not proposed universal count pins. Region returns m instead of v. Every retained non-geographic key and measurement, column order and payload dtype is exact; all retained rows with a known household Region carry that Region. One existing metadata difference is retained and reported: HC column-axis name changes sex_age -> None through the market merge. The full default/Region frames are private pickle evidence; none was written as a derived cache parquet.

The two original attrition test bodies both pass directly on actual 24,364-row HC and 14,740-row income outputs. Both matrices are upper triangular, every adjacent overlap is positive, all diagonals are positive and there are zero threshold-50 cross-table zero/large-overlap violations. Both diagonals are non-monotone; the old comment promises something the code never asserts. Six injected matrix faults fail the existing unchanged bodies. The separate existing 2009 Region-HC count pin of 2,975 also holds. There is no newly exposed attrition value/coverage failure to weaken or defer.

All four derived backfill cases fail their old default-m assertion when source readiness exposes the body; the three food cases would be hidden by non-strict xfails. The standalone HC m test is also guarded out before its equivalent obsolete assertion. All twelve process-local injections across old API/cache dtype functions -- missing canonical field, object numeric, string numeric for prices and quantities -- incorrectly pass. A required-column + is_float_dtype check rejects all six distinct bad payloads and accepts both float64 and nullable Float64 for each column. Six index/measurement mutants are rejected by the proposed default/Region contract checks.

Concrete implementation proposal:

1. `tests/test_food_prices_dtype.py`: remove the obsolete wide column lists and two tests that read derived cache files. Parameterize the normal public API over `(food_prices, Price)` and `(food_quantities, Quantity)`. Require a nonempty DataFrame, the canonical field, then the existing float-dtype predicate. Gate actual data cases on cached food_acquired/sample, never food_prices/food_quantities. Retain requires_s3 on real-data cases, rather than at module level, so ten pure fault/positive controls can run in the credential-free unit job: 2 columns × 3 bad shapes/types rejected, and 2 columns × 2 supported float dtypes accepted. No read/write of derived parquets or change to numeric production code.

2. `tests/test_panel_attrition.py`: change only prerequisite readiness and misleading commentary. HC readiness means household_roster plus sample; consistency additionally requires the real income table. Drop the in-tree fallback. Keep `_attrition_matrix`, `_is_upper_triangular`, `_nonzero_adjacent`, threshold=50 and both positive-diagonal loops unchanged. Keep the two real-data cases requires_s3. Replace the inaccurate monotonic-diagonal comment with its actual positive-diagonal meaning. The six existing-body negative controls remain validation evidence; no new estimator or redundant matrix helper is needed.

3. `tests/test_uganda_tables.py`: resolve readiness through the existing Country._FOOD_DERIVED / _ROSTER_DERIVED memberships to actual cached inputs, plus needed sample metadata; registered tables retain their own cache prerequisites. Only data_root paths count. Rename the misleading makefile-backfill test to a normal cached-source API test, retaining seven table cases. Remove all three non-strict migration xfails. On the four derived defaults require named t/v/i, absence of m and their food axes where applicable; keep registered shocks/income/cluster nonempty checks without inventing a v requirement for income. Parameterize the old HC m case over all four derived tables and request market='Region' explicitly: require m, absence of v, unique keys, no row multiplication and exact retained non-geographic keys/measurements/payload dtypes. Allow unresolved geography to remove rows; do not repin counts or suppress value changes. Fold the old fallback test's nonempty/i/j checks into the stronger food-expenditure default case and retire its false claim that a normal derived return exercises wave fallback. Real wave.data_scheme recursion tests already exist in test_wave_getattr.py. Remove the unused backup/unlink cleanup helpers and their imports; no scoped test needs filesystem deletion.

Existing machinery to reuse is inventoried in [ledger-draft.md](ledger-draft.md): runtime dispatch, data_root, is_float_dtype, panel_attrition, existing sample/market/wave regression tests, import-free credential markers and the proven MonkeyPatch/cache_clear restoration pattern. No imports from conftest or tests.conftest, new shared helper package, global fixture or shared marker-policy edit is proposed. Small readiness edits extend the local helpers already in the files. API exceptions after readiness remain failures; no broad exception-to-skip path is added.

Any local data fixture is limited to readiness checks and constructing the ordinary Country handle. It must not write a fixture parquet, run a legacy country compiler, purge/backup an output, or create a derived target. Fault controls use in-memory frames and scoped MonkeyPatch restoration. Cache-root isolation for validation remains in the task-private physical-copy runner; its success and failure cleanup has already been exercised without touching the outer source root.

Readiness shape, grounded in existing production membership rather than another registered table system:

```python
if table in ll.Country._FOOD_DERIVED:
    source = 'food_acquired'
elif table in ll.Country._ROSTER_DERIVED:
    source = 'household_roster'
else:
    source = table
# Check data_root('Uganda') / 'var' / f'{source}.parquet'.
# Derived/default-v cases additionally need sample; explicit Region cases
# also need cluster_features for the documented fallback.
```

This is a test prerequisite check, not an alternative loader or freshness policy. Let the normal API perform its own hash and coverage checks. Do not trust different-head parquets, stamp caches or silently skip genuine errors after the source is present. A missing source cache produces a precise readiness skip rather than an attempt to create a derived output.

Approval boundaries and reported contradictions:

- The global Index Info entry still says HC `(t,v,i,m)` at data_info.yml:56, but actual Country market=None and source signature say `(t,v,i)`. The proposal tests the explicit current Country contract and reports that schema inconsistency; it does not edit production schema or Feature behavior.
- CONTENTS' unqualified household Region completeness claim has its own three-missing-Region qualification. The proposed Region comparison uses actual retained observations and never fills a source Region or requires all bare rows to survive.
- The f8 "unit-wave columns unused at runtime" wording has the separate #1077 reviewed qualification; do not overwrite that fix. Unknown/Value/residual-code distinctions, split/mover IDs, redefined cross-wave v, rotation and refresh sample behavior are untouched.
- HC column-axis name loss is metadata-only, separately recorded. The new market tests promise exact retained measurements and column payload dtypes, not an unsupported claim that all frame axis metadata is unchanged.
- Preserve every nutrition/FCT fixture and scientific decision, including historical holds/qualified approvals/CPU variants. Nutrition/FCT were not copied, called or modified. #1062 production/routing remains separate and unchanged.

Exact post-approval validation and absence/cleanup plan:

1. Commit only the approved ledger first in the writable task lane, then implement the three files. Keep production/config/fixture/lock trees byte-identical. The diagnosis itself makes no commit/ref change.
2. Run the proposed 25 scoped cases on a new physical f8 Uganda prerequisite copy with all four derived parquet names absent: 15 real API cases and 10 pure dtype controls should execute with zero source-based skips in the held credentialed environment. No derived parquet may appear. Never manufacture those files to open a guard.
3. Run the same cases against an empty private data root and a warm source root with unavailable credentials. Preserve deliberate real-data skipping; the ten pure dtype controls should still execute. Current unmodified empty/credentialless controls each skip 15/15 without source acquisition; the replacement should be 10 pure passes + 15 data skips.
4. Re-run six missing/object/string mutants against the actual new validator, both float representations as positives, default missing-v/default-m/ignored-market/missing-m/duplicate-row/changed-measurement controls, and the six unchanged attrition faults. A faulty object or missing canonical field must not pass because its old wide column is absent. An index/value fault must not become xpass/xfail.
5. Run bounded supporting files: test_food_prices_units_kwarg.py, test_wave_getattr.py, test_add_market_index_dedup.py and the existing TestUganda2009MarketFallback class. Preserve all their source/value pins. If any new coverage/value failure appears, stop and diagnose it; do not remove assertions, broaden tolerances, change baselines or suppress it.
6. Run the API selection in a fresh process and compare complete outputs/attrs to the pinned f8 diagnosis where unchanged semantics are expected. Validate prerequisite hashes/coverage and absence of derived files. Snapshot source/ambient parquets before/after every normal/fault run.
7. For setup/restoration, use the established `with pytest.MonkeyPatch.context()` / finally data_root.cache_clear discipline in the private validation runner. Already executed four controls cover original LSMS_DATA_DIR present/absent × success/injected exception: exact environment presence/value restored, memoized path cleared and both outer source and private prerequisite bytes/mtimes unchanged. Physical private copies only; no shared L1 symlink or deletion/purge of ambient roots. Temporary copies stay in task-private storage. Remove unused destructive helpers rather than calling them.
8. Final gate: source/fixture/tracked manifest and HEAD proof, no in-tree data, no DVC CLI/acquisition/shared-cache write, no derived parquet creation, diff whitespace check and test-only/library-blob scope proof. No full suite or all-country hash census is required for these test-only edits.

Executed diagnosis commands are the private `prepare.py`, `run.sh run_current.py`, `run.sh probe_contracts.py`, `run.sh probe_controls_cleanup.py`, and `run.sh run_skip_control.py {empty,credentialless}`. Mounted image Python, exact f8 cwd/PYTHONPATH/config and private data root, unshare -rn plus active connect/connect_ex guards, numerical/build/make threads=1 were used. The baseline trace is serial; no xdist worker exceeds the root's four-worker cap. Rows/IDs/full warnings/logs remain private.

Stop here for root boundary approval. Neither the proposal nor the prototype controls constitute a tracked test change.

-- Sue
