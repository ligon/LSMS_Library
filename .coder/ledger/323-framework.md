# Prior-Art Ledger — GH #323 (framework / class fix)

**Search tier used:** ripgrep + git + direct measurement against the L2-wave parquet tier.

## §1 Task, restated

`country._normalize_dataframe_index` collapses a non-unique **declared** index with
`groupby(level=...).first()`. Where the duplicate rows carry *different* values, the
dropped rows are real data and vanish with no signal. This is the ROOT of #323: the
per-country index fixes close *instances*; this closes the *class*.

The class must become **impossible to hit silently** — the collapse is either
lossless (proved), or it is LOUD, and it survives the cache that previously hid it.

## §2 Existing machinery (do not reinvent)

| symbol | path:line | what it does | reuse / extend / new |
|--------|-----------|--------------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorder/drop levels; `groupby().first()` collapse at 4197-4210 | **extend** |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | `{"food_acquired": ("Quantity","Expenditure")}` — the ONE table with a real reduction policy | **reuse as-is** |
| `_collapse_duplicate_index` | `feature.py:106` | the second `.first()` site (Feature assembly) | **extend (same audit)** |
| `melt_visit_intervals` | `local_tools.py:2150` | landed recipe for `interview_date` multi-visit (Malawi, `3d7a7c61`) | reuse — NOT my job |
| `cache_freshness` / `stamp_parquet_hash` / `to_parquet(cache_hash=)` | `local_tools.py:1486/1507/1541` | v0.8.0 content-hash L2 staleness; embeds `lsms_cache_hash` in parquet schema metadata | **extend** — same mechanism carries the grain audit |
| `LSMS_CACHE_SCHEMA` | `local_tools.py:1354` | manual library-version cache-invalidation lever | **bump 1 -> 2** |

## §3 Definitions & conventions in force

- **NO AGGREGATION IN CORE** — `SkunkWorks/grain_aggregation_policy.org` §"The contract".
  The access path (`country.py`, `feature.py`, `local_tools`) never reduces grain; all
  aggregation is analyst-invoked in `transformations.py`. Step 1 of 5 landed (PR #471,
  `u`-in-index). Steps 2-5 pending. **This ledger's fix must not contradict it.**
- Precedent from the only landed step: **PR #471 fixed `crop_production` by ADDING `u`
  to the index — not by aggregating over it.**
- `aggregation:` in `data_scheme.yml` — **DEAD CONFIG, zero consumers** (grep:
  `diagnostics.py:174` and `country.py:2387` both list it in a `_skip` set). All 9
  declarations are `interview_date: {visit: first}`, and all 9 tables **already declare
  `visit` in the index** — so they have no duplicates and the collapse never fires for
  them. It is a legacy-reproduction shim (`senegal.py:262`), NOT a grain policy.

## §4 Invariants & assumptions (landmines)

- **The L2-COUNTRY parquet (`var/`) is written POST-collapse.** The L2-WAVE parquet
  (`{wave}/_/`) holds the truth. Any scanner run against `var/` returns a false zero.
- **The #323 warning is structurally unable to fire warm.** It is gated on
  `not df.index.is_unique`; the cached frame is already collapsed, so the gate is
  never true. *The bug hides behind the cache that the bug poisoned.* Every existing
  instrument sits downstream of the destruction (`diagnostics._check_duplicate_index`
  reports "pass").
- `.pth` trap: `.venv/.../lsms_library.pth` pins imports to the MAIN checkout;
  `PYTHONPATH` does **not** override it (verified). Only cwd-as-`sys.path[0]`
  (`cd $WT` + `python -c`/`-m`) or an explicit `sys.path.insert` wins. Assert it.
- `yaml.safe_load` **throws on the `!make` tag** in `data_scheme.yml`. Use
  `lsms_library.yaml_utils.load_yaml`. A blanket `except: continue` around it silently
  turns every country into a zero — this cost me one broken scanner (§6).

## §5 Reuse decision

- Duplicate detection + reporting: **new** (`_audit_index_collapse`), because no
  existing check runs *upstream* of the destruction.
- Persistence of the signal: **reuse** the v0.8.0 parquet-schema-metadata mechanism
  (`lsms_cache_hash`) — add a sibling key `lsms_grain_audit`. Do not build a sidecar.
- Additive policy: **reuse** `_ADDITIVE_MEASURE_COLUMNS` unchanged.
- `transformations.collapse()`: **NOT built here** (design step 4). Out of scope.

## §6 Measurement (this is the load-bearing part)

Instrument **validated on the known positives before any zero was trusted**:
Mali/2014-15/household_roster -> 32,026; Guyana/1992/housing -> 311. First scanner
returned 0/0 (broken: `safe_load` + `Data Scheme` key) and was rebuilt.

Census over the L2-**wave** tier, simulating the core exactly (reorder -> drop
undeclared levels -> count dups): **142 cells / 31 countries / 7,501,053 dropped rows.**

The row count alone is a **misleading instrument**. Splitting by whether the duplicate
rows actually *disagree*:

| verdict | cells | dropped rows | rows destroyed |
|---|---|---|---|
| **DESTRUCTIVE** (duplicate rows conflict) | 89 | 1,040,342 | **542,114** |
| redundant (duplicate rows identical -> `first()` is a lossless dedup) | 53 | 6,460,711 | 0 |

6.46M of the 7.5M "dropped" rows are a **lossless de-dup** (e.g. a cluster attribute
repeated once per household in the cluster). Warning on raw row counts would bury the
real 542k under 6.5M false alarms — *that is precisely how a warning becomes noise
nobody reads*, which is how #323 died the first time.

**Third silent-loss path, previously unreported:** `groupby(level=...)` defaults to
`dropna=True`, so any row with **NaN in a declared index level is DELETED OUTRIGHT** by
the collapse — over and above it. 14 cells, **485,231 rows**. Worst:
`Burkina_Faso/food_acquired/2014` loses 460,438 of 557,822 rows (82.5%). This fires on
the additive branch too. Reported loudly here; **behaviour deliberately unchanged**
(see §7).

Declared-but-**absent** index level (the silent-narrowing chain at `country.py:4143-4152`):
**0 real cells.** The 20 apparent hits were false positives of my probe — `map_index()`
renames the legacy `j` index to `i`. So the guard is free to add.

## §7 Decisions, and what I deliberately did NOT do

1. **Did NOT wire `aggregation:` into the core collapse.** It contradicts the
   NO-AGGREGATION-IN-CORE contract, and it does not fix the headline case: Mali's `pid`
   is a *household* id stamped on every member (5,149 distinct values over 37,175 rows,
   3,335 households with `pid.nunique()==1`), so **no reducer is correct** — `first()`
   keeps one person per household; `sum` is meaningless on `Sex`. A declared
   `aggregation` there would only *put a signature on the corpse*, converting a
   silently-wrong bug into a silently-wrong bug **with paperwork**. Duplicates on a
   declared index mean the **identifier is broken** or a **level is missing** — the core
   now says exactly that, and refuses to be declared away.
2. **Did NOT change `dropna` behaviour** for the NaN-key deletion. Restoring 485k rows
   (incl. +460k to a Burkina food_acquired total) is a *data* change needing per-country
   validation; doing it in the same diff that is supposed to make data changes *visible*
   would make the diff unreviewable. It is now LOUD and strict-mode-fatal instead.
   Follow-up issue.
3. **Signal survives the cache** — the one line that matters. Reports are embedded in
   the L2-country parquet schema metadata (`lsms_grain_audit`) at write time and
   **re-emitted on every warm read**. `LSMS_CACHE_SCHEMA` 1 -> 2 forces the existing
   poisoned caches to rebuild once, so the fix actually fires on machines where the bug
   is already baked in. Without that bump the fix would be invisible exactly where the
   bug lives.
4. **The cache bump surfaces a pre-existing CotedIvoire bug (NOT caused by this diff).**
   `LSMS_CACHE_SCHEMA` 1 -> 2 forces one rebuild of every L2-country parquet, which
   acts as a repo-wide `--rebuild-caches`. That makes two tests fail:
   `test_table_structure.py::{test_declared_columns_present,test_feature_is_sane}[CotedIvoire/cluster_features]`
   — *"declared column 'Latitude' not found in ['Region','Rural','t','v']"*. **Both
   reproduce identically on pristine `development` under `LSMS_NO_CACHE=1`**, so the
   defect is pre-existing: CotedIvoire's `cluster_features` config no longer produces
   the `Latitude`/`Longitude` it declares, and a **stale L2-country parquet was still
   serving the old columns and keeping the test green.** Same pattern as Uganda #245
   (CLAUDE.md: "the bug stayed hidden behind a stale country-level cache"). Needs a
   per-country fix + its own issue; deliberately NOT fixed here (out of scope for the
   framework agent, and the per-country agents are editing those trees concurrently).

### Test status (honest)

- `tests/test_grain_collapse.py` — **14 new tests, all pass**; all fail on pre-fix code.
- A **2,292-test subset** (70% of the 3,272 collected), incl. `test_table_structure`
  across every country, `test_feature`, `test_sample`, `test_conversion`,
  `test_join_v_silent_skip_warn`, `test_uganda_v_grain_invariants`,
  `test_add_market_index_dedup`: **2,290 passed, 2 failed** — both the pre-existing
  CotedIvoire failures in §7.4, proven on pristine `development`.
- `test_cache_hash_invalidation` + `test_build_transform_hash` +
  `test_canonical_shape_via_cache_miss`: **50 passed, 1 xpassed.**
- **The full 3,272-test run did NOT complete** before this was written: the box was
  carrying ~20 concurrent pytest processes from the parallel per-country agents, and
  the schema bump makes the *first* run after this change pay a one-time cache
  rebuild per country. **Not claimed as green.** It must be run to completion on a
  quiet machine before merge.
- `tests/test_currency.py::test_feature_ghana_per_wave` (**GH #589**) fails on
  pristine `development` — confirmed identically; not touched.

5. **Warn by default, raise under `LSMS_GRAIN_STRICT=1`.** Raising by default detonates
   31 countries and gets reverted; warning alone is how we got here. The warning is
   high-precision (89 cells, not 142) and cache-surviving; strict mode lets CI/tests
   ratchet. **No allowlist of known-bad cells** — a known-bad cell stays loud until it
   is fixed.
