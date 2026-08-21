# Prior-Art Ledger — GH #323 (India / `employment`)

**Search tier used:** ripgrep + git floor (gitnexus MCP not reachable from this
worktree; used `rg` over `lsms_library/` + `tests/` and read `country.py`
directly at the collapse site).

## §1 Task, restated

India's `employment` table is registered in `countries/India/_/data_scheme.yml`
with the canonical index `(t, i, pid)` and built by the **YAML path** from a
single source file, `1997-98/Data/SECT02AD.DTA`, via the `employment:` block in
`1997-98/_/data_info.yml` (`idxvars: i: hhcode, pid: idcode`).

That source is **activity-level, not person-level**: one row = one *job*. The
declared index therefore under-declares the key, `Country.employment()` returns a
non-unique index, and `_normalize_dataframe_index` (`country.py:4176-4210`)
collapses it with `groupby(...).first()`. The task is to restore the missing key
level so the framework never collapses — *not* to declare an aggregation.

Classification: **INDEX_INCOMPLETE**, and specifically **class-1 (silently
WRONG)**, not merely class-2 (silently MISSING) — see §4.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels to match the declared schema; collapses a non-unique index (`groupby().first()`, or `sum` for `_ADDITIVE_MEASURE_COLUMNS`) and warns (GH #323) | yes — `tests/test_normalize_index_j_preserved.py` | **reuse unchanged** (no library edit) |
| `_declared_index_levels` | `country.py` (called at 4120) | parses `index: (t, i, pid)` from `data_scheme.yml` | indirectly | reuse — it is what makes a widened index take effect |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py` | per-table map of columns that may be SUMmed on collapse (`food_acquired`) | yes | **deliberately NOT extended** — see §5 |
| `df.dropna(how='all')` | `country.py:2217` | universal safety-net: drops rows where every non-index column is NaN | — | **pre-existing**; explains the API row count (§4) |
| `_join_v_from_sample` | `country.py` | joins `v` from `sample()` at API time | yes | reuse — untouched; still resolves `v` on the widened index |
| GH #324 fix (`i: hh` → `i: hhcode`) | `India/1997-98/_/data_info.yml` `employment:` comment | earlier fix to the *same* block | — | context only |
| GH #602 (India has no `Rural`) | `India/1997-98/_/data_info.yml` `sample:` comment | deleted a bogus float-keyed `Rural` mapping | — | context — collides with `employment.Rural`, §6 |

## §3 Definitions & conventions in force

- Canonical index levels + required columns: `lsms_library/data_info.yml`
  (per `STANDING.md §3`). `employment` is **not** registered there — it is an
  India-only table (verified: `grep -rln '^  employment:' countries/*/_/data_scheme.yml`
  → India only), so there is no cross-country `index_info` to widen and no
  `Feature()` re-collapse one layer up (verified empirically: `Feature('employment')`
  returns 6,158 rows, unique).
- YAML build path (`idxvars` / `myvars` → `df_data_grabber`): per `CLAUDE.md`
  "Two Build Paths". `employment` is YAML-path; no `materialize: make`.
- `v` is joined from `sample()` at API time, never declared in a feature index:
  per `CLAUDE.md` "`sample()` and Cluster Identity". Unchanged here.
- Cache tiers + `LSMS_NO_CACHE`: per `CLAUDE.md` "Cache Behavior". Load-bearing
  for the test, see §4.

## §4 Invariants & assumptions

- **The L2-country parquet (`var/`) is written POST-collapse; the L2-wave parquet
  (`{wave}/_/`) holds the truth.** Any scan of `var/` for this bug returns a false
  negative. Instrument validated against the known positives before use:
  Mali/2014-15 `household_roster` → 32,026 dup rows ✓; Guyana/1992 `housing` →
  311 ✓. Only then India/1997-98 `employment` → **6,194** dup on `(t, i, pid)`.
- **The GH #323 warning fires only on a COLD build.** In warm operation the
  collapse is already baked into the L2-country cache and
  `_normalize_dataframe_index` is never re-entered. A regression test that calls
  `Country('India').employment()` on a warm cache is therefore **vacuous** — it
  passes on the buggy config. Verified: the first draft of
  `test_no_gh323_collapse_warning_*` passed pre-fix for exactly this reason. It
  now sets `LSMS_NO_CACHE=1` to force the rebuild, and fails pre-fix.
- **`groupby().first()` is SKIPNA** (`country.py:4199`). It fills each column
  *independently* from that column's first non-null value, so it does not merely
  drop rows — it **manufactures** rows present in no source record. This is what
  makes the defect class-1 rather than class-2:
  - `hhcode=1011, idcode=1`: source rows are `A=(15.0, NaN)`, `B..E=(0.0,'Rural')`,
    `F=(NaN,'Rural')`. The collapse returns **`(15.0, 'Rural')`** — a pair in no
    source row.
  - 107 persons received `Cash_per_day` and `Rural` from two *different* rows;
    680 persons' surviving wage came from a *non-first* activity.
- **The 3-key is exactly unique**: `(hhcode, idcode, actcode)` → 16,089 distinct
  triples over 16,089 rows, zero nulls in any key. So this is neither
  GENUINE_DUPLICATES nor phantom-NaN (GH #606), and no rows need to be dropped.
- **API row count ≠ source row count, and that is pre-existing.** `country.py:2217`
  drops rows where *every* non-index column is NaN. 9,931 of the 16,089 activity
  rows have neither a wage nor a workplace, so the API returns **6,158**, not
  16,089. This safety-net is identical before and after the fix, and no wage row
  is lost to it (all 5,627 wage rows survive). *The dispatch brief's stated target
  of `len == 16089` is therefore wrong*; 6,158 is the correct post-fix count.
- **`Cash_per_day` (`v02b02`) is a per-day wage RATE**, not a stock — see §5.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| activity key `act` | **new index level** (`act: actcode`) | The only fix that removes the collapse without inventing data. The 3-key is exactly unique, so widening is lossless and total. |
| `aggregation: first` | **rejected** | Provably wrong: destroys 62.0% of wage observations (5,627 → 2,136 non-null), loses 39.8% of reported cash (92,116 → 55,498), and fabricates cross-row pairs (§4). |
| `aggregation: sum` / `_ADDITIVE_MEASURE_COLUMNS` | **rejected** | `v02b02` is a per-day wage **rate**. Rates do not add without days-worked weights (`v02a02a..l` / `v02a03`). Summing would produce a meaningless "total wage rate". `food_acquired` is in the additive map because expenditure/quantity across transactions genuinely *are* additive; a wage rate is not. |
| person-level collapse (primary activity / days-weighted mean) | **deferred to the CONSUMER** | It is a real economic decision, not a framework default. Downstream of a faithful activity-level table it is one line and it is *auditable*: `df.xs('A', level='act')` for the primary activity. A silent `groupby` inside the framework is exactly what caused #323. |
| library code (`country.py`) | **unchanged** | The framework behaved as designed; the config under-declared the key. Zero-line library diff ⇒ zero blast radius for the other 39 countries. |

## §6 Open questions for the human

- **`employment.Rural` is misnamed and should be renamed or dropped** (filed
  separately; deliberately NOT folded into this commit). `v02a05b` is a
  per-*activity* **workplace** location (14,998/16,089 NaN; 728 `Urban`, 363
  `Rural`) — *not* a household residence flag. India's `sample` deliberately
  carries **no** `Rural` per GH #602 ("India simply has no Rural indicator"), so
  the collapse was laundering a sparse per-job workplace field into a column that
  reads as the very household indicator #602 removed. Widening the index de-fangs
  it (it is now honestly per-activity rather than a fabricated person-level
  value), but the **name still misleads**. Suggested: `Workplace_Rural`.
  Blocks: nothing in #323; it is a naming/semantics fix with its own blast radius
  (`data_scheme.yml` declares `Rural: str` for `employment`).
- `data_scheme.yml` still declares `Rural: str` under **`sample`** for India even
  though GH #602 removed the column from `sample`'s `data_info.yml`. Harmless
  today (the column simply isn't produced on `development`… but see below).
- **Heads-up, not a finding of this task:** the main repo working copy is checked
  out on branch `fix/602-spellings`, not `development`. On `development` the
  pre-#602 float-keyed mapping is still live, so a cold build of India `sample`
  there yields `Rural` populated with **raw stratum labels** (`' B-other'`,
  `'UP-qual'`, …) for 2,251/2,251 households. That is #602, already fixed on its
  own branch; noted here only because it briefly contaminated this task's control
  (see Phase 3).

---
### Phase 3 — verification

- `India/1997-98/_/data_info.yml :: employment.idxvars.act` — **OK (anchored on §4, §5)**:
  adds the exactly-unique third key; no aggregation declared, no rows dropped.
- `India/_/data_scheme.yml :: employment.index` — **OK (anchored on §5)**: widened to
  `(t, i, pid, act)`. The widened index *is* the enforcement; the comments beside it
  are rationale only (`CLAUDE.md`: "prose is not enforcement").
- `tests/test_india_employment_activity_index.py` — **OK (anchored on §4)**: 6 of its
  7 tests fail against the pre-fix config; the 7th is a schema-free unit pin on the
  skipna-`first` fabrication mechanism itself. The cold-build warning test was
  rewritten after it was caught passing vacuously on a warm cache (§4).
- **Control contamination (caught and corrected).** The first regression run used
  the *main repo* as the "base" config and reported `sample` as CHANGED. The main
  repo is on `fix/602-spellings`, not `development` — the control was wrong, not
  the fix. Re-run against a detached worktree of the true base commit
  (`2d3d5f71`, my branch's parent): **exactly one table changed — `employment`**;
  the other 9 India tables (incl. `sample`) are byte-identical by sha256 of a
  full sorted CSV dump. Lesson, same shape as the brief's instrument trap: *a
  result from a control you have not validated is a result about the control.*
- **Reinvention check:** none. No new helper, transform, or estimator was written;
  the fix is 3 semantic lines of YAML (one `idxvars` entry, one widened `index`).
  `_normalize_dataframe_index` and `_ADDITIVE_MEASURE_COLUMNS` were considered and
  deliberately left untouched (§5).
