# Prior-Art Ledger — coverage-matrix (country × feature × wave readiness)

> Per-task ledger. Living, git-tracked snapshot of the machinery, definitions,
> and conventions that bear on the coverage-matrix task. Edit in place; git
> history is the journal. Inherits the repo §0 baseline in `STANDING.md` — cites
> it, `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.
> Charter: `.coder/charter-coverage-matrix.md`.

**Search tier used:** ripgrep + git floor + 3 read-only Explore agents (gitnexus
not invoked; the reuse surface is small and was read directly).

## §1 Task, restated
Produce a refreshable, consultable readout of the ragged 3-tensor
`(country, feature, wave)`. Per cell, assign a **status tier** spanning "source
absent" → "broken build" → "builds-with-warnings" → "sanity-clean" → "blessed".
"Feature" means a `Feature()`-able table (declared in a country's
`data_scheme.yml` or auto-derived from a source table). "Wave" is a per-country
ragged label (`Country.waves`). The work **reuses** the audit/sanity/catalog
machinery; it does not re-implement sanity checks or re-derive the declared
matrix. A local `make matrix` target builds the snapshot + a self-contained HTML
view; nightly CI is out of scope (deferred).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `catalog.countries` / `features` | `lsms_library/catalog.py:71,109` | declared (config-only) country/feature axes; cross-filterable | yes (`tests/`) | **reuse** for axes |
| `catalog._country_dirs` / `_declared_tables` / `_all_features` | `lsms_library/catalog.py:21,38,51` | cheap config reads (lru_cached) of the declared matrix | indirectly | **reuse** |
| `Country.waves` | `lsms_library/country.py:1430` | ragged per-country wave-label list (module/yaml/dir discovery); sets `wave_folder_map` as side effect | — | **reuse** (access before `[w]`) |
| `Country.__getitem__` | `lsms_library/country.py:1542` | `Country(c)[w] -> Wave`, via `wave_folder_map` | — | **reuse** |
| `Wave.data_scheme` | `lsms_library/country.py:1505` (country) / `:542` (wave) | per-wave table list = `.py` script stems + `data_info.yml` keys (remaps `other_features→cluster_features`); **does NOT add derived tables** | — | **reuse** as per-wave coverage oracle |
| `Country.data_scheme` | `lsms_library/country.py:1505` | country feature list, **does** add derived tables when source present | — | **reuse** for per-country applicability |
| `_DERIVED_SOURCE` | `lsms_library/feature.py:171` | derived→source map (food_*, household_characteristics) | — | **reuse** to lift wave coverage to derived features |
| `diagnostics.load_feature` | `lsms_library/diagnostics.py:577` | load any feature (property or method) off a `Country` | yes | **reuse** to build the country-level table once |
| `diagnostics.is_this_feature_sane` | `lsms_library/diagnostics.py:591` | 16-check `SanityReport`; runs on **any** DataFrame (incl. a wave slice) | yes | **reuse** — grade each wave slice |
| `SanityReport.ok` / `Check.ok` | `lsms_library/diagnostics.py:104,86` | `ok = no check == "fail"` (warns allowed) | yes | **reuse** as the builds↔sane boundary |
| `diagnostics._PROPERTY_FEATURES` | `lsms_library/diagnostics.py:574` | `{panel_ids, updated_ids}` — dict properties, no wave axis | yes | **reuse** to route country-level-only cells |
| `country.JSON_CACHE_METHODS` | `lsms_library/country.py:75` | `{panel_ids, updated_ids}`; excluded from `_all_features` | — | **reuse** (same set) |
| `bench/feature_audit/scan.py` `Finding`/`_build` | `bench/feature_audit/scan.py:155,183` | per-cell record + warning/exception-capturing build wrapper | — | **reuse pattern** (`_build`-style capture); matrix is a thinner sibling |
| pandas `Styler.to_html` | (pandas ≥2.2 dep) | self-contained HTML, no new deps; precedent `bench/build_feature.py:66` | — | **reuse** for render |
| Makefile `profile` targets | `Makefile:134` | `$(POETRY) run python bench/<script>` + `setup` stamp pattern | — | **reuse pattern** for `matrix:` |

## §3 Definitions & conventions in force
- **Declared vs build-based matrix**: declared = config (cheap), build-based =
  actually runs (expensive). `catalog.py:8-12` states the distinction. Per
  STANDING.md §3 / `CLAUDE.md` "Cache Behavior".
- **Derived tables** (`food_expenditures`, `food_prices`, `food_quantities`,
  `household_characteristics`): auto-surfaced at runtime, **never** in
  `data_scheme.yml`; covered iff source (`food_acquired` / `household_roster`)
  present — `_DERIVED_SOURCE` `feature.py:171`, `CLAUDE.md` "Derived Tables".
- **`t` index level** = wave label; the assembled `Country(c).feature()` carries
  all waves in `t`. Per `CLAUDE.md` / `data_info.yml`.
- **Warm cache & data access**: read via the cache + lock-free S3 bypass; **never
  `dvc pull`** (STANDING.md §4 / `CLAUDE.md` Data Access). v0.8.0 content-hash
  makes re-runs rebuild only changed cells.

## §4 Invariants & assumptions
- Access `Country(c).waves` **before** `Country(c)[w]` — `[w]` depends on
  `wave_folder_map`, populated as a side effect of `.waves` (`country.py:1442,1545`).
- `Wave.data_scheme` omits derived tables; must union derived-from-source for the
  coverage layer (`country.py:542` vs `_DERIVED_SOURCE`).
- `panel_ids` / `updated_ids` have **no wave axis** and are not `Feature()`-able
  (`JSON_CACHE_METHODS`, `_PROPERTY_FEATURES`) → country-level (`wave=None`) cells.
- Wave-label ↔ `t`-value match is assumed exact. Multi-round folders (Tanzania
  `2008-15/`, mapped via `wave_folder_map`) may make a wave's label differ from
  its `t` value → risk of a **false `dropped`**. Logged as a known limitation;
  the readiness grader must not silently mark such cells (see §6, charter §5).
- Do not bake `v`/`m` into anything; read-only consumer of the API (STANDING.md §4).
- pandas-3 rules: `pd.NA`, no `inplace` (STANDING.md §4 / `CLAUDE.md`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| country/feature axes | reuse | `catalog.countries/features` is the declared matrix |
| per-wave coverage | reuse | `Wave.data_scheme` ∪ `_DERIVED_SOURCE` lift |
| feature build | reuse | `diagnostics.load_feature` (handles property/method) |
| per-wave readiness grade | reuse | slice built frame on `t`, run `is_this_feature_sane` |
| builds↔sane boundary | reuse | `SanityReport.ok` |
| build/​warning capture | extend | `scan.py:_build` pattern, thinned into `bench/matrix.py` |
| HTML render | reuse | pandas `Styler.to_html` (no new dep) |
| tier ladder + enumeration glue + reader | **new** | no existing object spans the 3-tensor or maps `SanityReport`→tier; this is the only genuinely new logic, and it composes the above rather than re-deriving any check |

## §6 Open questions for the human
- Multi-round wave-label↔`t` mismatch (Tanzania): accept possible false `dropped`
  in v1 (logged), or reconcile via `wave_folder_map` now? (Charter §5; default:
  log + defer.) — blocks only Tanzania-row accuracy, not the design.

### Deferred from code review (2026-06-25; non-blocking, documented)
- **Tanzania label↔`t` false `dropped`** (review Low #3): when a feature has a
  `t` axis but the country's wave labels are disjoint from the built `t` values,
  every wave reads `dropped`. Mitigation idea: detect total disjointness and
  annotate as "possible label/t mismatch" rather than plain `dropped`. Deferred.
- **No-`t`-axis feature emits W `n/a` + 1 `wave=None` summary** (Low #4): minor
  row-count inflation vs charter §2 "exactly one row per cell"; harmless (grid
  roll-up dedupes). Deferred.
- **Wave that errors on construction → `absent`, not `broken`** (Low #6): a real
  wiring break at the *wave* level reads as "no source". Country-level failures
  are already `broken`; per-wave construction errors are rare. Deferred.
- **Warning-level signal excluded from the tier** (Low #8): intentional — the
  tier is built from sanity checks, not `warnings`. Documented, not a bug.

## §7 Implementation note (where the code landed)
The data-model logic lives in **`lsms_library/coverage_matrix.py`** (importable,
testable; backs `ll.coverage()`), NOT in `bench/`. `bench/matrix.py` is a thin
CLI + HTML renderer importing that module. Module named `coverage_matrix` (not
`coverage`) to avoid shadowing the public `ll.coverage()` function in
`__init__.py` (a submodule named `coverage` would be masked by
`from .coverage_matrix import coverage`).

---
### Phase 3 — verification (fill at task end)
- `coverage_matrix.build_matrix` / `grade_feature` — OK (anchored §2/§5):
  composes `load_feature` + `is_this_feature_sane` + `Wave.data_scheme`; the only
  new logic is the tier mapping. No REINVENTION (sanity checks reused verbatim),
  no CONTRADICTION (warm-cache / `pd.NA` / read-only honored).
- `tier_from_report` — OK (§5): a 1-line wrapper over `SanityReport.ok`; not a
  re-derivation of the 16 checks.
- `bench/matrix.py` render — OK: hand-rolled HTML, no jinja2/Styler dep (the
  `.style` accessor needs jinja2, absent from `.venv.lustre`); honors §3 "no new
  deps".
- `ll.coverage()` reader — OK: snapshot read + `refresh='coverage'` live layer;
  tested in `tests/test_coverage_matrix.py` (16 tests pass).
- Readiness tiers validated live on Slurm (job 35192638, fc_jevons/savio3_htc):
  454 cells over Uganda/Iraq/Malawi/Niger → sane=342, builds=43, absent=69,
  broken=0, dropped=0. Anchor cells confirmed (Uganda food_expenditures /
  household_roster 2013-14 → sane). `builds` cells trace to genuine per-wave
  `no_all_null_columns` / `no_null_index_levels` sanity fails — correct
  discrimination.

### Phase 6 — code review (general-purpose agent, code-reviewer methodology)
- **High fixed:** the repo's global `*.csv` ignore swallowed
  `.coder/coverage/{latest,blessed}.csv` (broke a charter §2 done-criterion + the
  `blessed` tier). Added `!` negations in `.gitignore`; confirmed trackable.
- **Medium fixed:** per-wave availability was recomputed O(F×W); hoisted to
  once-per-country in `build_matrix` (the cheap live-coverage path). Output
  byte-identical (218 Uganda cells before/after).
- **Lows fixed:** `grade_country_level` truth-test (`if n == 0`), `coverage()`
  rejects unknown `refresh=` (was silent stale read), detail-table sorts
  worst-first by ladder. Remaining Lows deferred (see §6) — all non-blocking.
- Cleared by review: per-wave slice keeps the `t` level (int `t` safe via
  `str()`); no module/function shadowing post-rename; renderer handles empty
  matrix + NA `n_rows`; pandas-3 clean.

### Full-cube run (authoritative, Savio array job 35193615)
- 34 countries built in parallel, each from a **cleared cache** (per-country
  `lsms-library cache clear` then cold build) -> authoritative source-truth.
  1825 cells: sane=1186, absent=456, builds=136, dropped=39, broken=8.
- broken=8 = Armenia(2)+Nepal(6) = the documented no-microdata countries
  (CLAUDE.md "Countries Without Microdata") -> correct, not new bugs.
- dropped=39 = the worklist (Nigeria multi-round ×22, EHCVM
  household_characteristics, CotedIvoire, GhanaLSS). Triage is follow-up;
  some are the known multi-round wave-label↔`t` false-positive (§6 / review #3).
- Snapshot committed to `.coder/coverage/latest.csv` (lights up the docs grid).

### Determinism finding (important; from the warm-cache check)
- A **warm cache is non-authoritative**: Uganda `earnings` graded `sane` cold but
  `broken` warm (GH #479 stale script-path wave parquet: declared col `Earnings`
  vs built `earnings`). The matrix faithfully reports build state; the
  non-determinism is a library-cache property. -> Authoritative runs clear the
  cache first (now the cube's method; documented in `docs/guide/coverage.md`).
  This is a *found bug* surfaced by the tool (out of scope to fix per charter).

### Slurm/venv gotcha (for future cube runs)
- `bin/savio_venv.sh mount` (squashfuse) RACES when many array tasks share a
  node -> broken mounts (`ModuleNotFoundError: yaml`, exit 127). Use a per-task
  node-local tar-pipe from `.venv.lustre` instead (see
  `slurm_logs/2026-06-25_coverage_matrix/cube/build_shard.sbatch`). Single jobs
  (validation/aggregate) are fine with the squashfs mount.

### Docs integration (commit 9225a029)
- `docs/guide/coverage.md` + stdlib-only mkdocs hook `docs_hooks/coverage_matrix.py`
  render the snapshot CSV into the site at build time. Decoupled from data
  access (docs CI installs only mkdocs-material+mkdocstrings): a refreshed CSV
  commit -> docs redeploy re-renders. Reuse boundary: the hook duplicates ~4
  presentation dicts because the package is not importable in the docs env.
