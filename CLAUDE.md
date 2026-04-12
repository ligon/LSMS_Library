# LSMS Library

## Design Philosophy
Harmonize the *interface*, not the data. Surveys differ in structure; the library provides a uniform API without discarding survey-specific detail.

## Core API
```python
import lsms_library as ll
uga = ll.Country('Uganda')
uga.waves          # ['2005-06', '2009-10', ...]
uga.data_scheme    # ['people_last7days', 'food_acquired', ...]
food = uga.food_expenditures()  # Standardized DataFrame
```

The `Feature` class assembles the same table across every country that declares it:
```python
roster = ll.Feature('household_roster')
df = roster()                         # all countries
df = roster(['Ethiopia', 'Niger'])    # subset
```
The returned DataFrame prepends a `country` index level.

## Task-Specific Skills (read these on demand)

- `.claude/skills/add-feature/SKILL.md` — adding a new table to a country. Has sub-skills for `sample`, `food-acquired`, `shocks`, `assets`, `panel-ids`, and `pp-ph` (post-planting/post-harvest countries — Nigeria, Ethiopia, GhanaSPS).
- `.claude/skills/multi-round-waves.md` — Tanzania `2008-15/` multi-round folder pattern and `wave_folder_map`.
- `.claude/skills/tanzania-panel-design.md` — NPS sub-panel split (extended vs. refresh).
- `.claude/skills/demand-estimation.md` — running CFE demands via the Country API.
- `.claude/skills/release/SKILL.md` — poetry build gotchas when cutting a wheel.
- `scrum-master-hpc` (shared sucoder skill at `/home/ligon/Projects/sucoder-skills/scrum-master-hpc/SKILL.md`) — dispatching subagents, worktrees, DVC lock hygiene. Read this before using the Agent tool for multi-country work. Two library-specific addenda: (1) subagents share the parquet cache at `~/.local/share/lsms_library/`, so concurrent agents building different countries don't conflict; (2) the venv is at `{repo_root}/.venv/bin/python` (not in worktrees) — set `PYTHONPATH` to the worktree so development-branch code is picked up.

## Repository Layout
- Country root-level symlinks (e.g. `Uganda -> lsms_library/countries/Uganda`) are convenience only; actual config lives under `lsms_library/countries/`.
- **DVC repository is rooted at `lsms_library/countries/`, NOT the top-level repo.** `.dvc/`, remotes, and credentials all live there. Run `dvc` CLI commands from that directory or they fail with missing-remote errors.
- **Two Makefiles**: top-level `Makefile` (Poetry setup, pytest, build); `lsms_library/Makefile` (country-specific test/build/materialize/demands). `make -C lsms_library help` for details.

## Cache Behavior (v0.7.0+)

`load_dataframe_with_dvc` in `country.py` does a best-effort cache read at the top of the function, before consulting DVC at all. If the parquet under `data_root()` exists and `LSMS_NO_CACHE` is not set, it returns directly. This gives 10–17× cross-process speedups vs. pre-v0.7.0 on all 40 countries.

- **Default cache location**: `~/.local/share/lsms_library/{Country}/var/{table}.parquet`. Override with `data_dir` in `~/.config/lsms_library/config.yml` or the `LSMS_DATA_DIR` env var (env var wins).
- **No automatic staleness check.** Editing a wave's `data_info.yml`, a `_/{table}.py` script, or an upstream `.dta` file does NOT trigger a rebuild. Force one with `LSMS_NO_CACHE=1` in the session, `lsms-library cache clear --country {Country}`, or by deleting the parquet.
- **`LSMS_BUILD_BACKEND=make`** bypasses both the top read and the DVC stage layer entirely — every call rebuilds from source, with no cache writes or reads.
- **`trust_cache=True`** is a narrower in-process short-circuit at the top of `_aggregate_wave_data` that still calls `_finalize_result` (so kinship expansion, spelling normalization, and `_join_v_from_sample` still apply). Use for "cache is known fresh, skip all existence checks". It ignores `LSMS_NO_CACHE`.
- **Cache vs. API**: cached parquets store pre-transformation data. Kinship expansion, canonical spelling enforcement, and dtype coercion happen in `_finalize_result()` on every read — not at cache write time. So `pd.read_parquet(cache_path)` shows raw `Relationship` strings; the Country API shows decomposed `(Sex, Generation, Distance, Affinity)`.
- **Stage-layer countries** (Uganda, Senegal, Malawi, Togo, Kazakhstan, Serbia, GhanaLSS) declare a `dvc.yaml` with a bare `python3` cmd. On Savio that resolves to system Python 3.6.8 and fails; v0.7.0 turns the failure into a benign fallback via cache. v0.8.0 will retire the stage layer entirely. See `SkunkWorks/dvc_object_management.org`.

## Two Build Paths: YAML vs. Makefile/Script

There are two ways a table gets built at the wave level.

- **YAML path (`data_info.yml`)**: preferred for simple cases. `Wave.grab_data()` reads `idxvars`/`myvars`, calls `df_data_grabber()` to extract columns from one source file, applies formatting functions, and returns a DataFrame. No parquet written at the wave level.
- **Script path (legacy `_/{table}.py`)**: declared with `materialize: make` or `!make` in `data_scheme.yml`. A standalone Python script calls `to_parquet()` to write a wave-level parquet. Required when YAML cannot express the transformation:
  - Multiple rounds per wave directory needing distinct `t` values (see `.claude/skills/add-feature/pp-ph/SKILL.md`).
  - Multi-wave source files with a `round` column (Tanzania `2008-15/`; see `multi-round-waves.md`).
  - Elaborate unit conversions or cross-file joins (Nigeria / GhanaLSS `food_acquired`).

**Rule of thumb**: column mappings from one source file per wave → YAML. Cross-file concatenation, per-row `t` assignment, or multi-wave source files → script with `materialize: make`.

## Data Access

Microdata must be obtained from the [World Bank Microdata Library](https://microdata.worldbank.org/) under their terms of use. Contributors pushing write access need GPG/PGP keys.

**User config** lives at `~/.config/lsms_library/config.yml`:
```yaml
microdata_api_key: your_key_here
# data_dir: /path/to/override   # same as LSMS_DATA_DIR env var
```
Lookup order for each setting: environment variable → config file → None.

**Always read files with `get_dataframe()` from `local_tools`.** It handles `.dta` / `.csv` / `.parquet` via a fallback chain: local file on disk → DVC filesystem (`DVCFileSystem`) → WB NADA download via `data_access.get_data_file()`. A script written as `get_dataframe('../Data/file.dta')` works whether the file is local, DVC-cached, or has never been downloaded.

**Always write parquets with `to_parquet(df, 'name.parquet')`** from `local_tools`. It redirects to `data_root()` via `_resolve_data_path()`, which inspects the call stack to infer country/wave and handles three patterns: bare `foo.parquet` from wave scripts, `../var/foo.parquet` from country scripts, and `../wave/_/foo.parquet` cross-wave refs. Stale parquets from before this migration may still exist in-tree; they are harmless artifacts.

**Anti-patterns — do not use:**

| Anti-pattern                                             | Why                                              |
|----------------------------------------------------------|--------------------------------------------------|
| `dvc.api.open(fn, mode='rb')` + `from_dta(f)`            | Couples to DVC internals, skips the WB fallback  |
| `pd.read_stata('/absolute/path/...')`                    | Breaks on other machines, no DVC/WB fallback     |
| `pyreadstat.read_dta(path)` directly                     | Same — bypasses all access layers                |
| `from_dta('lsms_library/countries/...')` with abs path   | Non-portable; use relative `../Data/` paths      |

**Adding new waves**: `lsms_library.data_access.discover_waves()` / `add_wave()`; `push_to_cache_batch()` for batched `dvc add` + `dvc push`. See `CONTRIBUTING.org`.

**Three-tier credential model.** The WB Microdata API key is the sole real gate; the S3 bucket is a read cache over the authoritative WB NADA downloads.

| User has                                                   | Gets                                                  |
|------------------------------------------------------------|-------------------------------------------------------|
| Nothing                                                    | Import warns; data-access calls raise `RuntimeError`  |
| Valid WB Microdata API key                                 | Direct WB downloads + auto-unlocked S3 read cache     |
| WB API key + S3 write creds                                | The above + push access (for RAs materializing waves) |

Auto-unlock decrypts `s3_reader_creds.gpg` with an obfuscated passphrase at import time. That obfuscation is cosmetic anti-grep, NOT a security gate — the WB API key check is the authoritative policy. Don't "fix" the obfuscation.

## Canonical Schema

`lsms_library/data_info.yml` is the single source of truth for cross-country conventions:
- required columns per table (e.g. `household_roster` requires `Sex`, `Age`, `Generation`, `Distance`, `Affinity`);
- accepted values (e.g. `Sex: [M, F]`, `Affinity: [consanguineal, affinal, step, foster, unrelated, guest, servant]`);
- rejected spellings (e.g. `Relation` → use `Generation, Distance, Affinity`).

`tests/test_schema_consistency.py` reads from this file — never hardcode schema rules in tests.

**Kinship decomposition (Kroeber 1909).** `household_roster` uses four columns instead of a single `Relationship` string: `Sex`, `Generation` (0=same, +1=parent, −1=child), `Distance` (0=lineal, 1=sibling line, 2=cousin), and `Affinity`. `_expand_kinship()` in `_finalize_result()` transforms `Relationship` automatically using `lsms_library/categorical_mapping/kinship.yml`. Unrecognized labels emit a warning — add them to the YAML with their `[Generation, Distance, Affinity]` tuple.

**Canonical spellings.** Columns in `data_info.yml` can declare a `spellings` inverse dict mapping canonical value → list of accepted variants. `_enforce_canonical_spellings()` replaces variants with canonical forms at API time, on both column values and index levels.

**Automatic categorical mappings.** If a column/index name in a returned DataFrame matches a table name in the country's `categorical_mapping.org` (case-insensitive) and that table has a `Preferred Label` column, the mapping is applied automatically — no `mappings:` declaration needed. For name mismatches (e.g. `harmonize_food` for index `j`), use the explicit `mappings:` syntax in `data_info.yml`. Cross-country label harmonization is a design sketch; see `SkunkWorks/cross_country_label_harmonization.org`.

## Derived Tables

`household_characteristics`, `food_expenditures`, `food_prices`, and `food_quantities` are **auto-derived at runtime** via `_ROSTER_DERIVED` and `_FOOD_DERIVED` in `country.py` (source transforms live in `transformations.py`). **Do NOT register them in `data_scheme.yml`** — doing so bypasses the derivation path and forces legacy `!make` scripts to run.

`Country._DEPRECATED` maps removed/deprecated table names to deprecation messages. `__getattr__` checks it before `data_scheme`, returning a method that emits `DeprecationWarning` and calls a compatibility shim. Currently contains `locality` only. See `docs/migration/locality.md`.

## `sample()` and Cluster Identity

The `sample` table (index `(i, t)`, columns `v`, `weight`, `panel_weight`, `strata`, `Rural`) is the single source of truth for mapping households to their sampling cluster. **As of 2026-04-10, `v` is joined from `sample()` at API time** by `_join_v_from_sample()` in `country.py`, called from `_finalize_result()` for any household-level table when the country has `sample` in its `data_scheme` and `v` isn't already present.

Rules:
- Do NOT put `v` in feature `data_scheme.yml` indexes other than `cluster_features` (which owns it).
- Do NOT bake `v` into feature parquets. Wave scripts should write `(t, i, ...)` and let the framework join.
- Do NOT use `dfs:` merge blocks just to join `v` from a cover page — collapse to a single-file extraction.
- Two weight types: `weight` (cross-sectional; positive for all interviewed HH including refreshment); `panel_weight` (longitudinal; NaN/zero for refreshment). Pre-refreshment waves have the same value in both columns.
- Country caveat: `Country(name).household_roster()` only gets `v` in the index if the country has `sample` in its `data_scheme.yml`.
- `_join_v_from_sample()` skips when `v` is already in `df.columns` (not just `df.index.names`), so legacy scripts with `v` as a non-index column still work. Prefer putting `v` in the index or nowhere in new code.

Skill: `.claude/skills/add-feature/sample/SKILL.md`. Migration history: `slurm_logs/PLAN_sample_v_migration.org`, `slurm_logs/DESIGN_sample_as_v_source.org`.

`panel_ids` and `updated_ids` are `@property` attributes on `Country`, not methods — they return dicts, not DataFrames. Code iterating over `data_scheme` entries and calling `getattr(c, name)()` must special-case these. Use `diagnostics.load_feature(c, name)` which handles both.

## Panel ID Transitive Chains and the `attrs` Flag

`_finalize_result()` runs `id_walk()` and sets `df.attrs['id_converted'] = True` to prevent double-application. **`merge()` and `set_index()` drop `attrs` in pandas 2.x** — both appear in `_join_v_from_sample()`. When `attrs` is lost, `_finalize_result` runs `id_walk` a second time on already-converted data, and for countries with transitive chains (A→B→C, where B is itself a mapping key) this produces household-level ID collisions and duplicate index entries. Burkina Faso 2021-22 had 392 duplicate tuples before this landed in commit `4db41a27`.

**Rule**: any framework method touching a DataFrame downstream of `id_walk()` in `_finalize_result()` must explicitly copy `attrs`:
```python
result = flat.set_index(new_idx)
result.attrs = dict(df.attrs)  # preserve id_converted flag
```

## Gotchas with Teeth

- **`other_features` is obsolete** — it's fully replaced by `cluster_features` + `_add_market_index()` at query time. Do not read `other_features.parquet` in new code. The `m` index should NOT be baked into cached parquets; it's added on demand when the user passes `market='Region'`. If a wave-level script genuinely needs region for data processing (Malawi's region-specific unit factors), read the cover-page `.dta` directly.

- **EHCVM countries**: in Senegal, Mali, Niger, Burkina Faso, Benin, Togo, and Guinea-Bissau, each `grappe` is visited in exactly one `vague` — so `v: grappe` (not `v: [vague, grappe]`) and `i: [grappe, menage]` (not `[vague, grappe, menage]`). CotedIvoire 2018-19 is also EHCVM but predates the list above.

- **`format_id` is auto-applied to `idxvars` but NOT to `myvars`**. A numeric column reaching `myvars` (e.g. a cluster ID) stays float and gets `.0` suffixes when stringified. Fix with an explicit formatting function in the wave module. `is_this_feature_sane()` checks for this via `_check_float_stringified_index`.

- **Joining `v` via `dfs:` is legacy**. Since Phase 2 (2026-04-10) you should not add a cover-page sub-df just to pick up `v` — let `_join_v_from_sample()` do it. Existing `dfs:` merges are grandfathered but should be collapsed when touched.

- **Housing schema is categorical, not binary.** Since 2026-04-10, Uganda `housing` has `Roof` and `Floor` columns with material-name values (`Thatch`, `Iron Sheets`, `Cement`, …), mapped through `categorical_mapping.org`. Malawi does not yet have a `housing` table (tracked in #155). Consumers who want binary indicators derive them trivially (`df['Roof'] == 'Thatch'`); the reverse is not possible.

- **Categorical columns from `.dta` / `.sav`**. `get_dataframe()` returns pandas categoricals from Stata/SPSS. `select_dtypes(exclude=['object']).max()` crashes on unordered categoricals — exclude `'category'` too. `groupby().first()` crashes similarly — convert to string with `.astype(str).replace('nan', pd.NA)` first. YAML mapping keys must be string keys (`'urbana': Urban`) not numeric (`1: Urban`) when the raw labels are strings.

- **`lsms` upstream dependency has been retired.** `lsms (>=0.4.13,<0.5.0)` is no longer in `pyproject.toml`. ~15 country-level `_/country.py` files still carry dead `from lsms.tools import` at module top; on a fresh install + cold cache, rebuilds of Ethiopia/Tanzania/GhanaLSS `food_acquired` will crash with `ModuleNotFoundError`. Clean up when the file is next touched. Full file list in commit `05cd1ec7`'s message. **Do not use `from lsms.tools import` in new code** — use `get_dataframe` and `df_data_grabber` from `local_tools` instead.

- **`locality` is deprecated.** `Country('Uganda').locality()` emits `DeprecationWarning` and returns via `legacy_locality(country)` from `transformations.py`, which joins `sample()` and `cluster_features()` to reproduce the legacy `(i, t, m) -> v` shape. The 9 wave-level `locality.py` scripts and `uganda.other_features()` have been deleted. See `docs/migration/locality.md`.

- **`_log_issue` pollutes the working tree.** `lsms_library/country.py:168` appends to `lsms_library/ISSUES.md` on any cache/materialization failure. The file is tracked in git with no dedup or rotation, so expected data-unavailability conditions (Nepal has no `.dta` on disk, etc.) dirty the tree. Commit mid-session with explicit `git add <files>`, and revert spurious diffs with `git checkout HEAD -- lsms_library/ISSUES.md`. Tracked in GH #148.

## Pandas 3.0 Targets

This codebase targets pandas 3.0+. Headline rules — for the full breakdown see cq unit `ku_c12795f626444715a6d8b71acc657b60`:

- No `inplace=True` anywhere (it's removed in 3.0).
- `pd.NA`, not `np.nan`, for missing values in string / ID / categorical columns. `np.nan` is still fine for numeric floats.
- `pd.isna()` / `pd.notna()` over `np.isnan()` — the latter raises `TypeError` on `pd.NA`.
- `.bfill()` / `.ffill()`, not `fillna(method=...)` (removed in 2.0).
- Use `df.loc[mask, 'col'] = val`, not `df[mask]['col'] = val` — chained indexing silently fails under CoW.
- `.iloc[0]` for positional Series access; `series[0]` is deprecated.

## Countries Without Microdata

Some countries have configs but no source `.dta` in the repository:

| Country          | Reason                            | Source                                                       |
|------------------|-----------------------------------|--------------------------------------------------------------|
| Nepal            | NSO hosts data, not WB            | https://microdata.nsonepal.gov.np/ (free registration)       |
| Armenia          | No data files downloaded          | WB catalog, external hosting                                 |
| Timor-Leste 2001 | No `_/` config for this wave      | WB catalog                                                   |
| Guatemala        | No PSU/cluster variable in data   | ENCOVI 2000 design                                           |

## Design / Skunkworks References

- `SkunkWorks/dvc_object_management.org` — v0.8.0 content-hash cache invalidation plan; retires the stage layer.
- `SkunkWorks/dvcfilesystem_runtime_override.org` — how the pip-install scenario works (runtime config override, lazy credential validation, no git ancestor required).
- `SkunkWorks/cross_country_label_harmonization.org` — design sketch for `Feature(...)(harmonize=...)`.
