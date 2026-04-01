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

## Country Symlinks
Root-level symlinks (e.g., `Uganda → lsms_library/countries/Uganda`) are for convenience. Actual config is under `lsms_library/countries/`.

## DVC Repository Root
**The DVC repository is rooted at `lsms_library/countries/`, NOT the top-level repo.** DVC config, remotes, and credentials all live under `lsms_library/countries/.dvc/`. All `dvc` CLI commands (pull, push, status) must be run from `lsms_library/countries/` or they will fail with missing-remote/credential errors. The `get_dataframe()` fallback chain handles this automatically when scripts run from their normal `{Country}/{wave}/_/` working directory.

## DVC Caching and `data_root()`
- All materialized data (parquets, JSON caches) is written under `data_root()` (`lsms_library/paths.py`), **not** in the repo tree.
- Default location: `~/.local/share/lsms_library/{Country}/var/{table}.parquet`. Override with `LSMS_DATA_DIR` env var.
- First call builds from source, caches to `data_root(Country)/var/{table}.parquet`.
- Subsequent calls read cache (<1 sec).
- Caches auto-invalidate on source/config changes (hash-based).
- `LSMS_BUILD_BACKEND=make` bypasses DVC and builds directly with Make (useful for debugging).
- On clusters: `trust_cache=True` in `Country()` reads existing parquets directly, skipping all validation.

## Roster-Derived Tables
`household_characteristics` is **auto-derived from `household_roster`** via `roster_to_characteristics()` in `transformations.py`. It should NOT be registered in `data_scheme.yml` --- the `Country` class detects it via `_ROSTER_DERIVED` and applies the transformation automatically when `household_roster` exists. Adding `household_characteristics: !make` to a data_scheme bypasses this and forces legacy scripts to run instead.

## Adding New Surveys
New surveys are added via YAML config files under `lsms_library/countries/`, not Python code.

## Two Build Paths: YAML vs Makefile/Script

There are two ways a table gets built at the wave level:

### YAML path (`data_info.yml`)
The preferred path for simple cases. `Wave.grab_data()` reads `idxvars`/`myvars` from the wave's `data_info.yml`, calls `df_data_grabber()` to extract columns from the source `.dta` file, applies formatting functions, and returns a DataFrame. The Country class aggregates waves and caches to `data_root()`. **No parquet is written at the wave level.**

### Makefile/script path (legacy Python scripts)
Required for cases the YAML structure cannot express. Indicated by `materialize: make` or `!make` in `data_scheme.yml`. `Wave.grab_data()` falls back to running `make` in the wave's `_/` directory, which executes a standalone Python script (e.g., `household_roster.py`) that calls `to_parquet()` to write a `.parquet` file.

**When the script path is necessary** (do not try to replace these with YAML):

- **Multiple observations per wave**: Nigeria has post-planting and post-harvest rounds (`2018Q3`, `2019Q1`) in a single directory (`2018-19/`). The script loads two different source files, assigns different `t` values to each, concatenates, and deduplicates people who leave between rounds. YAML assumes one directory = one `t`.

- **Multi-wave source files**: Tanzania `2008-15/` has a single `.dta` file covering rounds 1--4. The script reads a `round` column and maps it to wave labels. YAML assumes one file = one wave.

- **Complex transformations**: Some tables (Nigeria `food_acquired`, GhanaLSS `food_acquired`) need elaborate unit conversions, label extraction, or cross-file joins that exceed what `df_data_grabber` supports.

**Data separation**: All parquet writes go to `data_root()`, never the repo tree. `to_parquet()` calls `_resolve_data_path()` (in `local_tools.py`), which inspects the call stack to infer country/wave and rewrites relative paths under `data_root()`. This handles three patterns:
- Bare `foo.parquet` from wave-level scripts → `data_root(Country)/wave/_/foo.parquet`
- `../var/foo.parquet` from country-level scripts → `data_root(Country)/var/foo.parquet`
- `../wave/_/foo.parquet` cross-wave refs → `data_root(Country)/wave/_/foo.parquet`

Users can override the location via the `LSMS_DATA_DIR` env var, which `data_root()` reads. Some stale parquets from before this migration still exist in-tree under `_/` directories — they are harmless artifacts.

**Rule of thumb**: If a table can be expressed as column mappings from a single source file per wave, use YAML. If it needs cross-file concatenation, per-row `t` assignment, or multi-wave source files, use a script with `materialize: make`.

## Post-Planting / Post-Harvest (PP/PH) Countries

Several LSMS-ISA countries collect data in two rounds per wave: **post-planting (pp)** and **post-harvest (ph)**. The same household is visited twice --- once after the planting season and once after harvest. This is a cross-cutting structural concern that affects every feature in those countries and is the single most common source of duplicate-index bugs.

### Which countries have dual-round structure

| Country | Program | Waves affected | File naming pattern | Notes |
|---------|---------|----------------|---------------------|-------|
| **Nigeria** | GHS/LSMS-ISA | All waves | `sect*_plantingw*.dta` / `sect*_harvestw*.dta` | Wave directories like `2018-19/` contain both pp and ph data |
| **Ethiopia** | ESS | All 5 waves | `sect*_pp_w*.dta` / `sect*_ph_w*.dta` | Heavy `!make` usage --- most features need scripts |
| **Tanzania** | NPS | `2008-15/` only | Single file with `round` column covering rounds 1--4 | Later waves (`2019-20`, `2020-21`) are single-round |
| **GhanaSPS** | SPS | Some waves | Planting/harvest questionnaires | Less structured than Nigeria/Ethiopia |

### Why YAML cannot express this

The YAML path (`data_info.yml`) assumes **one directory = one `t` value**. In pp/ph countries, a single wave directory (e.g., `Nigeria/2018-19/`) contains two source files that need **different `t` values** (e.g., `2018Q3` for post-planting, `2019Q1` for post-harvest). The YAML path has no mechanism to:

1. Load two different source files from the same directory
2. Assign a different `t` value to each
3. Concatenate the results and deduplicate

This is why pp/ph features **must use `materialize: make`** (or `!make`) with a Python script.

### How pp/ph affects index construction

Each round must receive a **distinct `t` value** so that the same household appearing in both rounds does not create duplicate index entries. The standard patterns are:

- **Nigeria**: Quarter-based `t` values --- `2018Q3` (post-planting) and `2019Q1` (post-harvest)
- **Ethiopia**: Wave label reuse (e.g., `2018-19`) since most features only use one round's data
- **Tanzania `2008-15/`**: The script reads a `round` column and maps values to wave labels (`2008-09`, `2010-11`, `2012-13`, `2014-15`)

### The duplicate-index bug

This is the most common bug in pp/ph countries. It occurs when both pp and ph data are loaded but assigned the **same `t` value**:

```
# BUG: Both rounds get t='2018-19' → household appears twice
pp['t'] = '2018-19'
ph['t'] = '2018-19'
df = pd.concat([pp, ph])   # → 50-87% duplicate indices
```

Symptoms:
- `df.index.duplicated().mean()` returns 0.50--0.87
- `is_this_feature_sane()` reports massive duplicate rates
- Household counts are roughly double the expected number

### How to fix: the script pattern

The correct pattern assigns distinct `t` values to each round, concatenates, and deduplicates:

```python
from lsms_library.local_tools import df_data_grabber, to_parquet

# Post-planting: assign t='2018Q3'
idxvars_pp = dict(i='hhid', t=('hhid', lambda x: '2018Q3'), v='ea', pid='indiv')
myvars_pp = dict(Sex=('s1q2', extract_string), Age='s1q6', Relationship=('s1q3', extract_string))
pp = df_data_grabber('../Data/sect1_plantingw4.dta', idxvars_pp, **myvars_pp)

# Post-harvest: assign t='2019Q1'
idxvars_ph = dict(i='hhid', t=('hhid', lambda x: '2019Q1'), v='ea', pid='indiv')
myvars_ph = dict(Sex=('s1q2', extract_string), Age='s1q4', Relationship=('s1q3', extract_string))
ph = df_data_grabber('../Data/sect1_harvestw4.dta', idxvars_ph, **myvars_ph)

# Concatenate and drop people who left between rounds
df = pd.concat([pp, ph])
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'household_roster.parquet')
```

Key details:
- The `t` lambda assigns a constant string to every row from that file
- Variable names may differ between pp and ph files (e.g., Nigeria's `s1q6` vs `s1q4` for Age)
- `dropna(how='all')` removes individuals who appeared in one round but have no data in the other (attrition between rounds)
- The `data_scheme.yml` must use `materialize: make` for these features

### Reference implementations

- **Nigeria `household_roster`**: `Nigeria/2018-19/_/household_roster.py` --- canonical pp/ph pattern with distinct `t` values and attrition handling
- **Nigeria `food_acquired`**: `Nigeria/2018-19/_/food_acquired.py` --- pp/ph with food item and unit harmonization across rounds
- **Tanzania `2008-15/`**: Multi-round single-file pattern with `round` column mapping

## Joining `v` (cluster) onto tables that lack it

Many roster source files (e.g., Uganda's gsec2) don't carry a cluster column. Join `v` from the survey cover page (e.g., gsec1) using the `dfs:` merge in `data_info.yml`:

```yaml
household_roster:
    dfs:
        - df_roster
        - df_cluster
    df_roster:
        file: ../Data/HH/gsec2.dta
        idxvars:
            i: hhid
            pid: pid
        myvars:
            Sex: h2q3
            Relationship: h2q4
            Age: h2q8
    df_cluster:
        file: ../Data/HH/gsec1.dta
        idxvars:
            i: hhid
        myvars:
            v: s1aq04a        # <-- v as myvar, NOT idxvar
    merge_on:
        - i
    final_index:
        - t
        - v
        - i
        - pid
```

Key details:
- Put `v` in `myvars` of the sub-df, not `idxvars`. An `idxvars`-only sub-df with empty `myvars` fails in `df_data_grabber`.
- The cluster column name changes across waves within a country (e.g., Uganda uses `comm`, `h1aq4a`, `parish_code`, `parish_name`, `s1aq04a` across its 8 waves) because sampling schemes evolve.
- **The `data_scheme.yml` must include `v` in the index** (e.g., `index: (t, v, i, pid)`). If it doesn't, `_normalize_dataframe_index` silently drops `v` from the result.

## Two Makefiles
- Top-level `Makefile`: Poetry setup, pytest, build.
- `lsms_library/Makefile`: Country-specific operations (test, build, materialize, demands).
  Use `make -C lsms_library help` for details.

## Data Access
Underlying microdata must be obtained from the [World Bank Microdata Library](https://microdata.worldbank.org/) under their terms of use. Contributors need GPG/PGP keys for repository write access.

### Reading data files: `get_dataframe()`

**Always use `get_dataframe()` from `local_tools` to read `.dta`/`.csv`/`.parquet` files.** It is the single entry point for reading data and handles all access modes transparently:

```python
from lsms_library.local_tools import get_dataframe

df = get_dataframe('../Data/sect1_hh_w5.dta')
```

The fallback chain is:
1. **Local file** on disk
2. **DVC filesystem** (`DVCFileSystem`) --- streams from the configured DVC remote
3. **`dvc.api.open()`** --- legacy DVC streaming
4. **`get_data_file()`** from `data_access.py` --- downloads from the World Bank Microdata Library NADA API as a last resort (requires `MICRODATA_API_KEY`)

This means a script written with `get_dataframe('../Data/file.dta')` works whether the file is already on disk, cached in DVC, or has never been downloaded at all.

### Anti-patterns (do not use)

| Anti-pattern | Why it's wrong |
|---|---|
| `dvc.api.open(fn, mode='rb')` + `from_dta(f)` | Couples to DVC internals, skips the WB fallback |
| `pd.read_stata('/absolute/path/to/file.dta')` | Breaks on other machines, no DVC/WB fallback |
| `pyreadstat.read_dta(path)` directly | Same --- bypasses all access layers |
| `from_dta('lsms_library/countries/...')` with absolute path | Non-portable; use relative `../Data/` paths |

Scripts in `{Country}/{wave}/_/` run from that directory, so `../Data/file.dta` is the standard relative path convention. `get_dataframe` (via `_resolve_data_path`) knows how to resolve these.

### Writing data files: `to_parquet()`

Use `to_parquet(df, 'feature_name.parquet')` from `local_tools`. It writes to `data_root()` (not the repo tree) via `_resolve_data_path()`, which infers country/wave from the call stack.

### Adding new waves: `data_access` module

The `data_access` module provides functions for discovering and downloading new survey waves:

```python
from lsms_library.data_access import discover_waves, add_wave

discover_waves("Ethiopia")          # What's new on the WB?
add_wave("Ethiopia", "6161")        # Download, dvc add, dvc push
```

`push_to_cache_batch()` handles batched `dvc add` + `dvc push` (dramatically faster than per-file). See CONTRIBUTING.org for the manual workflow.

## Cross-Country Feature Class
The `Feature` class assembles a single harmonized DataFrame across all countries that declare a given table:

```python
import lsms_library as ll

roster = ll.Feature('household_roster')
roster.countries          # ['Ethiopia', 'Mali', 'Niger', 'Uganda', ...]
roster.columns            # required columns from global data_info.yml

df = roster()                       # all countries
df = roster(['Ethiopia', 'Niger'])  # specific countries
```

The returned DataFrame has a `country` index level prepended. `Feature` discovers countries by scanning each `data_scheme.yml` for the table name, then calls `Country(name).{table}()` for each.

This is the preferred way to do cross-country comparisons, validation, and diagnostics.

### Cross-Country Label Harmonization (design intent, not yet implemented)

Within a country, `categorical_mapping.org` maps wave-specific labels to a "Preferred Label" so that the same concept has a consistent name across survey rounds. The cross-country analogue would map country-specific labels (e.g., French `"Sécheresse"` vs English `"Drought"`) to a global canonical label.

**Design:** Top-level mapping tables in `lsms_library/categorical_mapping/`:
- `shocks.yml` — shock type labels across countries
- `food_items.yml` — food item names across countries/languages
- `units.yml` — measurement unit names across countries/languages

Same org-table format as per-country `categorical_mapping.org`:
```
#+NAME: harmonize_shocks
| Preferred Label | Ethiopia    | Niger (EHCVM) | Mali (EHCVM) | Uganda          |
|-----------------+-------------+---------------+--------------+-----------------|
| Drought         | Drought     | Sécheresse    | Sécheresse   | Drought         |
| Flood           | Flood       | Inondation    | Inondation   | Flood           |
```

**API:** The `harmonize` parameter selects which column from the mapping table to use:

```python
import lsms_library as ll

# Raw labels (default) — each country's own labels preserved
shocks = ll.Feature('shocks')()

# Harmonize to canonical English
shocks = ll.Feature('shocks')(harmonize='Preferred')

# Harmonize to French
shocks = ll.Feature('shocks')(harmonize='French')

# Harmonize to aggregate categories (coarser grouping)
food = ll.Feature('food_acquired')(harmonize='Aggregate')
```

This mirrors the within-country `categorical_mapping.org` pattern: the org table has multiple named columns (``Preferred Label``, ``Aggregate``, ``French``, etc.) and the caller chooses which mapping to apply. Uganda's food item tables already use ``Preferred`` vs ``Aggregate`` columns for different granularities.

The mapping table determines what columns are available. If a table has only ``Preferred Label``, that's the only option. A richer table might offer ``Preferred``, ``Aggregate``, ``French``, ``FAO Code``, etc.

**Principle:** The library preserves what the survey says. Cross-country label harmonization is a form of aggregation — the analyst's decision, not the pipeline's. These mappings are convenience tools, not data transformations.

## Canonical Schema (`data_info.yml`)
`lsms_library/data_info.yml` is the single source of truth for cross-country conventions:
- **Required columns** per table (e.g., `household_roster` requires `Sex`, `Age`, `Generation`, `Distance`, `Affinity`)
- **Accepted values** (e.g., `Sex: [M, F]`, `Affinity: [consanguineal, affinal, step, foster, unrelated, guest, servant]`)
- **Rejected spellings** (e.g., `Relation` → use `Generation, Distance, Affinity`; `Effected` → `Affected`)

Tests in `test_schema_consistency.py` read from this file — never hardcode schema rules in tests.

## Kinship Decomposition
`household_roster` uses a decomposed representation of kinship (Kroeber 1909) instead of a single `Relationship` string. Four columns replace one:

| Column | Type | Description |
|--------|------|-------------|
| `Sex` | str | `M` or `F` |
| `Generation` | int | Vertical distance from head (0=same, +1=parent, -1=child) |
| `Distance` | int | Collateral distance (0=lineal, 1=sibling line, 2=cousin) |
| `Affinity` | str | `consanguineal`, `affinal`, `step`, `foster`, `unrelated`, `guest`, `servant` |

The runtime automatically expands any `Relationship` column into these three via `_expand_kinship()` in `_finalize_result()`, using the dictionary in `lsms_library/categorical_mapping/kinship.yml`. Per-wave `data_info.yml` files continue to produce a `Relationship` string from raw data — the decomposition happens transparently.

**Adding new labels:** If a survey has an unrecognized relationship string, a warning is emitted. Add the label to `lsms_library/categorical_mapping/kinship.yml` with its `[Generation, Distance, Affinity]` tuple.

## Automatic Categorical Mappings
If a column or index name in a returned DataFrame matches a table name in the country's `categorical_mapping.org` (case-insensitive), and that table has a `Preferred Label` column, the mapping is applied automatically. No YAML `mappings:` declaration needed.

For tables whose names don't match column names (e.g., `harmonize_food` for index `j`), use the explicit `mappings:` syntax in `data_info.yml`.

## Cross-Country Value Normalisation (`data_info.yml` spellings)
Columns in `data_info.yml` can declare a `spellings` inverse dictionary that the runtime enforces automatically. Each key is the canonical value; its list contains accepted variant spellings:
```yaml
Sex:
  type: str
  required: true
  spellings:
    M: [Male, male, Masculin, masculin, Homme, homme, m]
    F: [Female, female, Féminin, feminin, Femme, femme, f]
```
The canonical values are simply `spellings.keys()`. The runtime replaces variants with canonical forms in `_finalize_result()` via `_enforce_canonical_spellings()`. This applies to both column values and index levels.

## Pandas Conventions (>=3.0)
This codebase targets pandas 3.0+. Follow these rules in all new and modified code:

- **No `inplace=True`**: Use `df = df.set_index(...)` instead of `df.set_index(..., inplace=True)`. The `inplace` parameter is removed in pandas 3.0.
- **Use `pd.NA`, not `np.nan`, for missing values in string columns**: Pandas 3.0 defaults to `StringDtype` (PyArrow-backed) where `np.nan` is not a valid sentinel. Use `pd.NA` when replacing/filling missing values in string, ID, or categorical columns (e.g., `.replace('', pd.NA)`, `.replace('nan', pd.NA)`). `np.nan` is still fine for numeric (float) columns.
- **Use `pd.isna()` / `pd.notna()`, not `np.isnan()`**: `np.isnan()` raises `TypeError` on `pd.NA`. The pandas functions handle both `np.nan` and `pd.NA`.
- **Use `.bfill()` / `.ffill()`, not `.fillna(method=...)`**: The `method` parameter was removed in pandas 2.0.
- **No chained indexing for writes**: Copy-on-Write (CoW) is default in pandas 3.0. `df[mask]['col'] = val` silently fails. Use `df.loc[mask, 'col'] = val` instead. For reads, prefer `df.loc[mask, 'col'].iloc[0]` over `df.loc[mask, :]['col'][0]`.
- **No mutating views**: Do not modify DataFrames obtained from `_get_numeric_data()`, `select_dtypes()`, or `groupby()` and expect changes to propagate. Work on `df` directly or assign results back explicitly.
