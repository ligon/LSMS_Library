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

## DVC Caching
- First call builds from source, caches to `{country}/var/{dataset}.parquet`.
- Subsequent calls read cache (<1 sec).
- Caches auto-invalidate on source/config changes (hash-based).
- `LSMS_BUILD_BACKEND=make` bypasses DVC and builds directly with Make (useful for debugging).
- On clusters: `trust_cache=True` in `Country()` reads existing parquets directly, skipping all validation.

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

**Known issue**: These scripts write parquets to `_/` under the repo tree (e.g., `Uganda/2005-06/_/food_acquired.parquet`). The `data_root()` migration (March 2026) moved the *runtime cache* to `~/.local/share/lsms_library/` but did not update the legacy scripts. Making `to_parquet()` redirect to `data_root()` risks collisions between script outputs and runtime cache outputs (which apply different post-processing: kinship expansion, spelling normalisation, dtype enforcement). This separation is unresolved.

**Rule of thumb**: If a table can be expressed as column mappings from a single source file per wave, use YAML. If it needs cross-file concatenation, per-row `t` assignment, or multi-wave source files, use a script with `materialize: make`.

## Two Makefiles
- Top-level `Makefile`: Poetry setup, pytest, build.
- `lsms_library/Makefile`: Country-specific operations (test, build, materialize, demands).
  Use `make -C lsms_library help` for details.

## Data Access
Underlying microdata must be obtained from the [World Bank Microdata Library](https://microdata.worldbank.org/) under their terms of use. Contributors need GPG/PGP keys for repository write access.

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
