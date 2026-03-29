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
