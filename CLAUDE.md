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

## Pandas Conventions (>=3.0)
This codebase targets pandas 3.0+. Follow these rules in all new and modified code:

- **No `inplace=True`**: Use `df = df.set_index(...)` instead of `df.set_index(..., inplace=True)`. The `inplace` parameter is removed in pandas 3.0.
- **Use `pd.NA`, not `np.nan`, for missing values in string columns**: Pandas 3.0 defaults to `StringDtype` (PyArrow-backed) where `np.nan` is not a valid sentinel. Use `pd.NA` when replacing/filling missing values in string, ID, or categorical columns (e.g., `.replace('', pd.NA)`, `.replace('nan', pd.NA)`). `np.nan` is still fine for numeric (float) columns.
- **Use `pd.isna()` / `pd.notna()`, not `np.isnan()`**: `np.isnan()` raises `TypeError` on `pd.NA`. The pandas functions handle both `np.nan` and `pd.NA`.
- **Use `.bfill()` / `.ffill()`, not `.fillna(method=...)`**: The `method` parameter was removed in pandas 2.0.
- **No chained indexing for writes**: Copy-on-Write (CoW) is default in pandas 3.0. `df[mask]['col'] = val` silently fails. Use `df.loc[mask, 'col'] = val` instead. For reads, prefer `df.loc[mask, 'col'].iloc[0]` over `df.loc[mask, :]['col'][0]`.
- **No mutating views**: Do not modify DataFrames obtained from `_get_numeric_data()`, `select_dtypes()`, or `groupby()` and expect changes to propagate. Work on `df` directly or assign results back explicitly.
