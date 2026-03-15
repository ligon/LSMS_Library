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
- `LSMS_USE_DVC_CACHE=false` disables caching for debugging.
- On clusters: `use_parquet=True` in `Country()` reads existing parquets directly.

## Adding New Surveys
New surveys are added via YAML config files under `lsms_library/countries/`, not Python code.

## Two Makefiles
- Top-level `Makefile`: Poetry setup, pytest, build.
- `lsms_library/Makefile`: Country-specific operations (test, build, materialize, demands).
  Use `make -C lsms_library help` for details.

## Data Access
Contact ligon@berkeley.edu for credentials. Contributors need GPG/PGP keys.
