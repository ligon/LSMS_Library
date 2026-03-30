# Caching and Performance

## How Caching Works

The first time you call a table method (e.g. `uga.food_expenditures()`), the
library builds the dataset from source files and caches the result as a Parquet
file. Subsequent calls read from cache in under a second.

```python
uga = ll.Country('Uganda')

# First call: builds from source (~30 seconds)
hh = uga.household_characteristics()

# Subsequent calls: reads from cache (<1 second)
hh = uga.household_characteristics()
```

## Cache Location

Cached files are stored under the user data directory:

- **Linux**: `~/.local/share/lsms_library/{country}/var/{dataset}.parquet`
- **macOS**: `~/Library/Application Support/lsms_library/...`
- **Windows**: `%LOCALAPPDATA%/lsms_library/...`

Override with the `LSMS_DATA_DIR` environment variable.

## Cache Validation

Caches are validated using DVC's hash-based dependency tracking. If source data
or configuration files change, the cache is automatically invalidated and
rebuilt.

## Trust Cache Mode

On clusters with pre-built Parquet files, skip validation entirely:

```python
uga = ll.Country('Uganda', trust_cache=True)
```

If a requested Parquet is missing, the usual build path runs automatically.

## Managing Cache

Use the CLI to inspect or clear cached files:

```bash
# List cached datasets
lsms-library cache list --country Uganda

# Remove a specific cache
lsms-library cache clear --country Uganda --method shocks

# Dry-run removal for everything
lsms-library cache clear --all --dry-run
```

## Build Backends

The library supports multiple build backends:

| Backend | When Used | Description |
|---------|-----------|-------------|
| DVC (default) | Normal use | Hash-validated, remote-aware |
| Make | `LSMS_BUILD_BACKEND=make` | Direct build, useful for debugging |
| Legacy Makefile | `materialize: make` in data_scheme | Per-country/wave Makefiles |

```bash
# Bypass DVC and build directly with Make
export LSMS_BUILD_BACKEND=make
```

## Build Parallelism

Make-based builds default to half of available CPU cores. Override with:

```bash
export LSMS_MAKE_JOBS=4
```
