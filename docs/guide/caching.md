# Caching and Performance

## How Caching Works

When you call a table method (e.g. `uga.food_expenditures()`), the library
reads the relevant source files, harmonizes them, and writes the result as a
Parquet file under `data_root()`. Subsequent calls **within the same Python
session** are faster because Country-level ancillary lookups (sample weights,
panel IDs, market index) are cached in memory and the operating system keeps
recently-read source files in its page cache.

```python
uga = ll.Country('Uganda')

# First call: builds from source (seconds to minutes depending on country)
hh = uga.household_characteristics()

# Subsequent calls in the same session: faster via in-memory state
hh = uga.household_characteristics()
```

## Cross-Session Behavior (Important)

As of v0.6.0 the across-session read path for the Parquet cache is inconsistent:

- Countries that use the DVC materialize-stage pipeline --- Uganda, Senegal,
  Malawi, Togo, Kazakhstan, Serbia, GhanaLSS --- reload their cached Parquets
  on the next session via DVC's stage-status check.
- Other countries rebuild from source on every new session, even when a
  fresh Parquet is sitting in the cache directory. This is a known gap and
  the subject of the v0.7.0 release.

**v0.7.0** adds a uniform "read cache if present" layer so every country
benefits from the cache across sessions, plus an `LSMS_NO_CACHE=1` environment
variable for contributors editing source data who need to force rebuilds.
**v0.8.0** adds content-hash invalidation so that editing a wave's config or
source file correctly invalidates only the affected tables.

Until v0.7.0 ships, the `trust_cache=True` option (below) is the most
reliable way to force a fast cache read regardless of country, provided you
know the cache is fresh.

## Cache Location

Cached files are stored under the user data directory:

- **Linux**: `~/.local/share/lsms_library/{country}/var/{dataset}.parquet`
- **macOS**: `~/Library/Application Support/lsms_library/...`
- **Windows**: `%LOCALAPPDATA%/lsms_library/...`

Override with the `LSMS_DATA_DIR` environment variable or the `data_dir` key
in `~/.config/lsms_library/config.yml`. The env var takes precedence.

## Trust Cache Mode

On clusters (or any host) where pre-built Parquet files already exist and you
know they reflect current source data, skip validation entirely:

```python
uga = ll.Country('Uganda', trust_cache=True)
```

This reads the cached Parquet directly with no staleness check and no
attempt to invalidate. If a requested Parquet is missing, the usual build
path runs automatically. **Do not use `trust_cache=True`** after editing
source data or a wave's configuration unless you have cleared the affected
caches first --- you will silently get stale results.

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

When contributing changes to a wave's `data_info.yml` or `_/{table}.py`,
clear the affected country's cache before rebuilding so the next call
actually reads your new source data rather than an older cached parquet.

## Build Backends

The library supports several build paths; which one runs depends on the
country and environment:

| Backend | When Used | Description |
|---------|-----------|-------------|
| DVC stage layer | Default, for the 7 countries with a populated `dvc.yaml` | DVC hashes stage deps (Python files + country config dir) and reads cache when clean |
| Python aggregator | Default, for countries without `dvc.yaml` | Reads wave-level `data_info.yml` and builds in memory; cache is currently write-only for this path until v0.7.0 |
| Make / script | `LSMS_BUILD_BACKEND=make`, or when `data_scheme.yml` marks a table `materialize: make` | Per-country/wave Makefiles and scripts; the only choice for tables that cannot be expressed purely in YAML |

```bash
# Bypass the DVC stage layer for a country that has one
export LSMS_BUILD_BACKEND=make
```

## Build Parallelism

Make-based builds default to half of available CPU cores. Override with:

```bash
export LSMS_MAKE_JOBS=4
```
