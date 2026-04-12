# Caching and Performance

## How Caching Works

When you call a table method (e.g. `uga.food_expenditures()`), the library
reads the relevant raw data files, harmonizes them, and writes the result
as a Parquet file under `data_root()`. Subsequent calls in the same
process return immediately from in-memory state; subsequent calls in a
fresh process read the Parquet directly without re-running the
harmonization. The raw `.dta` blobs themselves are also cached locally,
so cold rebuilds (e.g. after editing a wave's `data_info.yml`) don't have
to re-download from S3.

```python
import lsms_library as ll
uga = ll.Country('Uganda')

# First call: builds from source (10s of seconds to minutes depending on country)
hh = uga.household_characteristics()

# Subsequent calls in the same session: in-memory state, ~0 cost
hh = uga.household_characteristics()
```

## Two Cache Layers

The library maintains two independent caches, both rooted under
`data_root()`:

| Layer | Path | Content | Populated by | Read by |
|---|---|---|---|---|
| **Layer 2 (parquet)** | `~/.local/share/lsms_library/{Country}/var/{table}.parquet` | Per-table harmonized output, ready for analysis | `Country.{table}()` after building from waves | The v0.7.0 top-of-function cache read in `load_dataframe_with_dvc` (`country.py:1611-1652`) |
| **Layer 1 (DVC blobs)** | `~/.local/share/lsms_library/dvc-cache/{md5[:2]}/{md5[2:]}` | Content-addressed copies of the raw `.dta` files (and other DVC-tracked sources) | `_ensure_dvc_pulled()` in `local_tools.py` calls `Repo.fetch` on first read of any DVC-tracked file | `DVCFS.open()` via `DataFileSystem._get_fs_path`'s `typ == "cache"` branch |

Both layers live under the same root, so a single `LSMS_DATA_DIR`
environment variable moves them together. On a shared cluster, set
`LSMS_DATA_DIR=/shared/...` and every user gets the same warm caches.

### How they interact

The fast path on warm reads is Layer 2 alone: the v0.7.0 top-of-function
read in `load_dataframe_with_dvc` returns the harmonized parquet without
ever touching DVC. All-warm calls finish in ~0.5 s regardless of how
many raw files the country has.

When Layer 2 is cold (e.g. after editing a `data_info.yml` and clearing
the parquet cache), the library re-runs wave aggregation. Each wave
script calls `get_dataframe()` for its raw `.dta` files; on a cache hit
the file is served from Layer 1's local copy via `DVCFS.open()`, on a
miss `_ensure_dvc_pulled` runs `Repo.fetch` to populate the cache and
then `DVCFS.open` reads from it. After the first cold rebuild, Layer 1
stays warm across sessions, so the next L2 rebuild doesn't need any
S3 traffic.

Empirical numbers on a Linux workstation, Niger `household_roster`
(~10 raw `.dta` files):

| Scenario | Time | What's running |
|---|---|---|
| Truly cold (no caches) | ~600 s | First-ever fetch, every blob from S3, full DVC index build |
| Cold-cold (L1 + L2 both empty) | 460-510 s | Per-file `Repo.fetch` (~11 s each) + harmonization + parquet write |
| **L2-cold L1-warm** | **~70 s** | Sidecar pre-check finds blobs in cache, no S3 traffic, harmonization + parquet write only |
| All-warm (L2 hit) | ~0.5 s | v0.7.0 top read returns the parquet directly |

The interesting row is L2-cold L1-warm: ~7× faster than cold-cold,
because the per-file `Repo.fetch` overhead and the S3 round-trips are
both gone.

## Cross-Session Behavior

As of v0.7.0, the library reads `{data_root}/{Country}/var/{table}.parquet`
unconditionally at the top of `load_dataframe_with_dvc`
(`lsms_library/country.py:1611-1652`) when the file exists. Every country
benefits from this; there is no longer a special-case for the seven
DVC-stage countries.

**There is no automatic staleness check.** When you edit a wave's
`data_info.yml`, a `_/{table}.py` script, or some other source, the
library cannot tell that the source changed. Two ways to invalidate:

- Set `LSMS_NO_CACHE=1` for the session — this skips the v0.7.0 top read
  and forces a Layer 2 rebuild from source.
- Run `lsms-library cache clear --country {Country}` (optionally with
  `--method {table}`) to evict the affected parquet before the next call.

Either way, the rebuild reads from Layer 1 (the DVC blob cache) where
possible, so you only pay the S3 cost once per blob per machine.

Content-hash invalidation that automatically detects edits to
`data_info.yml` and `_/{table}.py` is planned for v0.8.0.

## Cache Location

Both layers are under the user data directory:

- **Linux**: `~/.local/share/lsms_library/`
  - Layer 2: `{Country}/var/{dataset}.parquet`
  - Layer 1: `dvc-cache/{md5[:2]}/{md5[2:]}` (DVC 2.x layout) or
    `dvc-cache/files/md5/{md5[:2]}/{md5[2:]}` (DVC 3.x layout)
- **macOS**: `~/Library/Application Support/lsms_library/...`
- **Windows**: `%LOCALAPPDATA%/lsms_library/...`

Override with the `LSMS_DATA_DIR` environment variable or the `data_dir`
key in `~/.config/lsms_library/config.yml`. The env var takes precedence.

The library handles both DVC cache layouts because legacy `.dvc` sidecars
in the `countries/` repo carry `md5-dos2unix` hashes from before the DVC
3.0 cutover and write to the flat layout, while newer sidecars use the
`files/md5/` subpath. Users don't need to think about this; the
sidecar pre-check in `_ensure_dvc_pulled` looks at both locations.

## The package tree never contains DVC-tracked data

This is a hard architectural rule: the package source tree
(`lsms_library/countries/`) holds code, configs, and `.dvc` sidecars
only. Tracked data files live in the Layer 1 cache under `data_root()`,
not in the workspace.

`_ensure_dvc_pulled` calls `Repo.fetch` (not `Repo.pull`) to populate
the cache without checking files out into the package tree. And
`local_file()` inside `get_dataframe` refuses to use any workspace copy
of a file that has a sister `.dvc` sidecar — if it finds one, it emits
a `UserWarning` with a cleanup command and falls through to the DVC
cache path.

If you see warnings like:

> Refusing workspace copy of DVC-tracked file ... (sister .dvc sidecar
> exists). The package tree must not contain DVC-tracked data; falling
> through to the DVC cache path. Clean up with: `find lsms_library/countries -type f -name '*.dta' -execdir test -e '{}.dvc' \; -print -delete`

it means your dev checkout has leftover `.dta` files from a prior
`dvc pull` (or from an older version of the library that did
`Repo.pull` instead of `Repo.fetch`). Run the cleanup command to
remove them. The cache under `data_root()` already has the same
content; the warnings will stop and reads will use the canonical
cache path.

## Assume Cache Fresh Mode

```python
uga = ll.Country('Uganda', assume_cache_fresh=True)
```

With v0.7.0 in place, `assume_cache_fresh=True` is mostly redundant with the
default behavior. It remains useful as a stricter bypass that skips even
the `LSMS_NO_CACHE` check.

What `assume_cache_fresh=True` does **not** skip: `_finalize_result`. Kinship
expansion, canonical spelling normalization, dtype coercion, and the
`_join_v_from_sample` augmentation all still apply on read. The returned
DataFrame is **not** byte-identical to the on-disk parquet — the parquet
is closer to the raw source data, and `_finalize_result` is the
harmonization layer applied at every read.

**Do not use `assume_cache_fresh=True`** after editing source data or a wave's
configuration unless you have cleared the affected caches first — you
will silently get stale results.

> **Deprecated**: `trust_cache=True` is a legacy alias for `assume_cache_fresh=True`.
> It still works but emits a `DeprecationWarning` and will be removed in v0.8.0.

## Managing Cache

Use the CLI to inspect or clear cached parquets:

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
The Layer 1 blob cache stays populated either way, so the rebuild won't
re-fetch from S3.

To clear the Layer 1 blob cache too (rarely needed):

```bash
rm -rf ~/.local/share/lsms_library/dvc-cache/
```

## Adding new data files

Two workflows for getting new survey data into the library, both of
which write `.dta` (or other source) files into a `Data/` directory
under the package tree as a transient step:

1. **Manual** (per CONTRIBUTING.org). Download the source files,
   place them in the appropriate `lsms_library/countries/{Country}/{wave}/Data/`
   directory, and run `dvc add` to generate the `.dvc` sidecar. Then
   `dvc push` to upload the blob to the S3 remote.
2. **Automated via the World Bank Microdata API**. The
   `data_access.get_data_file()` fallback in `local_tools.py` can
   download missing files from the World Bank Microdata Library
   (requires `MICRODATA_API_KEY`) and add them to DVC for you.

In both cases the new file lives temporarily in the workspace. **Once
the `.dvc` sidecar exists, remove the workspace copy** — the file is
now in the Layer 1 cache and `local_file()` will refuse to read the
workspace copy on subsequent calls (with the cleanup warning). Removing
it keeps the package tree clean and consistent with the architectural
rule above.

```bash
# After dvc add succeeds:
rm lsms_library/countries/Niger/2024-25/Data/new_section.dta
# The blob is in ~/.local/share/lsms_library/dvc-cache/...
# and DVC.open will serve it from there.
```

## Build Backends

The library supports several build paths; which one runs depends on the
country and environment:

| Backend | When Used | Description |
|---------|-----------|-------------|
| Python aggregator | Default for all countries | Reads wave-level `data_info.yml` and builds in memory; Layer 2 parquet is written and read across sessions via the v0.7.0 top read |
| DVC stage layer | The 7 countries with a populated `dvc.yaml` (Uganda, Senegal, Malawi, Togo, Kazakhstan, Serbia, GhanaLSS) | DVC hashes stage deps and reads cache when clean. Note: on hosts where `python3` resolves to Python < 3.9 (e.g. Savio login nodes with Python 3.6.8), `stage.reproduce()` fails with `ModuleNotFoundError` and the library silently falls back to the Python aggregator path |
| Make / script | When `data_scheme.yml` marks a table `materialize: make` | Per-country/wave Makefiles and standalone Python scripts; the only choice for tables that cannot be expressed purely in YAML (post-planting/post-harvest dual rounds, multi-wave source files, etc.) |

```bash
# Force the Python aggregator path (bypasses the DVC stage layer for
# the 7 stage-configured countries; mostly useful for debugging stage
# resolution issues)
export LSMS_BUILD_BACKEND=make
```

Note that `LSMS_BUILD_BACKEND=make` dispatches `_aggregate_wave_data`
directly to `load_from_waves` (`country.py:1830`), bypassing both the
DVC stage layer and the Layer 2 Parquet cache. Use only when actively
debugging.

## Build Parallelism

Make-based builds default to half of available CPU cores. Override with:

```bash
export LSMS_MAKE_JOBS=4
```
