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

## Three Cache Tiers

The library maintains three independent caches, all rooted under
`data_root()`:

| Tier | Path | Content | Populated by | Read by |
|---|---|---|---|---|
| **L2-country (parquet)** | `~/.local/share/lsms_library/{Country}/var/{table}.parquet` | Per-country, per-table harmonized output (waves concatenated, ready for analysis) | `Country.{table}()` after building from waves | The v0.7.0 top-of-function cache read in `load_dataframe_with_dvc` (hash-gated since v0.8.0) |
| **L2-wave (parquet)** | `~/.local/share/lsms_library/{Country}/{wave}/_/{table}.parquet` | Per-wave harmonized output (one wave's portion of the table) | `Wave.grab_data()` on first call (YAML path, since 2026-04-15) or `_/{table}.py` scripts via `to_parquet()` (script path, always) | `Wave.grab_data()` on subsequent calls; `run_make_target` for script-path tables |
| **L1 (DVC blobs)** | `~/.local/share/lsms_library/dvc-cache/{md5[:2]}/{md5[2:]}` | Content-addressed copies of the raw `.dta` files (and other DVC-tracked sources) | `_ensure_dvc_pulled()` in `local_tools.py` downloads the blob **directly from S3** (sidecar md5 + the active DVC remote's fsspec backend) on first read, bypassing the DVC CLI / `Repo.fetch` (v0.7.3) | `DVCFS.open()` via `DataFileSystem._get_fs_path`'s `typ == "cache"` branch |

All three tiers live under the same root, so a single `LSMS_DATA_DIR`
environment variable moves them together. On a shared cluster, set
`LSMS_DATA_DIR=/shared/...` and every user gets the same warm caches.

### How they interact

The fast path on warm reads is L2-country alone: the v0.7.0 top-of-function
read in `load_dataframe_with_dvc` returns the per-country aggregated
parquet without ever touching the lower tiers. All-warm calls finish
in ~0.5 s regardless of how many raw files the country has.

When L2-country is cold (e.g. after editing a `data_info.yml` and clearing
the parquet cache), the library re-runs wave aggregation. For each wave
in turn, `Wave.grab_data()` checks L2-wave first and reads it directly
on a hit. On an L2-wave miss the wave script calls `get_dataframe()` for
its raw `.dta` files; on an L1 hit the file is served from the local
blob cache via `DVCFS.open()`, on an L1 miss `_ensure_dvc_pulled`
downloads the blob directly from S3 (via the sidecar md5, bypassing
`Repo.fetch`) and then `DVCFS.open` reads from it.
After the first cold rebuild, L1 and L2-wave both stay warm across
sessions, so the next L2-country rebuild doesn't need any S3 traffic
or repeated DVC SQLite lookups.

Empirical numbers on a Linux workstation, Niger `household_roster`
(~10 raw `.dta` files):

| Scenario | Time | What's running |
|---|---|---|
| Truly cold (no caches) | ~600 s | First-ever fetch, every blob from S3, full DVC index build |
| Cold-cold (L1 + L2-country both empty) | 460-510 s | Per-file blob fetch + harmonization + parquet write |
| **L2-country cold, L1 warm** | **~70 s** | Sidecar pre-check finds blobs in cache, no S3 traffic, harmonization + parquet write only |
| All-warm (L2-country hit) | ~0.5 s | v0.7.0 top read returns the parquet directly |

> **Note (v0.7.3+):** the two cold rows were measured in the
> `Repo.fetch` era (~11 s/blob). The direct-S3 bypass now fetches each
> blob in ~0.1 s and lock-free, so cold builds are dominated by
> harmonization, not fetching — the absolute cold numbers are lower than
> shown. The relative ordering still holds.

The interesting row is "L2-country cold, L1 warm": much faster than
cold-cold, because the per-file fetch overhead and the S3
round-trips are both gone. Adding the L2-wave tier (2026-04-15) further
removes the per-wave DVC SQLite metadata lookup on cold L2-country
rebuilds (Uganda `cluster_features`: 408 s → 0.02 s per wave).

## Cross-Session Behavior

As of v0.7.0, the library reads `{data_root}/{Country}/var/{table}.parquet`
at the top of `load_dataframe_with_dvc` when the file exists **and its
embedded content hash is fresh (or absent)** — the v0.8.0 staleness check
described next; before v0.8.0 this read was unconditional. Every country
benefits from this; there is no longer a special-case for the seven
DVC-stage countries.

**Automatic content-hash staleness (v0.8.0).** Each cached parquet
carries an embedded content hash (in its pyarrow schema metadata) that
covers everything determining its *pre-finalize* contents: the
`LSMS_CACHE_SCHEMA` version, the wave's `data_info.yml`, the country's
`data_scheme.yml` and `Makefile`, the wave and country modules
(`{wave_folder}.py`, `{country}.py`, `mapping.py`), any `_/{table}.py`
script, the build-time `*.org` inputs in `_/` (`food_items.org`,
`categorical_mapping.org`, … — `CONTENTS.org` excluded), and the **DVC
sidecar md5** of each declared source file (the `.dta` itself is never
hashed or downloaded). On read,
the hash is recomputed from the current sources and compared:

- **match** → the cached parquet is served (fast path preserved);
- **mismatch** → the cache is treated as stale and rebuilt from source;
- **no embedded hash** (a parquet built before v0.8.0) → trusted once and
  re-stamped, so the *next* read is guarded (no rebuild-all on upgrade).

So editing a `data_info.yml`, a wave/country `_/{table}.py` script, the
country module, or a source `.dta` (via its sidecar) now invalidates the
cache automatically — no manual step required. The recompute is cheap
(~1–4 ms; small config/sidecar reads only, no `.dta` hashing, no S3, no
directory walk), preserving the v0.7.0 fast-path win.

**Known limitations.** (1) Script-path tables whose sources are
referenced via *dynamically-constructed* paths (not string literals in
the `_/{table}.py`) are covered only by hashing the script text, not the
source content — an out-of-band edit to such a source still needs a
manual `cache clear`. (2) `assume_cache_fresh=True` deliberately skips
the hash check ("I promise the cache is current"). (3) `LSMS_NO_CACHE=1`
still forces a full rebuild.

You can still invalidate manually:

- Set `LSMS_NO_CACHE=1` for the session to force a rebuild from source.
- Run `lsms-library cache clear --country {Country}` (optionally with
  `--method {table}`) to evict the affected L2-country and L2-wave
  parquets before the next call.

Either way, the rebuild reads from L1 (the DVC blob cache) where
possible, so you only pay the S3 cost once per blob per machine.

## Cache Location

All three tiers are under the user data directory:

- **Linux**: `~/.local/share/lsms_library/`
  - L2-country: `{Country}/var/{dataset}.parquet`
  - L2-wave: `{Country}/{wave}/_/{dataset}.parquet`
  - L1: `dvc-cache/{md5[:2]}/{md5[2:]}` (DVC 2.x layout) or
    `dvc-cache/files/md5/{md5[:2]}/{md5[2:]}` (DVC 3.x layout)
- **macOS**: `~/Library/Application Support/lsms_library/...`
- **Windows**: `%LOCALAPPDATA%/lsms_library/...`

Override with the `LSMS_DATA_DIR` environment variable or the `data_dir`
key in `~/.config/lsms_library/config.yml`. The env var takes precedence.

This controls only the **data** root (caches + DVC blobs). The **config
tree** (`countries/{C}/_/...`) is separately overridable via
`LSMS_COUNTRIES_ROOT` / `countries_dir` (GH #436) — see CLAUDE.md "Data
Access". Keeping the two independent is what lets a git worktree read its
own config while sharing the warm cache.

The library handles both DVC cache layouts because legacy `.dvc` sidecars
in the `countries/` repo carry `md5-dos2unix` hashes from before the DVC
3.0 cutover and write to the flat layout, while newer sidecars use the
`files/md5/` subpath. Users don't need to think about this; the
sidecar pre-check in `_ensure_dvc_pulled` looks at both locations.

## The package tree never contains DVC-tracked data

This is a hard architectural rule: the package source tree
(`lsms_library/countries/`) holds code, configs, and `.dvc` sidecars
only. Tracked data files live in the L1 cache under `data_root()`,
not in the workspace.

`_ensure_dvc_pulled` downloads blobs directly from S3 (via the sidecar
md5, bypassing the DVC CLI / `Repo.fetch`) to populate the cache without
checking files out into the package tree. And
`local_file()` inside `get_dataframe` refuses to use any workspace copy
of a file that has a sister `.dvc` sidecar — if it finds one, it emits
a `UserWarning` with a cleanup command and falls through to the DVC
cache path.

If you see warnings like:

> Refusing workspace copy of DVC-tracked file ... (sister .dvc sidecar
> exists). The package tree must not contain DVC-tracked data; falling
> through to the DVC cache path. Clean up with: `find lsms_library/countries -type f -name '*.dta' -execdir test -e '{}.dvc' \; -print -delete`

it means your dev checkout has leftover `.dta` files from a prior
`dvc pull` (or from an older version of the library that checked files
out via `Repo.pull`). Run the cleanup command to
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
`lsms-library cache clear` evicts both L2-country and L2-wave; the L1
blob cache stays populated either way, so the rebuild won't re-fetch
from S3.

To clear the L1 blob cache too (rarely needed):

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
now in the L1 cache and `local_file()` will refuse to read the
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
| Python aggregator | Default for all countries | Reads wave-level `data_info.yml` and builds in memory; the L2-country parquet is written and read across sessions via the v0.7.0 top read, and each wave's intermediate is cached as L2-wave |
| DVC stage layer | The 7 countries with a populated `dvc.yaml` (Uganda, Senegal, Malawi, Togo, Kazakhstan, Serbia, GhanaLSS) | DVC hashes stage deps and reads cache when clean. Note: on hosts where `python3` resolves to Python < 3.9 (e.g. Savio login nodes with Python 3.6.8), `stage.reproduce()` fails with `ModuleNotFoundError` and the library silently falls back to the Python aggregator path |
| Make / script | When `data_scheme.yml` marks a table `materialize: make` | Per-country/wave Makefiles and standalone Python scripts; the only choice for tables that cannot be expressed purely in YAML (post-planting/post-harvest dual rounds, multi-wave source files, etc.). These also write L2-wave parquets via `to_parquet()` |

```bash
# Force the Python aggregator path (bypasses the DVC stage layer for
# the 7 stage-configured countries; mostly useful for debugging stage
# resolution issues)
export LSMS_BUILD_BACKEND=make
```

Note that `LSMS_BUILD_BACKEND=make` dispatches `_aggregate_wave_data`
directly to `load_from_waves` (`country.py:1830`), bypassing both the
DVC stage layer and both L2 parquet tiers. L1 is still used to serve
raw blobs. Use only when actively debugging.

## Build Parallelism

Make-based builds default to half of available CPU cores. Override with:

```bash
export LSMS_MAKE_JOBS=4
```
