# DESIGN: DVC Layer-1 Caching Is Dormant

**Status:** to be considered
**Branch:** `claude/dvc-caching-investigation-LMjwZ`
**Date:** 2026-04-11

## Summary

Two independent performance problems, both masked today by the
Layer-2 parquet cache under `data_root()`:

1. **Layer-1 (raw `.dta`) caching is dormant.** `DVCFileSystem.open()`
   streams from the remote without populating the local DVC cache;
   `local_tools.DVCFS.open()` never passes the `cache_remote_stream=True`
   kwarg that DVC 3.x requires (added in iterative/dvc#9183). Every
   cold call re-pulls the blob from S3.
2. **DVC setup/teardown is redundant.** `dvc.api.open()` and
   `dvc.repo.Repo()` are expensive to construct (remote-backend
   init, index scan, lock acquisition, config reload), and the
   library reconstructs them on hot paths that could reuse a
   single long-lived handle.

Neither is visible on warm Layer-2 reads, but both bite hard on
any rebuild, fresh materialization, or new process.

## Key facts

- Upstream DVC behavior: `DVCFileSystem.open()` (and `dvc.api.open()`)
  **stream from the remote and do not populate the local DVC cache
  by default.** Caching is **opt-in** via a `cache_remote_stream=True`
  kwarg on `.open()`, added in iterative/dvc#9183 and used only by
  `dvc plots show/diff` inside DVC itself.
- `lsms_library/local_tools.py:247` calls
  `DVCFS.open(fn, mode='rb')` with **no** `cache_remote_stream=True`.
  Same for the `dvc.api.open()` fallback at line 254.
- `lsms_library/local_tools.py:33` constructs `DVCFS` with **no**
  `config` override, so DVC's default cache dir would apply anyway —
  but it's moot, because nothing is ever written into it.

At Layer 1 (raw `.dta` blobs), `lsms_library` is effectively pulling
every file from S3 on every cold call. The only reason performance
feels OK in practice is Layer 2 (the parquet cache under
`data_root()`), which memoizes the *post-extraction* dataframes and
hides the re-fetch cost as long as the parquet is fresh.

### A note on `~/.dvc/` vs `~/.cache/dvc/`

`~/.dvc/` is not a DVC location at all. DVC's default user-wide cache
lives at `~/.cache/dvc/` (XDG-compliant). If neither that nor
`~/.local/share/lsms_library/` exists on a host, it's consistent with
"nothing has ever been cached on this host" — which is the expected
state given the dormant Layer-1 code path.

## Relevant upstream DVC references

- iterative/dvc#9030 — "dvc does not cache stream opened files":
  maintainer confirms *"DVCFileSystem does not support caching, we
  just use fs.open() which only streams."*
- iterative/dvc#9183 — the PR that added opt-in caching. Core change
  is:
  ```python
  kw = {}
  if kwargs.get("cache_remote_stream", False):
      kw["cache_odb"] = repo.cache.local
  return dvc_fs.open(dvc_path, mode=mode, **kw)
  ```
  Only wired up inside `dvc plots show/diff` — not the default.
- iterative/dvc#9382 — `dvc.api.open()` respects a custom `cache.dir`
  only in DVC ≥ 2.58.2 / 3.x. Current pin is `dvc[s3] >=3.67.0,<4.0.0`
  (`pyproject.toml:16`), so this part is fine — but again, moot if
  nothing is cached.

## What is and isn't cached today

| Layer | What it holds | Currently populated? | Notes |
|---|---|---|---|
| 1a. DVC object cache (`~/.cache/dvc/` by default) | Raw `.dta` blobs pulled from S3 | **NO** — `DVCFS.open()` streams without caching | Would need `cache_remote_stream=True` or `dvc pull` CLI |
| 1b. DVC tmp state (`countries/.dvc/tmp/`) | Lock files, rwlock, runs db | Only when DVC CLI or repo operations run | Irrelevant to `DVCFS.open()` streaming |
| 2. Parquet cache under `data_root()` | Post-extraction harmonized tables | **YES, when materialized** — hides the re-fetch cost | `~/.local/share/lsms_library/{Country}/var/{table}.parquet` |
| 3. DVC-validated stage outputs (`country.py:1611-1762`) | Checks via `stage.status()` and reruns `stage.reproduce()` | Same as Layer 2 — uses DVC stages to rebuild parquets | This is the path that internally calls `DVCFS.open()` during a rebuild |

**What this means operationally.** Every time Layer 2 is cold or
invalidated and the rebuild path runs, the underlying `.dta` file is
re-streamed from S3, even if the identical byte-for-byte blob was
just streamed a minute ago from a different code path. You never
pay this cost *twice in the same Python process* for the same table
(Layer 2 captures it), but you pay it **every time you rebuild**, on
**every machine**, for **every user**.

## Diagnostic recipe

Everything below is read-mostly and only creates files in
well-known, self-owned directories. Run from the repo root with
your working `lsms_library` env (the sandbox here has none).

### 1. Confirm where DVC *thinks* its cache is

```bash
# Ask DVC directly (needs the venv where lsms_library is installed)
cd lsms_library/countries
dvc cache dir
dvc config cache.dir           # empty unless explicitly set
dvc config --list              # full resolved config
python -c "import dvc; print(dvc.__version__)"
```

Expected on a default install: `dvc cache dir` prints something like
`/home/you/.cache/dvc` (or `$XDG_CACHE_HOME/dvc`). `dvc config
cache.dir` is empty. DVC version ≥ 3.67.

### 2. Check what's actually on disk *before* any call

```bash
ls -la ~/.cache/dvc 2>&1 || echo "(no DVC cache yet)"
du -sh ~/.cache/dvc 2>&1 || true
ls -la ~/.local/share/lsms_library 2>&1 || echo "(no Layer 2 cache yet)"
ls -la lsms_library/countries/.dvc/           # should have config, tmp/ maybe, NOT cache/
```

If `~/.cache/dvc/` does not exist and `~/.local/share/lsms_library/`
does not exist, this machine has never materialized a table and we
have a clean slate to test.

### 3. Trigger a single cold `get_dataframe()` call and watch the cache

```python
# cold_fetch.py — run once with a clean state
import os, time, shutil, subprocess, pathlib

# Nuke any prior state so the test is unambiguous (DESTRUCTIVE —
# only do this if you don't need your existing caches!)
for p in ["~/.cache/dvc", "~/.local/share/lsms_library"]:
    shutil.rmtree(pathlib.Path(p).expanduser(), ignore_errors=True)

from lsms_library.local_tools import get_dataframe

t0 = time.time()
df = get_dataframe("Uganda/2019-20/Data/HH/gsec1.dta")  # small, fast file
t1 = time.time()
print(f"first call: {t1-t0:.2f}s, rows={len(df)}")

# Now inspect what hit disk
def du(p):
    p = pathlib.Path(p).expanduser()
    if not p.exists(): return "(missing)"
    r = subprocess.run(["du", "-sh", str(p)], capture_output=True, text=True)
    return r.stdout.strip()

print("~/.cache/dvc after call 1:", du("~/.cache/dvc"))
print("~/.local/share/lsms_library after call 1:", du("~/.local/share/lsms_library"))

# Second call — same file, same process
t0 = time.time()
df2 = get_dataframe("Uganda/2019-20/Data/HH/gsec1.dta")
t1 = time.time()
print(f"second call (same process): {t1-t0:.2f}s")
```

**Expected outputs given current code:**

- `~/.cache/dvc` remains empty or missing → confirms `DVCFileSystem`
  is streaming without caching.
- `~/.local/share/lsms_library` may remain empty too, because
  `get_dataframe` on a raw `.dta` path does *not* itself write a
  parquet — that only happens when a higher-level
  `Country('Uganda').{table}()` call runs the aggregation and hits
  `to_parquet()`.
- The second call should take roughly the same wall time as the
  first (both pull from S3). This is the smoking gun.

### 4. Contrast: trigger a `Country.{table}()` call

```python
import lsms_library as ll, time, subprocess, pathlib

def du(p):
    p = pathlib.Path(p).expanduser()
    if not p.exists(): return "(missing)"
    return subprocess.run(["du","-sh",str(p)], capture_output=True, text=True).stdout.strip()

c = ll.Country("Uganda")

t0 = time.time()
df = c.household_roster()
print(f"first:  {time.time()-t0:.2f}s")
print("  dvc cache: ", du("~/.cache/dvc"))
print("  lsms data: ", du("~/.local/share/lsms_library"))

t0 = time.time()
df = c.household_roster()
print(f"second: {time.time()-t0:.2f}s")
```

**Expected:** first call is slow (streams many `.dta` files from S3),
`~/.cache/dvc` stays empty (Layer 1 not cached), but
`~/.local/share/lsms_library/Uganda/var/household_roster.parquet`
appears (Layer 2 populated). Second call is <1 s because Layer 2
serves it.

**If you then `rm ~/.local/share/lsms_library/Uganda/var/household_roster.parquet`
and run again**, the third call should be slow again — and that
slowness is exactly the unavoidable S3 re-fetch that the missing
Layer 1 cache would have prevented.

### 5. Optional: force DVC to cache via the CLI

You can prove Layer 1 *would* work if it were wired up, by using the
DVC CLI directly (which does populate the cache):

```bash
cd lsms_library/countries
dvc pull Uganda/2019-20/Data/HH/gsec1.dta
ls -la ~/.cache/dvc    # should now have files/md5/.. subdirs
du -sh  ~/.cache/dvc
```

If this works but `get_dataframe()` never populates the same
directory, you've confirmed the library-level gap.

## DVC handle latency — a separate problem

`DVCFileSystem(...)` and `dvc.repo.Repo(...)` are *expensive* to
construct. Under the hood they:

- Load `.dvc/config` and resolve remote configuration
- Initialize the S3 (or other) backend client
- Scan the DVC index under `countries/`
- Acquire read locks under `.dvc/tmp/`
- Patch credentials lazily (which is fine but adds branches)

The library correctly constructs **one** module-level `DVCFS` in
`local_tools.py:33`, so within a single process the primary path
(`get_dataframe` → `DVCFS.open()`) reuses it. But there are four
other places where these handles get rebuilt on hot paths.

### Hot spot 1: `Repo()` reconstructed per table load

`lsms_library/country.py:1623` (inside `load_dataframe_with_dvc()`):

```python
with _working_directory(dvc_root):
    repo = Repo(str(dvc_root))
    stage_infos = self._resolve_materialize_stages(method_name, waves)
    ...
```

This runs **once per table rebuild**. A user that loads
`household_roster()`, then `food_acquired()`, then
`household_characteristics()` pays the `Repo()` construction cost
three times even though `self.file_path.parent` is identical on every
call. Memoize a `_dvc_repo` handle on the `Country` instance (or
module-level keyed by `dvc_root`) and reuse it.

### Hot spot 2: fresh `DVCFileSystem` in `data_access.get_data_file()`

`lsms_library/data_access.py:1146-1147`:

```python
from dvc.api import DVCFileSystem
fs = DVCFileSystem(os.fspath(_COUNTRIES_DIR))
```

This is redundant with `local_tools.DVCFS` — same root, same config.
Import and reuse the singleton:

```python
from .local_tools import DVCFS as fs
```

### Hot spot 3: `dvc.api.open()` sprinkled throughout `local_tools.py`

`local_tools.py` still calls `dvc.api.open(fn, mode='rb')` at lines
254, 429, 438, 452, 510, 523, 537, 551, 564, and 598. Each call
internally constructs a fresh `Repo`/`DVCFileSystem`. For the
fallback path at 254 this is at least hidden behind the
`local_file()`/`file_system_path()` branches, but the others are on
direct read paths for specialized formats (food prices, roster
helpers from `lsms.tools`). Replace with `DVCFS.open()` where
possible. The fallback at 254 can be deleted once all direct paths
use `DVCFS`.

### Hot spot 4: legacy per-wave scripts constructing their own fs

Six legacy wave-level scripts construct `dvc.api.DVCFileSystem('../../')`
or `dvc.api.open(...)` directly:

- `countries/Guatemala/2000/_/food_acquired.py:12`
- `countries/Panama/{1997,2003,2008}/_/food_acquired.py`
- `countries/Panama/_/panama.py:19`
- `countries/Burkina_Faso/2014/_/food_acquired.py:25`
- plus `dvc.api.open()` uses in several Ethiopia / Burkina Faso wave
  scripts

Each of these runs as its own `make`/`stage.reproduce()` subprocess,
so they can't share a handle with the parent. The cost is unavoidable
per-script but can be removed if the scripts are rewritten to use
`get_dataframe()` (which reuses the module-level `DVCFS`), which is
already the recommended anti-pattern in CLAUDE.md.

### Measuring it

Run inside a working lsms_library env:

```python
import time
from dvc.api import DVCFileSystem
from dvc.repo import Repo

for _ in range(3):
    t0 = time.time()
    fs = DVCFileSystem("/path/to/lsms_library/countries")
    print(f"DVCFileSystem() ctor: {time.time()-t0:.2f}s")

for _ in range(3):
    t0 = time.time()
    r = Repo("/path/to/lsms_library/countries")
    print(f"Repo() ctor:          {time.time()-t0:.2f}s")
```

Typical numbers (on a warm machine with credentials already resolved)
are 0.5-2 s per DVCFileSystem construction and similar for Repo.
Worth eyeballing on the user's host to quantify. Multiplied by the
number of tables loaded in a session, this dominates wall time on
anything that isn't hitting a warm Layer-2 parquet.

## The fix

Targeted changes that address both problems:

### Change 1: pass `cache_remote_stream=True` at the open site

`lsms_library/local_tools.py:247`:

```python
with DVCFS.open(fn, mode='rb', cache_remote_stream=True) as f:
    df = read_file(f, convert_categoricals=convert_categoricals, encoding=encoding)
```

This is the dormant path added in iterative/dvc#9183 — it plumbs
`cache_odb=repo.cache.local` through to the underlying fsspec
`open()`, which writes the streamed blob to the DVC cache dir as a
side effect. Guard it with a `try/except TypeError` in case an
older-than-3.x DVC rejects the kwarg, falling back to the current
uncached behavior.

### Change 2: co-locate the DVC cache under `data_root()` (optional)

`lsms_library/local_tools.py:30-33`:

```python
from .paths import data_root  # already exported
_PACKAGE_ROOT = Path(__file__).resolve().parent
_COUNTRIES_DIR = _PACKAGE_ROOT / "countries"
_DVC_CACHE_DIR = data_root() / "dvc-cache"
_DVC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
DVCFS = DVCFileSystem(
    os.fspath(_COUNTRIES_DIR),
    config={"cache": {"dir": os.fspath(_DVC_CACHE_DIR)}},
)
```

With this in place, a single `LSMS_DATA_DIR=/shared/lsms` env var
controls **both** layers. Crucial on a shared cluster: every agent
that mounts `/shared/lsms` sees the same DVC blobs and the same
parquets, so the S3 pull happens exactly once per file per cluster,
not once per agent.

Both changes have precedent in `CLAUDE.md` lines 520-545 (which
already documents the `DVCFileSystem` config override pattern and
notes DVC's lazy credential validation makes the import-time
sequencing safe).

### Change 3: memoize the DVC `Repo` per `Country`

`lsms_library/country.py` around line 1623. Replace the per-call
construction with a lazy attribute on the `Country` instance:

```python
@property
def _dvc_repo(self) -> "Repo":
    cached = getattr(self, "_dvc_repo_cache", None)
    if cached is None:
        from dvc.repo import Repo
        cached = Repo(str(self.file_path.parent))
        self._dvc_repo_cache = cached
    return cached
```

Then in `load_dataframe_with_dvc()`:

```python
with _working_directory(dvc_root):
    repo = self._dvc_repo
    stage_infos = self._resolve_materialize_stages(method_name, waves)
    ...
```

This eliminates redundant `Repo()` construction when a user loads
multiple tables from the same `Country` instance. Locking is
unchanged — each call still goes through the existing
`with repo.lock, _redirect_stdout_to_stderr():` block. The only
thing cached is the handle.

### Change 4: reuse `DVCFS` in `data_access.get_data_file()`

`lsms_library/data_access.py:1146-1147`:

```python
# before
from dvc.api import DVCFileSystem
fs = DVCFileSystem(os.fspath(_COUNTRIES_DIR))

# after
from .local_tools import DVCFS as fs
```

Same root, same config, one less construction per fetch.

### Change 5 (optional, bigger scope): route all DVC reads through `DVCFS`

Replace the `dvc.api.open(fn, mode='rb')` calls at `local_tools.py`
lines 254, 429, 438, 452, 510, 523, 537, 551, 564, 598 with
`DVCFS.open(fn, mode='rb', cache_remote_stream=True)`. This folds
every DVC access in the library through the one cached, cache-on-open
filesystem handle. The fallback at line 254 can then be deleted —
`dvc.api.open()` and `DVCFS.open()` share the same remote config, so
there's no case where the former succeeds when the latter doesn't.

This is a larger diff than Changes 1-4, so keep it as a follow-up if
Changes 1-4 already resolve the observed latency.

### Verification after the fix

Re-run the Section 3 diagnostic. Expected changes:

- **Layer-1 caching (Changes 1-2):** After the first
  `get_dataframe()` call, `~/.cache/dvc/` (or
  `$LSMS_DATA_DIR/dvc-cache/`) is populated with a `files/md5/...`
  tree and `du -sh` shows a non-zero size matching the file's
  on-wire size. The second call (even with the parquet cache
  cleared) returns noticeably faster, because the `.dta` is now
  served from the local DVC cache instead of S3.
- **Repo memoization (Change 3):** Time
  `c = Country("Uganda"); c.household_roster(); c.food_acquired()`;
  the second-table wall-clock overhead (excluding the actual data
  build) should drop by the per-`Repo()` construction cost measured
  in the "Measuring it" section above.
- **`get_data_file` singleton (Change 4):** No user-visible change
  on cached paths; just saves one DVCFileSystem construction per
  World Bank fallback fetch.

## Critical files

| Purpose | File | Lines |
|---|---|---|
| `DVCFS` singleton (no cache config passed) | `lsms_library/local_tools.py` | 30-33 |
| `DVCFS.open()` call site (no `cache_remote_stream`) | `lsms_library/local_tools.py` | 245-250 |
| `dvc.api.open()` fallback and 9 other direct uses | `lsms_library/local_tools.py` | 252-256, 429, 438, 452, 510, 523, 537, 551, 564, 598 |
| Redundant `DVCFileSystem()` construction per WB fetch | `lsms_library/data_access.py` | 1146-1147 |
| `Repo()` reconstructed per table load | `lsms_library/country.py` | 1623 |
| `data_root()` — Layer 2 root | `lsms_library/paths.py` | 28-52 |
| DVC stage validate/rebuild path | `lsms_library/country.py` | 1611-1762 |
| `trust_cache=True` short-circuit (skips DVC entirely) | `lsms_library/country.py` | 1293-1299 |
| DVC pin | `pyproject.toml` | 16 |
| Legacy per-wave `dvc.api.DVCFileSystem('../../')` | `countries/{Guatemala,Panama,Burkina_Faso}/.../food_acquired.py` | several |
| On-disk DVC config (no `cache.dir` set) | `lsms_library/countries/.dvc/config` | 4-5 |

## Recap

1. At Layer 1 (raw `.dta` blobs from S3), the current code is **not
   caching anything** — `DVCFileSystem.open()` streams by default in
   DVC 3.x, and the library doesn't flip the opt-in flag.
2. Layer 2 (parquet cache under `data_root()`) *is* working and is
   persistent across processes. It's why performance feels
   reasonable day-to-day: once a table is materialized, it's reused.
3. **`Repo()` is reconstructed per table load** in
   `country.py:1623`, and a fresh `DVCFileSystem()` is built per
   `get_data_file()` fetch in `data_access.py:1147`, even though
   `local_tools.DVCFS` is already a module-level singleton at the
   same root. These are unnecessary per-call latency hits.
4. The diagnostic recipe in Sections 3-5 plus the "Measuring it"
   snippet under Hot spot 4 will confirm both problems
   unambiguously on a working host.
5. The fix is a small set of targeted changes (Changes 1-4):
   add `cache_remote_stream=True` at one open site; optionally point
   the DVC cache at `data_root()/dvc-cache` so one env var controls
   both layers; memoize the DVC `Repo` per `Country`; and have
   `data_access.get_data_file()` reuse the existing `DVCFS`
   singleton. Change 5 (routing all `dvc.api.open()` calls through
   `DVCFS`) is a larger follow-up.

## Sources

- [DVCFileSystem | DVC docs](https://dvc.org/doc/api-reference/dvcfilesystem)
- [iterative/dvc#9030 — dvc does not cache stream opened files](https://github.com/iterative/dvc/issues/9030)
- [iterative/dvc#9183 — add `cache_remote_stream` kwarg](https://github.com/iterative/dvc/pull/9183)
- [iterative/dvc#9382 — `dvc.api.open` disregards cache folder from config](https://github.com/iterative/dvc/issues/9382)

## Empirical verification (added 2026-04-11)

**Status update:** Change 1 above (`cache_remote_stream=True`) was
implemented in commit `3ad235f0` and **reverted in commit `e203a430`**
because it does not work in our installed DVC 3.67.0.  The diagnostic
this design proposes is correct about *what* is broken (Layer 1 is
dormant, S3 fetches are not cached) but **wrong about the fix**.

What we found by running the diagnostic recipe end-to-end:

### 1. The cache.dir on this system is not `~/.cache/dvc/`

`~/.config/dvc/config` (the global DVC config) overrides `cache.dir`:

```ini
[cache]
    dir = /global/scratch/users/ligon/.dvc/cache
    type = copy
```

That cache exists, is 80 GB, and was last meaningfully populated on
2026-04-02 — presumably from someone running `dvc pull` historically.
The Section 2 diagnostic ("inspect `~/.cache/dvc`") will report the
location as missing on this host, but that's because DVC is using a
different directory entirely; it isn't evidence that nothing has ever
been cached.

### 2. The kwarg name is `cache`, not `cache_remote_stream`

In DVC 3.67.0, `dvc/fs/dvc.py:355-369` reads:

```python
def _open(self, path, mode="rb", **kwargs):
    ...
    return dvc_fs.open(dvc_path, mode=mode, cache=kwargs.get("cache", False))
```

The `cache_remote_stream` kwarg referred to in iterative/dvc#9183
either was renamed before merging or refers to a different DVC
version path.  Our 3.67.0 silently accepts and drops
`cache_remote_stream` via `**kwargs` — no `TypeError`, no warning,
no cache write.

### 3. Even the correct kwarg name (`cache=True`) does not populate the cache

Direct probe with the actual 3.67 kwarg name:

```
file: Niger/2018-19/Data/grappe_gps_ner2018.dta  (35 KB)
DVCFS.open(fn, mode='rb', cache=True)
  open+read: 116.85s, size=35366
  expected cache: /global/scratch/users/ligon/.dvc/cache/files/md5/dc/03f1832290d7a76db5c7d6d3f1438c
  exists after open: False
```

So `cache=True` is forwarded by `_DVCFileSystem._open` to
`DataFileSystem.open(..., cache=True)`, but the inner method does
not trigger a write to the local cache directory in this version.

### 4. `DVCFileSystem.get_file()` also does not populate the cache

The "use the explicit download API" alternative also failed:

```
file: Niger/2018-19/Data/s00a_co_ner2018.dta  (2026 bytes)
DVCFS.get_file(remote, local_temp)
  111.41s, downloaded 2026 bytes (correct content)
  expected cache file at /global/scratch/users/ligon/.dvc/cache/
    files/md5/9f/0b129c17e66a7f5b36dc68660683fb
  exists after get_file: False
```

So neither `DVCFileSystem.open()` nor `DVCFileSystem.get_file()` writes
to the local DVC cache in 3.67.0, regardless of kwargs.  The
DVCFileSystem class genuinely has no stream-and-cache mode in this
version.

### 5. End-to-end Niger run from a fresh clone

The `bench/run_bench.sh Niger household_roster` benchmark from a fresh
`git clone` (no local `.dta` files, no cached parquets):

```
RUN 1 (cold-cache subprocess):
  import lsms_library                          22.948s
  Country('Niger')                              0.013s
  household_roster() #1 (cold)                363.410s   <-- S3 stream
  household_roster() #2 (warm in-proc)          0.784s

RUN 2 (fresh subprocess, data_root populated by RUN 1):
  household_roster() #1 (cold)                  1.103s   <-- v0.7.0 layer-2 fix
  household_roster() #2 (warm in-proc)          0.498s

dvc cache state after the run:
  /global/scratch/users/ligon/.dvc/cache: unchanged (no new md5 files)
```

Confirms (a) the v0.7.0 layer-2 fix at `country.py:1611-1812` works as
designed even from a clean state (363s cold → 1.1s cross-process), and
(b) layer 1 is genuinely dormant because the kwarg-based approach is a
no-op.  **The 363-second cold S3 cost is unavoidable on every fresh
layer-2 cache miss until layer 1 is fixed by other means.**

### What still might work for Layer 1

The path that definitely populates the cache is **`Repo.pull()`** (or
the equivalent CLI `dvc pull`).  The 80 GB existing cache is evidence
of this — it was built that way over many months.  An eventual layer-1
fix would call `Repo.pull(targets=[remote_path])` programmatically
inside `get_dataframe`, before the fall-through to `DVCFS.open()`,
when the file is dvc-tracked but not locally present.  Heavier than
streaming-with-side-effect-caching but it actually populates the
cache.  Deferred to a future session.

### Status of the proposed Changes 1-5

| # | Description | Status |
|---|---|---|
| 1 | `cache_remote_stream=True` at `local_tools.py:247` | **Reverted in `e203a430`** — kwarg silently dropped, doesn't populate cache.  Both `cache_remote_stream` and the actual 3.67 kwarg `cache` are no-ops. |
| 2 | Co-locate DVC cache under `data_root()` | Not needed: existing `~/.config/dvc/config` already overrides `cache.dir` to scratch.  Changing it would orphan the existing 80 GB cache. |
| 3 | Memoize `Repo` per `Country` | Not yet implemented.  Lower priority because the v0.7.0 layer-2 cache read at `country.py:1611-1638` bypasses the `Repo` construction entirely on warm-cache reads, which is the dominant path. |
| 4 | Reuse `DVCFS` singleton in `data_access.get_data_file()` | **Implemented in `3ad235f0`** and kept (the revert in `e203a430` only reverted Change 1).  Saves the per-call DVC handle construction cost on every WB-API fallback fetch. |
| 5 | Route all `dvc.api.open()` through `DVCFS` | Not implemented.  Defer with the rest of layer 1.  No urgency given that layer 2 is the dominant fast path. |

### Bottom line

- The Layer 2 (parquet) v0.7.0 fix in `country.py:1611-1812` is the
  user-visible win.  Empirically validated end-to-end on Niger
  (17× cross-process speedup) and Tanzania (4:26 → 163ms).
- The Layer 1 (DVC blob) caching path proposed in this doc does
  **not** work in DVC 3.67.0 via kwargs on `DVCFileSystem.open()` or
  `DVCFileSystem.get_file()`.  A real layer-1 fix needs `Repo.pull()`,
  deferred to a future session.
- The `data_access.get_data_file()` singleton-reuse cleanup
  (Change 4) is in and works as expected.
- The `~/.cache/dvc/` location referenced earlier in this doc is
  misleading on this host — the real cache lives at
  `/global/scratch/users/ligon/.dvc/cache/` per the global DVC config.

## 2026-04-11 follow-up: Layer 1 caching restored via runtime config override + Repo.pull

The "deferred to a future session" item above landed in the same
afternoon.  The breakthrough was reframing the question: instead of
asking "how do we make `DVCFileSystem.open()` populate the cache",
ask "how do we make `DVCFileSystem.open()` *read* from a populated
cache".  Reading from a populated cache is automatic in DVC 3.67.0
via `DataFileSystem._get_fs_path` iterating
`["cache", "remote", "data"]`; the only thing needed is to wire up
`info.cache` correctly via a runtime `config={"cache": {"dir": X}}`
override on the `DVCFileSystem` constructor.

### Probes that landed it

- **Probe 1 (storage_map inspection)**: confirmed the runtime config
  override propagates from `DVCFileSystem(..., config=...)` through
  `fs.repo` to every index entry's `storage_map[key].cache`.  The
  `info.cache` is a real `LocalHashFileDB` rooted at the override
  directory.
- **Probe 2 (cwd footgun)**: first attempt at `Repo.pull(targets=[X])`
  failed with `NoOutputOrStageError` because `Repo.pull` resolves
  targets against `os.getcwd()`, not against the repo root.  Fix:
  wrap the call in a chdir context manager (the same idiom
  `country.py:1658` uses).
- **Probe 2 (multi-MB end-to-end)**: cold S3 stream of a 7 MB Niger
  file took **17.465 s**; warm cache read of the same file took
  **0.010 s**.  ~1750× speedup, same order of magnitude as the
  Layer 2 numbers reported above.
- **Probe 2 (side-effect caching disproof)**: `DVCFileSystem.open()`
  does **not** populate the cache as a side effect of streaming; the
  empty cache stayed empty after the 17 s cold read.  This means
  Layer 1 warming requires explicit `Repo.pull`, not a clever
  `DVCFS.open` flag.

### Critical correction to the layout discussion above

The "two cache layouts coexist" interpretation in earlier versions of
this doc was wrong.  The flat
`{cache_dir}/{md5[:2]}/{md5[2:]}` layout we observed via `Repo.pull`
is the **DVC 2.x cache layout**, used because the LSMS `countries/`
sidecars carry `md5-dos2unix` hashes from the original DVC 2.x
`dvc add`-s.  The DVC 3.0 cache layout is
`{cache_dir}/files/md5/{md5[:2]}/{md5[2:]}`, used for sidecars with
raw `md5` hashes.  DVC 3.x is backward-compatible and reads from
both; `dvc cache migrate` is the explicit consolidation path.  See
https://dvc.org/doc/user-guide/upgrade.

`repo.cache.local.path` reports the DVC-3.x-native subpath
(`{cache_dir}/files/md5`) regardless of which layout is in actual
use; this is misleading diagnostic noise but not a bug.  The
storage_map's `info.cache.odb.path` reports the legacy root, which
is what `Repo.pull` actually writes to for legacy-hash sidecars.

### What landed

- `lsms_library/local_tools.py:33` — `DVCFS` now constructed with
  `config={"cache": {"dir": data_root() / "dvc-cache"}}`.
- `lsms_library/local_tools.py` — new `_ensure_dvc_pulled(fn)` helper
  with sidecar pre-check (cache hit = two `os.path.exists` calls,
  no DVC API), `_dvc_working_directory` cwd wrapper, dual-layout
  blob lookup (DVC 2.x flat + DVC 3.0 `files/md5/`), full exception
  swallowing.
- `lsms_library/local_tools.py:get_dataframe` — calls
  `_ensure_dvc_pulled(fn)` in the `file_system_path` branch before
  the `DVCFS.open(fn)` stream.
- `tests/test_dvc_caching.py` — new `TestLayer1Caching` class with
  unit tests for: Piece 1 cache.dir override, sidecar pre-check
  cache-hit short-circuit (both layouts), cache-miss falling
  through to `Repo.pull`, error swallowing, cwd-independence
  regression test, countries-relative path handling.

### Follow-ups (separate workstreams)

- **Migrate `.dvc` sidecars to DVC 3.0 hash algorithm**.  Run
  `dvc cache migrate` and regenerate every sidecar under
  `countries/`.  Eliminates the legacy hash code path entirely.
  Significant git churn (every tracked `.dta`'s md5 changes), needs
  `dvc gc -c` to clean orphaned legacy blobs from S3, needs CI
  rebaseline if any tests hardcode hash values.  Decoupled from
  this fix by the dual-layout pre-check.
- **Probe 6 — `Repo.pull` cost characterization**.  Time cold and
  warm `Repo.pull` calls for small/medium/large files; verify the
  warm-pull cost is acceptable for the cache-miss fallback path.
- **Probe 7 — End-to-end payoff measurement**.  Re-run the Niger
  cold-rebuild benchmark with Pieces 1+2 in place; expected drop
  from 363 s to ~10-30 s for the L2-cold / L1-warm case.
- **Cache migration UX**.  When LSMS users on hosts with prior
  `~/.cache/dvc/` populated by `dvc pull` CLI install this version,
  they don't automatically benefit from those blobs because the
  override pins the cache elsewhere.  Document the migration in
  `docs/guide/caching.md` (Piece 3 of the plan).

## 2026-04-11 follow-up #2: structural correction — `Repo.fetch` not `Repo.pull`

After the initial fix landed (commit `8d333f8`), end-to-end smoke tests
on the user's dev workstation revealed that the `_ensure_dvc_pulled`
helper had a structural problem: it called `DVCFS.repo.pull(...)`,
which is `Repo.fetch + Repo.checkout`.  The `checkout` step
materializes the file in the workspace at its DVC-tracked path --
**inside the package tree**.

The user's hard architectural rule: *"we definitely don't want those
dta files materializing in the package tree, regardless of who's
responsible"*.  The package tree is for code and `.dvc` sidecars; data
lives under `data_root()`.

### Why this matters

`Repo.pull`'s workspace-checkout side effect created a hidden coupling:

1. First `get_dataframe` call on a fresh clone -> `Repo.pull` -> blob
   in cache **and** file in workspace.
2. Subsequent `get_dataframe` calls -> `local_file()` returns True
   (workspace file exists) -> reads directly from disk -> the entire
   DVC code path is short-circuited -> the cache (which we worked so
   hard to populate correctly) is never read.

The 1750x speedup from the multi-MB Probe 2 was measuring the right
primitive in isolation but didn't reflect production behavior, because
the production hot path never reached the `DVCFS.open` cache-read
branch.  The user's "L2-cold L1-warm" timing of 43.1 s vs cold-cold
of 41.4 s was the smoking gun: identical timings because both runs
were going through the local-file path, with our Layer 1 helper
silently bypassed.

### The fix

`Repo.pull` -> `Repo.fetch` in `_ensure_dvc_pulled`.  `Repo.fetch`
populates the local cache without checking out to the workspace.
After this change:

1. First `get_dataframe` call on a fresh clone -> `Repo.fetch` -> blob
   in cache, workspace untouched.
2. Subsequent `get_dataframe` calls -> `local_file()` returns False
   (no workspace file) -> falls into `file_system_path` ->
   `_ensure_dvc_pulled` finds blob in cache (sidecar pre-check hit) ->
   no fetch needed -> `DVCFS.open` reads from cache via the
   `_get_fs_path[typ='cache']` branch.  The 1750x cache-read speedup
   is now actually exercised in production.

### Coordinated changes still needed (gated by probes)

This commit lands change #1 (the fetch fix) only.  Two related
changes are gated on a probe and a future commit:

**Change #2: Path normalization at the `get_dataframe` boundary.**
With workspace files no longer being materialized, `local_file()`
returns False for DVC-tracked paths and we fall into
`DVCFS.open('../Data/foo.dta')`.  But `DVCFS` is rooted at
`_COUNTRIES_DIR` and its path normalization is fs-root-relative, so
`'../Data/...'` may not resolve correctly.  **Probe needed**: from
a wave-script-style cwd, call `DVCFS.open('../Data/foo.dta').read()`
and see whether it works.  If yes, no change needed.  If no, add a
canonicalizer in `_resolve_data_path` (or a sibling helper) to
convert cwd-relative paths to fs-root-relative form before passing
to DVCFS.

**Change #3: Harden `local_file()` with a sister-sidecar check.**
The right rule is "a file in the workspace is pollution iff it has
a sister `.dvc` sidecar".  If yes, refuse to use it (force the DVC
path so reads go through the cache).  If no, the file is either
freshly downloaded new data being prepped for `dvc add`, or scratch
data the user is supplying directly, or output from the
WB-fallback auto-add path -- all legitimate uses.  Pure Python
check at the call site, no filesystem permission tricks needed.
The user considered (and rejected) a more aggressive read-only
approach because the legitimate add-new-data workflows (manual
per CONTRIBUTING.org, WB-fallback auto-add in
`data_access.get_data_file`) need write access to those
directories.

**Cleanup task (separate one-time op)**: existing dev checkouts
have `.dta` files under `lsms_library/countries/` from the prior
`Repo.pull` behavior or from manual `dvc pull` invocations.
Clean up with a safe filter that only deletes files where a sister
`.dvc` sidecar exists:

    find lsms_library/countries -type f -name '*.dta' \
        -execdir test -e '{}.dvc' \; -print -delete

Worth running on every dev workstation that's been used with the
old behavior.

### What landed in this commit

- `lsms_library/local_tools.py:_ensure_dvc_pulled` -- one-line change:
  `DVCFS.repo.pull(...)` -> `DVCFS.repo.fetch(...)`.  Plus updated
  docstring explaining why fetch and not pull.
- `lsms_library/local_tools.py:get_dataframe` -- comment update at
  the call site to reflect the fetch rationale.
- `tests/test_dvc_caching.py:TestLayer1Caching` -- 12 mock references
  updated from `repo.pull` to `repo.fetch`.  Test docstrings updated.
- `slurm_logs/DESIGN_dvc_layer1_caching.md` -- this follow-up #2
  appendix.

## 2026-04-11 follow-up #3: path-normalization probe and `local_file` hardening

Two changes were originally gated on the path-normalization probe:

**Probe result (this session)**: from a wave-script-style cwd
(`cd lsms_library/countries/Niger/2018-19/_/`), calling
`DVCFS.open('../Data/grappe_gps_ner2018.dta', mode='rb').read()`
returned `35366` bytes -- the correct file content.  fsspec /
DVCFileSystem already handles cwd-relative paths in this form.
**Change #2 (path canonicalizer in `_resolve_data_path`) is not
needed.**

**Change #3 landed in this commit**: `_is_polluted_workspace_copy`
helper + `local_file()` hardening.  The new helper returns True iff a
file exists on disk *and* has a sister `.dvc` sidecar; `local_file()`
calls it after a successful `open()` and, if True, emits a
`UserWarning` and returns False to force the read through the DVC
cache path.  Files without sidecars are still legitimate fast paths
(new data being prepped for `dvc add`, scratch data, WB-fallback
downloads).  The warning includes the cleanup command so users can
fix their workspace.

The hardening is opt-out only via the cleanup itself: until users
remove the polluted workspace files under `lsms_library/countries/`,
they will see warnings on every read.  That's the intended UX --
the warnings are the symptom that signals the cleanup is needed.

Tests in `TestLayer1Caching`:
- `test_is_polluted_workspace_copy_true_when_sidecar_exists`
- `test_is_polluted_workspace_copy_false_when_no_sidecar` (covers
  the new-data add cases the user pointed out)
- `test_is_polluted_workspace_copy_false_on_bad_input`
- `test_get_dataframe_warns_and_falls_through_on_polluted_workspace`
