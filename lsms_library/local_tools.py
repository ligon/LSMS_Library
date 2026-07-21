"""Low-level tooling shared by wave-level scripts and the Country class.

This is the catch-all utility module that backs the library's data
access, ID handling, categorical-mapping lookup, and wave-level feature
extraction helpers. Most user-facing scripts only need a small slice of
it; the library's public-facing alias ``lsms_library.tools`` points
here.

Key functions
-------------
- :func:`get_dataframe` — the single entry point for reading
  ``.dta``/``.csv``/``.parquet`` files. Fallback chain: local file on
  disk → DVC filesystem → World Bank NADA download. Wave-level scripts
  should always use this over :func:`pd.read_stata` or
  :func:`dvc.api.open` directly.
- :func:`to_parquet` — the single entry point for writing parquet
  caches. Redirects relative paths to ``data_root()`` via
  :func:`_resolve_data_path`, which infers country/wave from the call
  stack so a wave-level script writing ``foo.parquet`` lands under
  ``~/.local/share/lsms_library/{Country}/{wave}/_/foo.parquet``.
- :func:`df_data_grabber` — the YAML path's extraction engine. Given a
  source ``.dta`` file and ``idxvars``/``myvars`` mappings (with
  optional formatting functions), returns a DataFrame with the
  canonical index and columns.
- :func:`format_id` — canonical string-format helper for household,
  cluster, and person IDs. Handles both numeric and string inputs with
  optional zero-padding. Auto-applied to ``idxvars`` by
  :func:`df_data_grabber`, but NOT to ``myvars`` — see ``CLAUDE.md``.
- :func:`id_walk` — applies a country's ``updated_ids`` mapping to
  rewrite household IDs into a canonical wave's coordinates.
  Idempotent by construction: each wave's mapping is closure-resolved
  via :func:`_close_id_map` before application, so running ``id_walk``
  twice on the same DataFrame produces the same result. The
  ``df.attrs['id_converted']`` flag remains as a fast-path
  optimization — losing it (e.g. via :meth:`.set_index` or
  :meth:`.merge`) is no longer a correctness hazard.
- :func:`panel_ids` — returns households observed in at least two
  waves after applying ``id_walk``.
- :func:`map_index` — remaps an old parquet's index structure to the
  new scheme declared in ``data_info.yml``.
- :func:`get_categorical_mapping` — reads a named org-mode table
  (typically ``harmonize_food``, ``unit``, ``shocks``) from the
  country's ``categorical_mapping.org`` and returns it as a dict.
- :func:`get_formatting_functions` — loads the per-wave Python module
  of formatting helpers referenced by ``data_info.yml``.

Conventions
-----------
Scripts in ``{Country}/{wave}/_/`` run from that directory, so
``../Data/file.dta`` is the standard relative path. :func:`get_dataframe`
and :func:`to_parquet` both rely on :func:`_resolve_data_path` to
translate such paths to either a source location or a cache location
under ``data_root()``.

See ``CLAUDE.md`` for the full anti-pattern list and data-access rules.
"""
from __future__ import annotations

from ._build_registry import build_transform
from ligonlibrary.dataframes import from_dta
import pyreadstat
import struct
import tempfile
import numpy as np
import pandas as pd
import dvc.api
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
import warnings
import yaml
import json
import difflib
import re
import types
from pyarrow.lib import ArrowInvalid
from functools import lru_cache
from pathlib import Path
import os
import ast
import hashlib
import ssl
import time
import uuid
from importlib.resources import files
from dvc.api import DVCFileSystem
import pyreadstat
import inspect
from typing import Any
from .paths import data_root, var_path, wave_data_path, countries_root
from .config import s3_creds_path as _s3_creds_path

# Initialize DVC filesystem once and reuse it.
#
# The runtime ``config={"cache": {"dir": ...}}`` override pins the
# DVC object cache to a user-writable location under ``data_root()``,
# so the same ``LSMS_DATA_DIR`` controls both the parquet cache
# (L2-country and L2-wave tiers) and the DVC blob cache (L1).
# Without this override, the cache falls back to
# ``{_COUNTRIES_DIR}/.dvc/cache`` which is unwritable in pip-installed
# layouts.
#
# The override also makes the DVCFileSystem cache-read fast path
# (``DataFileSystem._get_fs_path`` iterating ``["cache", "remote", "data"]``)
# resolve into a populated directory, so any blob present at
# ``{_DVC_CACHE_DIR}/{md5[:2]}/{md5[2:]}`` is served from local disk
# instead of streaming from S3.  See ``_ensure_dvc_pulled`` below for
# the warming side of the round-trip.
#
# Similarly, the ``credentialpath`` override on the ``ligonresearch_s3``
# remote redirects DVC's S3 credential lookup away from the packaged
# (and in pip-installed layouts, read-only) ``.dvc/s3_creds`` path to
# the user-writable ``s3_creds_path()``.  DVC is lazy about credential
# validation: the file at this path does not need to exist at
# ``DVCFileSystem`` construction time.  The auto-unlock pass later in
# ``lsms_library/__init__.py`` populates it before the first S3 access.
_PACKAGE_ROOT = Path(__file__).resolve().parent
# GH #436: import-time snapshot of the countries config-tree root.  Honors
# ``LSMS_COUNTRIES_ROOT`` set *before* import (the worktree model); does not
# track a later override + ``countries_root.cache_clear()`` in-process.
_COUNTRIES_DIR = countries_root()
_DVC_CACHE_DIR = data_root() / "dvc-cache"
_DVC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
DVCFS = DVCFileSystem(
    os.fspath(_COUNTRIES_DIR),
    config={
        "remote": {
            "ligonresearch_s3": {
                "credentialpath": str(_s3_creds_path()),
            },
        },
        "cache": {"dir": os.fspath(_DVC_CACHE_DIR)},
    },
)


@contextmanager
def _dvc_working_directory(path):
    """Temporarily switch the process working directory.

    ``Repo.pull(targets=[...])`` resolves targets against ``os.getcwd()``,
    not against the repo root, so callers must change directory before
    invoking it.  ``country.py`` has its own copy at module level; this
    one is duplicated here to avoid a circular import.
    """
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


#: Bounded retry for a single-blob S3 fetch before giving up to the streaming
#: fallback (see :func:`_get_file_with_retry`).  Overridable via env for
#: constrained / flaky-egress hosts.
_FETCH_ATTEMPTS = int(os.getenv("LSMS_FETCH_ATTEMPTS", "3"))
_FETCH_BASE_DELAY = 0.5  # seconds; doubled per retry


def _is_transient_fetch_error(exc: BaseException) -> bool:
    """Whether *exc* is a transient remote-read error worth retrying.

    Concurrent multipart S3 downloads (s3fs ``_download_file_part_concurrent``)
    occasionally corrupt a TLS record under heavy parallel load -- e.g. a
    ``make -jN`` country build fetching several large ``.dta`` blobs at once --
    raising ``ssl.SSLError('... DECRYPTION_FAILED_OR_BAD_RECORD_MAC ...')`` or
    ``aiohttp.ClientPayloadError('Response payload is not completed')``.  These
    are not connectivity failures (a retry succeeds) and not the same as
    ``FileNotFoundError`` (wrong cache layout), which must NOT be retried.
    """
    if isinstance(exc, FileNotFoundError):
        return False
    if isinstance(exc, (ConnectionError, TimeoutError, ssl.SSLError)):
        return True
    name = type(exc).__name__
    msg = str(exc)
    return (
        name in {"ClientPayloadError", "ClientError", "ClientOSError",
                 "ServerTimeoutError", "ServerDisconnectedError",
                 "IncompleteRead", "ReadTimeoutError", "ConnectTimeoutError"}
        or "DECRYPTION_FAILED" in msg
        or "BAD_RECORD_MAC" in msg
        or "payload is not completed" in msg
    )


def _get_file_with_retry(fs, src: str, dst: str, *,
                         attempts: int | None = None,
                         base_delay: float = _FETCH_BASE_DELAY,
                         sleep=time.sleep) -> None:
    """``fs.get_file(src, dst)`` with bounded retry on transient TLS errors.

    Re-raises ``FileNotFoundError`` immediately (the caller then tries the next
    cache layout).  Retries transient errors (see
    :func:`_is_transient_fetch_error`) with exponential backoff, cleaning up any
    partial ``dst`` between attempts; re-raises once ``attempts`` is exhausted
    or the error is non-transient (the caller then falls through to the
    streaming fallback).  ``sleep`` is injectable for tests.
    """
    attempts = attempts if attempts is not None else _FETCH_ATTEMPTS
    for attempt in range(max(1, attempts)):
        try:
            fs.get_file(src, dst)
            return
        except FileNotFoundError:
            raise
        except Exception as exc:  # noqa: BLE001 -- classify, then retry or re-raise
            try:
                Path(dst).unlink()
            except OSError:
                pass
            if attempt + 1 >= attempts or not _is_transient_fetch_error(exc):
                raise
            sleep(base_delay * (2 ** attempt))


def _ensure_dvc_pulled(fn) -> None:
    """Best-effort warm of the local DVC cache for a tracked path.

    The hot path (cache hit) is two ``os.path.exists`` calls and a tiny
    YAML parse -- no DVC Python API, no ``DVCFS.repo``, no index walk.

    On a cache miss this downloads the blob directly from the configured
    S3 remote, bypassing ``Repo.fetch``.  Why bypass: ``Repo.fetch`` is
    decorated with ``@locked``, which routes through ``lock_repo``
    (``dvc/repo/__init__.py:38``).  That context manager calls
    ``Repo._reset()`` on entry and exit, dropping the cached
    ``Repo.index``.  The next access rebuilds the index by walking
    every ``.dvc`` sidecar in the countries tree (~93s on Lustre,
    invariant of the actual blob's size).  Pyinstrument profiling
    confirmed ~78s of that goes into ``StorageMapping.__getitem__``
    alone.  We have everything we need without going there: the md5
    from the parsed sidecar, the destination cache path, and an
    authenticated ``S3FileSystem`` from ``DVCFS.repo.cloud.get_remote``.
    A direct ``s3.get_file`` is ~0.1s for a 165 KB blob; the speedup
    over ``Repo.fetch`` is ~850x.  As a side effect, concurrent callers
    fetching different blobs no longer contend on ``.dvc/tmp/lock`` --
    we never take the DVC repo lock at all.

    See ``slurm_logs/HANDOFF_2026-05-07.org`` for the empirical
    investigation (lock contention reproducer, batched-fetch bench,
    pyinstrument profile, and the direct-S3 timing experiment).

    Earlier history: ``slurm_logs/DESIGN_dvc_layer1_caching.md``
    landed the ``Repo.fetch``-based design that this function used to
    have; that design moved the DVC index work from ``DVCFS.open``'s
    streaming fallback to a single ``Repo.fetch`` call per cache miss,
    which was a big improvement, but still paid the index walk cost
    on every miss.  The current bypass eliminates it.

    Every error is swallowed.  This function is best-effort warming
    glue; the caller's ``DVCFS.open`` streaming fallback handles any
    failure to populate the cache.
    """
    try:
        # Try several interpretations of fn to find the .dvc sidecar.
        # Wave scripts pass cwd-relative paths like '../Data/foo.dta'.
        # Interactive callers may pass countries-relative paths like
        # 'Niger/2018-19/Data/foo.dta'.  Both forms must work.
        fn_path = Path(fn)
        candidates = []
        if fn_path.is_absolute():
            candidates.append(fn_path)
        else:
            candidates.append((Path.cwd() / fn_path).resolve())
            candidates.append((_COUNTRIES_DIR / fn_path).resolve())

        abs_path = None
        for c in candidates:
            if (c.parent / f"{c.name}.dvc").exists():
                abs_path = c
                break
        if abs_path is None:
            return  # no sidecar found at any interpretation; not DVC-tracked

        sidecar = abs_path.parent / f"{abs_path.name}.dvc"
        with sidecar.open() as fh:
            sidecar_data = yaml.safe_load(fh)
        md5 = sidecar_data["outs"][0]["md5"]
        # Check both DVC 2.x and DVC 3.0 cache layouts.  DVC 3.x is
        # backward-compatible and reads from both.  Most blobs in the
        # LSMS countries repo land in the legacy flat layout because
        # the existing .dvc sidecars carry md5-dos2unix hashes from
        # the original DVC 2.x dvc-adds; new sidecars (post-migration)
        # would use the DVC-3.0 files/md5/ subpath layout.  See
        # https://dvc.org/doc/user-guide/upgrade for the version
        # history.
        cache_layouts = (
            _DVC_CACHE_DIR / md5[:2] / md5[2:],                  # DVC 2.x
            _DVC_CACHE_DIR / "files" / "md5" / md5[:2] / md5[2:], # DVC 3.x
        )
        if any(p.exists() for p in cache_layouts):
            return  # cache hit -- DVCFS.open() will serve from local
        # NB: an earlier ``rel_path = abs_path.relative_to(_COUNTRIES_DIR)``
        # guard lived here -- it was needed when the cache-miss path called
        # ``Repo.fetch(targets=[str(rel_path)])`` and would have produced a
        # NoOutputOrStageError otherwise.  The bypass below uses md5 +
        # cache_layouts directly and has no such requirement; the guard
        # was actively harmful when ``abs_path`` resolves under a worktree
        # whose path doesn't share a prefix with the imported
        # ``_COUNTRIES_DIR`` (e.g. a wave script in a worktree but
        # importing lsms_library from a separate main checkout via the
        # venv's ``.pth`` file -- ``relative_to`` raises ``ValueError``
        # and the function silently bails without fetching).  We don't
        # need the path to be inside ``_COUNTRIES_DIR`` to fetch the blob;
        # we only need its md5, which we already have.

    except (OSError, yaml.YAMLError, KeyError, IndexError, TypeError):
        # Bad sidecar shape (None / wrong type), missing 'outs'/'md5' key,
        # corrupt YAML, or filesystem error -> bail to streaming.  Programmer
        # bugs (NameError, AttributeError) propagate.
        return

    # Cache-miss path: download the blob directly from the S3 remote,
    # bypassing ``Repo.fetch`` entirely.
    #
    # Why bypass: ``Repo.fetch`` is decorated with ``@locked``, which
    # routes through ``lock_repo`` (``dvc/repo/__init__.py:38``).  That
    # context manager calls ``Repo._reset()`` both on entry and on exit,
    # which drops the cached ``Repo.index`` (``@cached_property``).  The
    # next access rebuilds the index via ``Index.from_repo(self)`` --
    # walking every ``.dvc`` sidecar in the countries tree on Lustre.
    # Pyinstrument confirms this is the dominant cost: ~78s in
    # ``StorageMapping.__getitem__`` per ``Repo.fetch`` call, ~93s total
    # wall-clock per call regardless of file size (122 KB and 2.7 MB
    # both took ~95s end-to-end -- the actual S3 GET is rounding error).
    #
    # We don't need any of that.  We have:
    #   1. The blob's md5 (from the .dvc sidecar, parsed above).
    #   2. The cache target path (``cache_layouts[0]``).
    #   3. The S3 remote ``fs`` and ``path`` from ``DVCFS.repo.cloud``.
    # That's enough for a direct ``fsspec.get_file`` call: ~0.1s for a
    # 165 KB blob, dominated by the S3 GET roundtrip.  No index walk,
    # no repo.lock acquisition, no ``_reset`` thrash.  Concurrent
    # callers fetching different blobs no longer contend at all.
    #
    # See ``slurm_logs/HANDOFF_2026-05-07.org`` for the empirical
    # investigation that established the ~850x speedup over
    # ``Repo.fetch``.
    # The bypass is *remote-agnostic*: ``remote.fs`` is whatever fsspec
    # implementation matches the active remote's URL scheme
    # (``s3fs.S3FileSystem``, ``gcsfs.GCSFileSystem``,
    # ``adlfs.AzureBlobFileSystem``, the GDrive wrapper,
    # ``HTTPFileSystem``, ``SFTPFileSystem``, ``LocalFileSystem``, ...).
    # All of them implement ``get_file(src, dst)`` with the same
    # contract.  The active remote name is resolved at call time from
    # ``.dvc/config``'s ``[core] remote =`` setting -- no S3-specific
    # assumptions.  Adding a new DVC remote requires only that its
    # fsspec implementation honour ``get_file()``, which is the standard
    # fsspec contract.
    try:
        remote = DVCFS.repo.cloud.get_remote(name=DVCFS.repo.config["core"].get("remote"))
        fs = remote.fs
        # Try DVC 2.x layout first (where most LSMS blobs live), then
        # DVC 3.x layout as fallback.  ``get_file`` fails fast with a
        # FileNotFoundError if the source isn't there, which is cheaper
        # than a separate HEAD-style ``exists()`` probe (each HEAD on
        # this Lustre+S3 setup is ~1s).
        src_candidates = [
            f"{remote.path}/{md5[:2]}/{md5[2:]}",                 # DVC 2.x
            f"{remote.path}/files/md5/{md5[:2]}/{md5[2:]}",       # DVC 3.x
        ]
        # Atomic write via tempfile + rename.  PID-suffixed temp name
        # avoids collisions if two processes happen to fetch the same
        # blob concurrently (rare in practice; each contender wants a
        # different blob).
        dst = cache_layouts[0]
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(dst.suffix + f".tmp.{os.getpid()}")
        for src in src_candidates:
            try:
                _get_file_with_retry(fs, src, str(tmp))
                tmp.rename(dst)
                return
            except FileNotFoundError:
                continue  # wrong layout -- try the other one
            except Exception:  # noqa: BLE001
                # Transient retries exhausted, or a non-transient remote / disk
                # error (incl. aiohttp ClientPayloadError, not an OSError).
                # Clean up any partial temp and fall through to the streaming
                # fallback at the call site -- this function swallows by design.
                try:
                    tmp.unlink()
                except OSError:
                    pass
                continue
        # Both layouts missed -- the blob is genuinely not on the
        # remote.  Fall through to streaming fallback at the call site.
        try:
            tmp.unlink()
        except OSError:
            pass
    except (OSError, ValueError, KeyError, RuntimeError, AttributeError):
        # Best-effort fetch.  Any remote / config / sidecar failure
        # falls through to ``DVCFS.open`` streaming fallback at the
        # call site.
        pass


def _dvc_cache_path(fn) -> "Path | None":
    """Resolve a DVC-tracked path to its on-disk cache file, if cached.

    Mirrors ``_ensure_dvc_pulled``'s sidecar-parsing logic but without
    side effects: parses the ``.dvc`` sidecar for ``fn``, computes the
    expected cache layouts (DVC 2.x flat + DVC 3.x ``files/md5/``), and
    returns the first one that exists.  Returns ``None`` if no sidecar
    is found or no cache layout is populated.

    Used by ``get_dataframe`` to bypass ``DVCFS.open()`` once the cache
    has been populated -- ``DVCFS.open()`` walks the DVC sidecar tree
    on every call (~10-20s on Lustre per file), but a direct
    ``open(cache_path, 'rb')`` is microseconds.
    """
    try:
        fn_path = Path(fn)
        candidates = []
        if fn_path.is_absolute():
            candidates.append(fn_path)
        else:
            candidates.append((Path.cwd() / fn_path).resolve())
            candidates.append((_COUNTRIES_DIR / fn_path).resolve())

        abs_path = None
        for c in candidates:
            if (c.parent / f"{c.name}.dvc").exists():
                abs_path = c
                break
        if abs_path is None:
            return None

        sidecar = abs_path.parent / f"{abs_path.name}.dvc"
        with sidecar.open() as fh:
            md5 = yaml.safe_load(fh)["outs"][0]["md5"]
        for layout in (
            _DVC_CACHE_DIR / md5[:2] / md5[2:],                 # DVC 2.x
            _DVC_CACHE_DIR / "files" / "md5" / md5[:2] / md5[2:], # DVC 3.x
        ):
            if layout.exists():
                return layout
        return None
    except (OSError, yaml.YAMLError, KeyError, IndexError, TypeError):
        return None


def _gc_stale_dvc_lock() -> None:
    """Best-effort cleanup of orphaned ``.dvc/tmp/*lock*`` files.

    DVC's HardlinkLock (``core.hardlink_lock = true``) leaves a hardlink
    pair behind when its holder dies -- the canonical ``lock`` file plus
    one or more md5-named ``.lock`` claim files, all sharing one inode
    with ``Links: 2``.  flufl.lock encodes a 365-day "lifetime" in the
    file's mtime, so the orphan is treated as held until 2027-something
    and every subsequent acquire fails.

    On Lustre, the default zc.lockfile (used when ``hardlink_lock`` is
    off) also fails to acquire when a stale flufl-format lockfile is
    present -- the open() succeeds but fcntl.lockf returns EAGAIN even
    though no process holds an fd into the file.  Recovery without this
    helper requires a manual ``find .dvc/tmp/ -name '*lock*' -delete``.

    This GC removes any file under ``.dvc/tmp/`` matching ``*lock*``
    that:
      1. Has mtime more than 1 day in the future (sentinel for the
         365-day flufl lifetime), AND
      2. Has no process holding an open fd into it (probed via
         ``/proc/*/fd/*``; on platforms without procfs, skipped --
         on Linux this is reliable).

    Both conditions must hold to avoid GC'ing a lock currently in use
    by a peer process.
    """
    try:
        lock_dir = _COUNTRIES_DIR / ".dvc" / "tmp"
        if not lock_dir.exists():
            return
        candidates = list(lock_dir.glob("*lock*"))
        if not candidates:
            return
        future_mtime_threshold = time.time() + 86400  # 1 day from now
        # Probe /proc/*/fd/ once for the union of inodes we'd consider
        # GC'ing.  An inode in the held-fds set is excluded from GC.
        held_inodes = _proc_held_inodes(candidates)
        for p in candidates:
            try:
                st = p.stat()
            except OSError:
                continue
            if st.st_mtime <= future_mtime_threshold:
                continue  # not a flufl-orphan signature
            if st.st_ino in held_inodes:
                continue  # someone has it open right now
            try:
                p.unlink()
            except OSError:
                pass
    except OSError:
        # Best-effort.  Never let GC failures propagate.
        pass


def _proc_held_inodes(paths) -> set:
    """Return the set of inodes that any process has an open fd into.

    Conservative on failure: returns the FULL set of `paths`' inodes if
    we can't read /proc/, so we err on the side of NOT GC'ing.
    """
    try:
        target_inodes = {p.stat().st_ino for p in paths if p.exists()}
    except OSError:
        return set()
    if not target_inodes:
        return set()
    proc = Path("/proc")
    if not proc.exists():
        # No procfs (e.g. macOS).  Conservative: claim all inodes are held.
        return target_inodes
    held = set()
    for pid_dir in proc.iterdir():
        if not pid_dir.name.isdigit():
            continue
        fd_dir = pid_dir / "fd"
        try:
            for fd in fd_dir.iterdir():
                try:
                    target = os.readlink(fd)
                    st = os.stat(target)
                    if st.st_ino in target_inodes:
                        held.add(st.st_ino)
                except (OSError, FileNotFoundError):
                    continue
        except (OSError, PermissionError):
            continue  # skip processes we can't read
    return held


def _warm_dvc_cache_for_feature(country, feature: str, wave: str | None = None) -> None:
    """Pre-warm the DVC blob cache for source files declared in
    ``data_info.yml`` for the requested feature.

    Pre-fetches each missing blob via direct S3 download (the same
    bypass used by ``_ensure_dvc_pulled`` -- see that function's
    cache-miss path for the rationale).  Each download costs ~0.1s
    after the S3 client is warmed; for typical features this means
    a handful of source files per wave fetched in a fraction of a
    second total, vs the ~95s ``Repo.fetch`` would take.

    Scope:
      - Walks each wave's ``data_info.yml`` for the feature's ``file:``
        entries (top-level and nested under ``dfs:``).
      - Skips files that are not DVC-tracked (no sister ``.dvc`` sidecar).
      - Skips files whose blob is already in the cache.
      - If ``wave`` is given, only that wave is warmed.

    Limitations:
      - Script-path (``materialize: make`` / ``!make``) features have
        their source-file declarations in Python strings inside the
        wave script, NOT in ``data_info.yml``.  Those features are not
        covered by this helper; ``_ensure_dvc_pulled`` handles them
        per-call (still cheaply, since the bypass eliminates the
        contention regardless).  Once script-path features grow YAML
        input declarations, this helper can be extended to read them.

    Best-effort:
      - All exceptions are swallowed.  A pre-warm failure must not
        block the actual ``make`` invocation; the per-process call
        path in ``_ensure_dvc_pulled`` is the safety net.
    """
    try:
        if wave is not None:
            wave_names = [wave]
        else:
            wave_names = list(country.waves)

        # Collect (md5, dst_cache_path) pairs for every uncached, DVC-
        # tracked source file the feature declares across the requested
        # waves.
        targets: list[tuple[str, Path]] = []
        seen_md5: set[str] = set()
        for w in wave_names:
            try:
                wave_obj = country[w]
            except (KeyError, AttributeError):
                continue
            di_path = wave_obj.file_path / "_" / "data_info.yml"
            if not di_path.exists():
                continue
            try:
                data_info = yaml.safe_load(di_path.read_text())
            except (OSError, yaml.YAMLError):
                continue
            if not isinstance(data_info, dict):
                continue
            block = data_info.get(feature)
            if not block:
                continue
            for fn in _collect_file_paths_from_block(block):
                # Resolve relative to the wave's _/ directory (matches
                # the convention wave scripts use).
                abs_path = (wave_obj.file_path / "_" / fn).resolve()
                sidecar = abs_path.with_name(abs_path.name + ".dvc")
                if not sidecar.exists():
                    continue  # untracked source — no DVC fetch needed
                try:
                    md5 = yaml.safe_load(sidecar.read_text())["outs"][0]["md5"]
                except (KeyError, IndexError, TypeError, OSError, yaml.YAMLError):
                    continue
                if md5 in seen_md5:
                    continue
                seen_md5.add(md5)
                # DVC 2.x layout is the canonical destination for our
                # cache (see ``_ensure_dvc_pulled``'s comment block).
                dst = _DVC_CACHE_DIR / md5[:2] / md5[2:]
                if dst.exists() or (_DVC_CACHE_DIR / "files/md5" / md5[:2] / md5[2:]).exists():
                    continue  # already cached
                targets.append((md5, dst))

        if not targets:
            return

        # Resolve the active DVC remote once; reuse across all blobs.
        # Remote-agnostic -- ``remote.fs`` is whatever fsspec backend
        # the active remote uses (S3, GCS, Azure, HTTP, SSH, ...).
        try:
            remote = DVCFS.repo.cloud.get_remote(name=DVCFS.repo.config["core"].get("remote"))
            fs = remote.fs
            remote_path = remote.path
        except (OSError, KeyError, AttributeError, RuntimeError):
            return  # remote unreachable; fall back to per-call path

        for md5, dst in targets:
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst.with_suffix(dst.suffix + f".tmp.{os.getpid()}")
            for src in (
                f"{remote_path}/{md5[:2]}/{md5[2:]}",                  # DVC 2.x
                f"{remote_path}/files/md5/{md5[:2]}/{md5[2:]}",        # DVC 3.x
            ):
                try:
                    fs.get_file(src, str(tmp))
                    tmp.rename(dst)
                    break
                except FileNotFoundError:
                    continue
                except OSError:
                    try:
                        tmp.unlink()
                    except OSError:
                        pass
                    continue
            else:
                # Both layouts missed for this blob; clean tmp if any.
                try:
                    tmp.unlink()
                except OSError:
                    pass
    except Exception:
        # Last-ditch swallow: never let pre-warm break a build.
        pass


def _collect_file_paths_from_block(block):
    """Walk a data_info.yml feature block and yield every ``file:`` value
    encountered, including those nested in ``dfs:`` sub-blocks."""
    if isinstance(block, dict):
        f = block.get("file")
        if isinstance(f, str):
            yield f
        elif isinstance(f, list):
            for x in f:
                if isinstance(x, str):
                    yield x
        for v in block.values():
            yield from _collect_file_paths_from_block(v)
    elif isinstance(block, list):
        for item in block:
            yield from _collect_file_paths_from_block(item)


def _is_polluted_workspace_copy(fn) -> bool:
    """True if *fn* exists on disk AND has a sister ``.dvc`` sidecar.

    A workspace copy of a DVC-tracked file is pollution from a checkout
    side effect (e.g., ``dvc pull`` from the package tree, or a third-
    party tool that did the equivalent).  The DVC cache (under
    ``data_root()``) is the canonical location for tracked data; the
    package tree is for code and ``.dvc`` sidecars only.

    This helper is used by ``local_file`` inside ``get_dataframe`` to
    refuse the workspace copy and force the read through the DVC cache
    path instead.

    A file *without* a sister sidecar is **not** pollution -- it could
    be freshly downloaded new data being prepped for ``dvc add``,
    scratch data the user supplied directly, output from the
    WB-fallback auto-add path in ``data_access.get_data_file``, or any
    other legitimate non-tracked use.  Returns False in all of those
    cases.
    """
    try:
        p = Path(fn).resolve()
        sidecar = p.parent / f"{p.name}.dvc"
        return sidecar.exists()
    except (OSError, ValueError):
        # Path-resolution failure (loop, permission denied) -> not pollution.
        # TypeError / AttributeError signal a programmer bug and propagate.
        return False


def _to_numeric(x,coerce=False):
    try:
        if coerce:
            return pd.to_numeric(x,errors='coerce')
        else:
            return pd.to_numeric(x)
    except (ValueError,TypeError):
        return x
    
@lru_cache(maxsize=3)
def _resolve_data_path(fn: str, stack_depth: int = 2) -> str:
    """Rewrite a relative path to land under data_root when appropriate.

    Handles three patterns:
      - ``../var/foo.parquet`` from country-level scripts (Uganda/_/)
      - bare ``foo.parquet`` from wave-level scripts (Uganda/2005-06/_/)
      - ``../2005-06/_/foo.parquet`` cross-wave refs from country-level scripts

    Always active so that data is always written outside the package tree.
    """
    fn_str = str(fn)
    p = Path(fn_str)

    # Only rewrite relative paths
    if p.is_absolute():
        return fn_str

    try:
        caller_file = Path(inspect.stack()[stack_depth].filename).resolve()
        rel = caller_file.relative_to(countries_root())
    except (IndexError, ValueError, TypeError):
        return fn_str

    parts = rel.parts  # e.g. ('Uganda', '_', 'food_acquired.py')
                        #   or ('Uganda', '2005-06', '_', 'shocks.py')
    if len(parts) < 2:
        return fn_str

    country = parts[0]

    # Pattern 1: ../var/foo.parquet  (country-level script)
    if fn_str.startswith("../var/"):
        name = fn_str[len("../var/"):]
        resolved = data_root(country) / "var" / name
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return str(resolved)

    # Pattern 2: bare filename from a wave-level script
    if len(parts) >= 3 and parts[1] != "_" and "/" not in fn_str:
        wave = parts[1]
        resolved = data_root(country) / wave / "_" / fn_str
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return str(resolved)

    # Pattern 3: ../{wave}/_/foo.parquet  (country-level script referencing wave output)
    # Used by country-level food_acquired.py scripts that concat wave-level parquets.
    m = re.match(r"^\.\./([^/]+)/_/(.+)$", fn_str)
    if m and parts[1] == "_":  # caller is a country-level script
        wave, name = m.group(1), m.group(2)
        resolved = data_root(country) / wave / "_" / name
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return str(resolved)

    return fn_str


def _try_get_data_file(fn: str | Path) -> Path | None:
    """Try to obtain *fn* via :func:`data_access.get_data_file`.

    Translates the various path forms used by :func:`get_dataframe`
    into a path relative to the countries directory (which is what
    ``get_data_file`` expects).

    Returns the local ``Path`` on success, or ``None``.
    """
    try:
        from .data_access import get_data_file
    except ImportError:
        # data_access import-time failure (missing optional dep) -> caller
        # falls back to other paths.
        return None

    p = Path(fn)
    # Already relative to countries dir (e.g. "Uganda/2013-14/Data/GSEC1.dta")
    if not p.is_absolute():
        rel = p
    else:
        # Absolute path — try to strip the countries prefix
        try:
            rel = p.relative_to(_COUNTRIES_DIR)
        except ValueError:
            return None

    return get_data_file(rel)


def get_dataframe(fn: str | Path, convert_categoricals: bool = True, encoding: str | None = None, categories_only: bool = False) -> pd.DataFrame:
    """From a file named fn, try to return a dataframe.

    Hope is that caller can be agnostic about file type,
    or if file is local or on a dvc remote.
    """
    fn = _resolve_data_path(fn)

    def local_file(fn):
    # Is the file local?
        try:
            with open(fn) as f:
                pass
        except FileNotFoundError:
            return False
        # Hardening: a workspace copy of a DVC-tracked file is pollution
        # from a checkout side effect (manual ``dvc pull``, third-party
        # tool, etc.), not a legitimate fast path.  Refuse to use it so
        # the read goes through the DVC cache (under ``data_root()``)
        # via ``file_system_path`` -> ``_ensure_dvc_pulled`` ->
        # ``DVCFS.open``.  Files without a sister sidecar are fine
        # (new data being prepped for ``dvc add``, user scratch data,
        # WB-fallback downloads).
        if _is_polluted_workspace_copy(fn):
            warnings.warn(
                f"Refusing workspace copy of DVC-tracked file {fn} "
                f"(sister .dvc sidecar exists). The package tree must "
                f"not contain DVC-tracked data; falling through to the "
                f"DVC cache path. Clean up with: "
                f"find lsms_library/countries -type f -name '*.dta' "
                f"-execdir test -e '{{}}.dvc' \\; -print -delete",
                stacklevel=3,
            )
            return False
        return True
    
    def file_system_path(fn):
        # Is the file DVC-tracked?  We answer this by checking for the
        # sidecar directly, NOT by trying ``DVCFS.open(fn)``.  The
        # latter walks the DVC sidecar tree (~10-20s on Lustre per
        # call) just to determine existence.  We only need to know
        # whether a ``.dvc`` sidecar exists at one of the standard
        # interpretations of ``fn``; that's a couple of stat() calls.
        fn_path = Path(fn)
        candidates = (
            [fn_path] if fn_path.is_absolute()
            else [Path.cwd() / fn_path, _COUNTRIES_DIR / fn_path]
        )
        for c in candidates:
            try:
                if (c.parent / f"{c.name}.dvc").exists():
                    return True
            except OSError:
                continue
        # Sidecar not found via the cheap path.  Fall back to DVCFS.open
        # for the rare case where the file is genuinely DVC-tracked but
        # not at one of the standard interpretations.
        try:
            with DVCFS.open(fn) as f:
                pass
            return True
        except FileNotFoundError:
            return False

    def _pyreadstat_via_tempfile(reader, f, suffix, **kwargs):
        """Call a pyreadstat reader on a stream by writing to a temp file."""
        f.seek(0)
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(f.read())
            tmp_path = tmp.name
        try:
            df_pr, meta = reader(tmp_path, **kwargs)
            return df_pr
        finally:
            os.unlink(tmp_path)

    def read_file(f,convert_categoricals=convert_categoricals,encoding=encoding):
        if isinstance(f,str):
            try:
                return pd.read_spss(f,convert_categoricals=convert_categoricals)
            except (pd.errors.ParserError, UnicodeDecodeError, TypeError):
                pass

            # pyreadstat fallback for SAV/SPSS files that pd.read_spss can't handle
            try:
                df_pr, meta = pyreadstat.read_sav(f, apply_value_formats=convert_categoricals, encoding=encoding)
                return df_pr
            except (ValueError, pyreadstat.ReadstatError, OSError,
                    NotImplementedError):
                # Format-fallback: parser/IO errors mean "try next reader".
                # Programmer bugs (TypeError, AttributeError) propagate.
                pass
        elif Path(fn).suffix.lower() == '.sav':
            # SAV file accessed as a stream (e.g. via DVC); pyreadstat needs a path
            try:
                return _pyreadstat_via_tempfile(
                    pyreadstat.read_sav, f, '.sav',
                    apply_value_formats=convert_categoricals, encoding=encoding)
            except (ValueError, pyreadstat.ReadstatError, OSError,
                    NotImplementedError):
                # Format-fallback: parser/IO errors mean "try next reader".
                pass

        try:
            return pd.read_parquet(f, engine='pyarrow')
        except (ArrowInvalid,):
            pass

        try:
            f.seek(0)
            return from_dta(f,convert_categoricals=convert_categoricals,encoding=encoding,categories_only=categories_only)
        except (ValueError, struct.error):
            pass

        try:
            # pyreadstat handles older Stata formats that pandas StataReader cannot.
            if isinstance(f, str):
                df_pr, meta = pyreadstat.read_dta(f, apply_value_formats=convert_categoricals, encoding=encoding)
                return df_pr
            else:
                return _pyreadstat_via_tempfile(
                    pyreadstat.read_dta, f, Path(fn).suffix or '.dta',
                    apply_value_formats=convert_categoricals, encoding=encoding)
        except (ValueError, pyreadstat.ReadstatError, OSError,
                NotImplementedError):
            # Format-fallback: parser/IO errors mean "try the next reader"
            # (csv, excel, feather, fwf).  Programmer bugs propagate.
            pass

        try:
            f.seek(0)
            return pd.read_csv(f,encoding=encoding)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        try:
            f.seek(0)
            return pd.read_excel(f)
        except (pd.errors.ParserError, UnicodeDecodeError, ValueError):
            pass

        try:
            f.seek(0)
            return pd.read_feather(f)
        except (pd.errors.ParserError, UnicodeDecodeError,ArrowInvalid) as e:
            pass

        try:
            f.seek(0)
            return pd.read_fwf(f)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass


        raise ValueError(f"Unknown file type for {fn}.")

    if local_file(fn):
        try:
            with open(fn,mode='rb') as f:
                df = read_file(f,convert_categoricals=convert_categoricals,encoding=encoding)
        except (TypeError,ValueError): # Needs filename?
            df = read_file(fn,convert_categoricals=convert_categoricals,encoding=encoding)
    elif file_system_path(fn):
        # Layer-1 (raw .dta blob) caching.  After ``_ensure_dvc_pulled``
        # populates the cache, we open the cache file *directly* rather
        # than via ``DVCFS.open()``.  Why: ``DVCFS.open()`` resolves the
        # virtual path through DVC's filesystem, which walks the
        # ``.dvc`` sidecar tree on Lustre (~10-20s per call when cold).
        # We already have everything needed to find the cache file: the
        # md5 from the sidecar (parsed by ``_dvc_cache_path``) and the
        # canonical cache layout at ``_DVC_CACHE_DIR``.  See
        # ``slurm_logs/HANDOFF_2026-05-07.org`` for the profiling data
        # (167s of a 198s ``Country.household_roster()`` call was in
        # ``_DVCFileSystem.open``; bypassing it brings that to ~ms).
        _ensure_dvc_pulled(fn)
        cache_path = _dvc_cache_path(fn)
        if cache_path is not None:
            try:
                with open(cache_path, mode='rb') as f:
                    df = read_file(f, convert_categoricals=convert_categoricals, encoding=encoding)
            except (TypeError, ValueError):  # Reader needs a filename, not a stream
                df = read_file(str(cache_path),
                               convert_categoricals=convert_categoricals,
                               encoding=encoding)
        else:
            # No usable cache file (sidecar missing/corrupt or fetch failed).
            # Fall back to ``DVCFS.open`` for streaming.  Slow but correct.
            try:
                with DVCFS.open(fn, mode='rb') as f:
                    df = read_file(f, convert_categoricals=convert_categoricals, encoding=encoding)
            except TypeError:  # Needs filename?
                df = read_file(fn, convert_categoricals=convert_categoricals, encoding=encoding)

    else:
        try:
            with dvc.api.open(fn,mode='rb') as f:
                df = read_file(f,convert_categoricals=convert_categoricals,encoding=encoding)
        except (OSError, ValueError, KeyError, ImportError):
            # DVC handle / config / network failure -> fall back to the WB
            # Microdata download path.  Programmer bugs (TypeError,
            # AttributeError) propagate so they aren't silently masked.
            local_path = _try_get_data_file(fn)
            if local_path is not None:
                with open(local_path, mode='rb') as f:
                    df = read_file(f, convert_categoricals=convert_categoricals, encoding=encoding)
            else:
                raise

    return df

#def regularize_string(s):

@build_transform()  # build-path: extraction/formatting baked verbatim into the L2 parquet (#522)
def df_data_grabber(fn: str | Path, idxvars: dict[str, Any] | str, convert_categoricals: bool = True, encoding: str | None = None, orgtbl: str | None = None, missing_ok: bool = False, **kwargs: Any) -> pd.DataFrame:
    """From a file named fn, grab both index variables and additional variables
    specified in kwargs and construct a pandas dataframe.

    A special case: if fn is an orgfile, grab orgtbl.

    For both idxvars and kwargs, expect one of the three following formats:

     - Simple: {newvarname:existingvarname}, where "newvarname" is the name of
      the variable we want in the final dataframe, and "existingvarname" is the
      name of the variable as it's found in fn; or

     - Tricky: {newvarname:(existingvarname,transformation)}, where varnames are
       as in "Simple", but where "transformation" is a function mapping the
       existing data into the form desired for newvarname; or

     - Trickier: {newvarname:(listofexistingvarnames,transformation)}, where newvarname is
       as in "Simple", but where "transformation" is a function mapping the variables in
       listofexistingvarnames into the form desired for newvarname.

    Options convert_categoricals and encoding are passed to get_dataframe, and
    are documented there.

    Ethan Ligon                                                      March 2024

    """

    def grabber(df,v):
        if isinstance(v,str): # Simple
            if v in df.columns:
                return df[v]
            elif missing_ok:
                return pd.Series(np.nan, index=df.index)
            else:
                raise KeyError(f"{v} not in columns of dataframe.")
        else:
            s,f = v
            if isinstance(f,types.FunctionType):  # Tricky & Trickier
                if isinstance(s,str):
                    if s not in df.columns:
                        if missing_ok:
                            return pd.Series(np.nan, index=df.index)
                        raise KeyError(f"{s} not in columns of dataframe.")
                    return df[s].apply(f)
                else:
                    missing_cols = [c for c in s if c not in df.columns]
                    if missing_cols:
                        if missing_ok:
                            return pd.Series(np.nan, index=df.index)
                        raise KeyError(f"{missing_cols} not in columns of dataframe.")
                    return df[s].apply(f,axis=1)
            elif isinstance(f,dict):
                    if isinstance(s, list) and len(s) > 1:
                        # s refers to multiple columns, we should apply row-wise
                        return df[s].apply(lambda row: f.get(tuple(row), row), axis=1)
                    else:
                        # s is a single column name (string) or single-element list
                        col = s if isinstance(s, str) else s[0]
                        return df[col].map(lambda x: f.get(x, x))
            elif isinstance(f, tuple):
                for i in f:
                    return grabber(df, (s, i))

        raise ValueError(df_data_grabber.__doc__)

    if orgtbl is None:
        df = get_dataframe(fn,convert_categoricals=convert_categoricals,encoding=encoding)
    else:
        df = df_from_orgfile(fn,name=orgtbl,encoding=encoding)
        if df.shape[0]==0:
            raise KeyError(f'No table {orgtbl} in {fn}.')

    out = {}

    if isinstance(idxvars,str):
        idxvars={idxvars:idxvars}

    for k,v in idxvars.items():
        out[k] = grabber(df,v)
        # All index variables are identifiers, not quantities.
        # Coerce to string via format_id to prevent float/int IDs
        # (e.g., grappe=1114002.0) and ensure consistent merge keys.
        out[k] = out[k].apply(format_id)

    out = pd.DataFrame(out)

    if len(kwargs):
        try:
            for k,v in kwargs.items():
                out[k] = grabber(df,v)
        except AttributeError as exc:
            # Legacy fallback: a myvar whose value is a literal source-column
            # name can be copied straight across when ``grabber`` cannot parse
            # it.  But when a ``(column, function)`` myvar's *function* raised
            # the AttributeError internally, this fallback instead masks it as
            # a misleading ``KeyError`` on the renamed target key -- which cost
            # real debugging time on GH #476.  Chain the original error so the
            # genuine cause survives when the fallback itself fails.
            try:
                if isinstance(kwargs,str):
                    out[k] = df[k]
                else: # A list?
                    for k in kwargs:
                        out[k] = df[k]
            except Exception as fallback_exc:
                raise fallback_exc from exc
    # When ``kwargs`` (myvars) is empty, ``out`` already holds just
    # the renamed idxvars frame (built above from the ``idxvars`` dict)
    # which is what ``set_index(list(idxvars.keys()))`` needs.  An
    # earlier ``else: out = df`` branch overwrote ``out`` with the raw
    # DataFrame, which still had source column names (``identif`` /
    # ``id4`` etc.) so ``set_index(['i','v'])`` raised
    # ``KeyError: "None of ['i','v'] are in the columns"``.  Two
    # countries hit this: Azerbaijan 1995 and Timor-Leste 2001 sample
    # df_cover (both declare a deliberately-empty ``myvars: {}``).

    out = out.set_index(list(idxvars.keys()))

    return out

def get_categorical_mapping(fn: str = 'categorical_mapping.org', tablename: str | None = None, idxvars: str = 'Code',
                            dirs: list[str] = ['./','../../_/','../../../_/'], asdict: bool = True, **kwargs: Any) -> dict[Any, Any] | pd.DataFrame | pd.Series:
    """Return mappings for categories.

    By default, searches for =tablename= in an orgfile
    'categorical_mapping.org'. But if fn is a path to a dta file instead,
    returns categories for tablename from the stata file.
    """
    ext = Path(fn).suffix

    if ext.lower()=='.dta': # A stata file.
        cats = get_dataframe(fn,convert_categoricals=True,categories_only=True)
        if tablename is None:
            return cats
        else:
            return cats[tablename]

    for d in dirs:
        try:
            if d[-1]!="/": d+='/'
            df = df_data_grabber(d+fn,idxvars,orgtbl=tablename,**kwargs)
            df = df.squeeze()
            if asdict:
                return df.to_dict()
            else:
                return df
        except (FileNotFoundError,KeyError) as error:
            exc = error

    exc.add_note(f"No table {tablename} found in any file {fn} in directories {dirs}.")
    raise exc


def harmonized_unit_labels(fn: str = '../../_/unitlabels.csv', key: str = 'Code', value: str = 'Preferred Label') -> dict[Any, str]:
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels = unitlabels.set_index(key)

    return unitlabels.squeeze().str.strip().to_dict()


def harmonized_food_labels(fn: str = '../../_/food_items.org', key: str = 'Code', value: str = 'Preferred Label') -> dict[Any, str]:
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()

def change_id(x: pd.DataFrame, fn: str | None = None, id0: str | None = None, id1: str | None = None, transform_id1: Any = None) -> pd.DataFrame:
    """Replace instances of id0 with id1.

    The identifier id0 is assumed to be unique.

    If mapping id0->id1 is not one-to-one, then id1 modified with
    suffixes of the form _%d, with %d replaced by a sequence of
    integers.
    """
    idx = x.index.names

    if fn is None:
        x = x.reset_index()
        if x['j'].dtype==float:
            x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)

        x = x.set_index(idx)

        return x

    try:
        with open(fn,mode='rb') as dta:
            id = from_dta(dta)
    except IOError:
        with dvc.api.open(fn,mode='rb') as dta:
            id = from_dta(dta)

    id = id[[id0,id1]]

    for column in id:
        if id[column].dtype==float:
            id[column] = id[column].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif id[column].dtype==int:
            id[column] = id[column].astype(str).replace('nan',pd.NA)
        elif id[column].dtype==object:
            id[column] = id[column].replace('nan',pd.NA)

    ids = dict(id[[id0,id1]].values.tolist())

    if transform_id1 is not None:
        ids = {k:transform_id1(v) for k,v in ids.items()}

    d = defaultdict(list)

    for k,v in ids.items():
        d[v] += [k]

    try:
        d.pop(np.nan)  # Get rid of nan key, if any
    except KeyError: pass

    updated_id = {}
    for k,v in d.items():
        if len(v)==1: updated_id[v[0]] = k
        else:
            for it,v_element in enumerate(v):
                updated_id[v_element] = '%s_%d' % (k,it)

    x = x.reset_index()
    x['j'] = x['j'].map(updated_id).fillna(x['j'])
    x = x.set_index(idx)

    assert x.index.is_unique, "Non-unique index."

    return x



def df_from_orgfile(orgfn: str | Path, name: str | None = None, set_columns: bool = True, to_numeric: bool = True, encoding: str | None = None) -> pd.DataFrame:
    """Extract the org table with name from the orgmode file named orgfn; return a pd.DataFrame.

    If name is None (the default), then we assume the orgtable is the very first
    thing in the file, with the possible exception of options (lines starting with #+).

    Note that we assume that cells with the string '---' should be null.

    Ethan Ligon                                                       March 2023
    """
    # Grab file as a list of strings
    with open(orgfn,'r',encoding=encoding) as f:
        contents = f.readlines()

    # Get indices of #+name: lines:
    names = [i for i,s in enumerate(contents) if f'#+name: {name}'.lower()==s.strip().lower()]

    if name is None:
        # No table requested: the table is assumed to be the first thing in the
        # file (modulo leading #+ option lines).  This is the documented
        # default and the only case in which a missing #+name header is OK.
        start = 0
    elif len(names)==0:
        # A specific table was requested but does not exist.  Do NOT silently
        # fall back to the first table in the file (GH #461): that silently
        # mis-maps (e.g. an absent harmonize_seed_crop returning the `u` units
        # table -> every crop labelled with a unit).  Raise a clear KeyError;
        # both df_data_grabber and get_categorical_mapping already catch it and
        # continue their search-path / add a contextual note.
        raise KeyError(f'No table named {name!r} in {orgfn}.')
    elif len(names)>1:
        start = names[0]
        warnings.warn(f'More than one table with {name} in {orgfn}.  Reading first one at line {start}.')
    else:
        start = names[0]

    # Advance to line that starts table
    i = start
    while contents[i].strip()[:2] == '#+': i +=1

    table =[]
    nextline = contents[i].strip()
    if set_columns and len(nextline) and nextline[0] == '|':
        columns = [s.strip() for s in nextline.split('|')[1:-1]]
        i+=1
        nextline = contents[i].strip()
    else:
        columns = None

    while len(nextline) and nextline[0] == '|':
        line = contents[i].strip()
        if line[-1] == '|' and  line[:2] != '|-':
            table.append([s.strip() for s in line.split('|')[1:-1]])
        i+=1
        try:
            nextline = contents[i].strip()
        except IndexError: # End of file?
            break

    df = pd.DataFrame(table,columns=columns)

    df = df.replace({'---':pd.NA})

    if to_numeric:
        # Try to convert columns to numeric types, but fail gracefully
        df = df.apply(_to_numeric)

    return df

def change_encoding(s: str, from_encoding: str, to_encoding: str = 'utf-8', errors: str = 'ignore') -> str:
    """
    Change encoding of a string s from_encoding to_encoding.

    For example, strings in data may be encoded in latin-1 or ISO-8859-1.
    We usually want utf-8.
    """
    return bytes(s,encoding=from_encoding).decode(to_encoding,errors=errors)

# ---------------------------------------------------------------------------
# L2 cache content-hash invalidation (v0.8.0)
# ---------------------------------------------------------------------------
# A harmonized parquet (L2-country ``var/{table}.parquet`` or L2-wave
# ``{wave}/_/{table}.parquet``) is stamped with a SHA-256 of everything
# that determines its *pre-finalize* contents.  On read, the stamp is
# recomputed and compared; a mismatch forces a rebuild from source.  See
# SkunkWorks/dvc_object_management.org ("v0.8.0 Roadmap") for the design
# and the converged decisions captured in its "Convergence" section.
#
# Bump LSMS_CACHE_SCHEMA when a LIBRARY-WIDE change to the *pre-write*
# extraction logic (Wave.grab_data / df_data_grabber / the parquet layout
# written by to_parquet) means existing parquets must be treated as stale
# even though their per-table source inputs are unchanged.  Do NOT bump
# for *post-read* transforms (Country._finalize_result, kinship expansion,
# canonical spellings, automatic categorical mappings, _join_v_from_sample)
# -- those re-run on every read and never touch the cached parquet.
#
# 2 (GH #323): the grain-aggregation policy became declarative (data_info.yml
#   `Aggregation:`), so a non-unique declared index now SUMS its additive
#   measures instead of dropping rows via groupby().first().  That collapse runs
#   PER WAVE, *before* the L2-country parquet is written (country.py
#   ~_normalize_dataframe_index in the load_from_waves loop), so the old lossy
#   result is baked into every warm cache -- the bug hides behind the cache the
#   bug poisoned.  Bumping invalidates them so the fix actually reaches readers.
LSMS_CACHE_SCHEMA = 2

# Schema-metadata key under which the content hash is embedded.  Embedding
# (rather than a ``{parquet}.hash`` sidecar) means the hash rotates
# atomically with the parquet via os.replace, can never desync from its
# data, and is removed together with the parquet by ``cache clear``.
_CACHE_HASH_KEY = b"lsms_cache_hash"

# Extensions treated as survey source data when scanning script-path
# tables for literal file references.
_DATA_SUFFIXES = (".dta", ".csv", ".tab", ".dat", ".sav", ".txt", ".xlsx",
                  ".xls", ".parquet")

def cached_file_hash(path: str | Path) -> str | None:
    """SHA-256 hex digest of *path*'s bytes, or ``None`` if absent/unreadable.

    The name is historical -- this hashes content directly on every call.
    An earlier (path, mtime, size) memo was removed because it could miss
    an edit that lands within the filesystem's mtime granularity while
    keeping the file size constant (a same-length one-character change to
    a sidecar md5 or a YAML value).  The inputs hashed here are tiny
    (sidecars ~91 B; ``data_info.yml`` / wave modules a few KB), so a
    direct read is sub-millisecond and the memo bought almost nothing on
    the L2 read path while introducing a real correctness gap.
    """
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def source_fingerprint(data_path: str | Path) -> str:
    """Fingerprint a survey source file for staleness hashing.

    Prefers the DVC sidecar (``{data_path}.dvc`` -- ~91 B carrying DVC's
    own md5 of the blob) so we never hash or download the multi-MB
    ``.dta``.  Falls back to hashing the local file content for untracked
    sources (a contributor mid-``dvc add``).  When neither exists the
    source is unverifiable and a ``missing:`` sentinel is returned so the
    composed hash still changes if the file later appears.
    """
    data_path = Path(data_path)
    sidecar = data_path.with_name(data_path.name + ".dvc")
    h = cached_file_hash(sidecar)
    if h is not None:
        return f"dvc:{data_path.name}:{h}"
    h = cached_file_hash(data_path)
    if h is not None:
        return f"raw:{data_path.name}:{h}"
    return f"missing:{data_path.name}"


def scan_script_data_refs(script_path: str | Path):
    """Yield data-file references that appear as *string literals* in a
    ``_/{table}.py`` script (e.g. ``'../Data/foo.dta'``).

    Uses ``ast`` so only genuine literals are picked up (not comments or
    expressions).  Dynamically-constructed paths are not discoverable;
    the script's own text is hashed separately, so a change to *which*
    file is read still busts the hash.  Only an out-of-band edit to a
    source that is BOTH undeclared AND referenced via a computed path
    escapes -- a documented limitation of script-path coverage.
    """
    try:
        src = Path(script_path).read_text()
    except OSError:
        return
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            val = node.value
            if val.lower().endswith(_DATA_SUFFIXES):
                yield val


def _tmp_sibling(fn: Path) -> Path:
    """A collision-free temp path in the same directory as *fn*.

    Uses pid + a uuid4 so two threads in the same process (e.g. a
    ``Feature()`` fan-out wrapped in a ThreadPool) never share a temp
    name -- which would let one thread's cleanup unlink the other's
    in-flight write.  Same-dir keeps ``os.replace`` atomic (same fs).
    """
    return fn.with_name(f"{fn.name}.tmp.{os.getpid()}.{uuid.uuid4().hex[:12]}")


def _atomic_write_table(table, fn: str | Path) -> None:
    """Write a pyarrow Table to *fn* via a temp file + ``os.replace``
    so concurrent readers never observe a torn parquet."""
    import pyarrow.parquet as pq
    fn = Path(fn)
    tmp = _tmp_sibling(fn)
    try:
        pq.write_table(table, str(tmp))
        os.replace(tmp, fn)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def read_parquet_cache_hash(path: str | Path) -> str | None:
    """Return the embedded ``lsms_cache_hash`` from a parquet's schema
    metadata, or ``None`` for a legacy parquet / unreadable footer.

    Reads only the footer (``read_schema``), so cost is ~1 ms regardless
    of row count -- safe on the L2 read hot path.
    """
    try:
        import pyarrow.parquet as pq
        md = pq.read_schema(str(path)).metadata
    except (OSError, ArrowInvalid):
        return None
    except Exception:
        return None
    if not md:
        return None
    raw = md.get(_CACHE_HASH_KEY)
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode()
    return raw


def cache_freshness(path: str | Path, expected_hash: str | None) -> str:
    """Classify an L2 parquet against the expected content hash.

    Returns one of:

    - ``'unverifiable'`` -- *expected_hash* is None (inputs couldn't be
      enumerated); preserve the v0.7.0 "read if present" behavior (no
      regression).
    - ``'fresh'`` -- stored hash matches expected.
    - ``'legacy'`` -- parquet predates hashing (no stored hash);
      trust-once, caller should re-stamp so the next read is guarded.
    - ``'stale'`` -- stored hash present and differs -> rebuild.
    """
    if expected_hash is None:
        return "unverifiable"
    stored = read_parquet_cache_hash(path)
    if stored is None:
        return "legacy"
    return "fresh" if stored == expected_hash else "stale"


def stamp_parquet_hash(path: str | Path, expected_hash: str | None) -> bool:
    """Best-effort: embed *expected_hash* into an existing parquet's
    schema metadata via an atomic rewrite.

    Used for trust-once-then-stamp migration of legacy / script-written
    parquets so subsequent reads are guarded.  Never raises; returns
    ``True`` on success (or when the stamp is already present).
    """
    if expected_hash is None:
        return False
    # Cheap writability probe first: on a read-only cache (pip install,
    # Savio Lustre) the rewrite can never succeed, so skip the expensive
    # full read_table and bail immediately.
    try:
        if not os.access(Path(path).parent, os.W_OK):
            return False
    except OSError:
        return False
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(str(path))
        md = dict(table.schema.metadata or {})
        if md.get(_CACHE_HASH_KEY) == expected_hash.encode():
            return True
        md[_CACHE_HASH_KEY] = expected_hash.encode()
        table = table.replace_schema_metadata(md)
        _atomic_write_table(table, path)
        return True
    except (OSError, ArrowInvalid):
        return False
    except Exception:
        return False


def to_parquet(df: pd.DataFrame, fn: str | Path, index: bool = True, absolute_path: bool = False, cache_hash: str | None = None) -> pd.DataFrame:
    """
    Write df to parquet file fn.

    Parquet (pyarrow) is slightly more picky about data types and layout than is pandas;
    here we fix some possible problems before calling pd.DataFrame.to_parquet.

    The write is atomic: data is written to a PID-temp file and then
    ``os.replace``-d into place, so a concurrent reader (multiple agents
    share the cache root) never observes a half-written parquet.

    Parameters
    ----------
    absolute_path : bool, default False
        If True, skip the call-stack-inspecting :func:`_resolve_data_path`
        rewrite and use ``fn`` verbatim.  Intended for library callers
        (not wave/country scripts) that build an absolute path themselves
        and don't want the caller-inference heuristic to run.
    cache_hash : str | None, default None
        If given, embed this string under ``lsms_cache_hash`` in the
        parquet's schema metadata.  Used by the L2 cache layer to stamp a
        content hash for staleness detection (see :func:`cache_freshness`).
        ``None`` writes a plain parquet (e.g. wave-script outputs, which
        are stamped trust-once on first read instead).
    """
    if not absolute_path:
        fn = _resolve_data_path(fn)
    if len(df.shape)==0: # A series?  Need a dataframe.
        df = pd.DataFrame(df)

    # Can't mix types of category labels.
    for col in df:
        if df[col].dtype == 'category':
            cats = df[col].cat.categories
            if str in [type(x) for x in cats]: # At least some categories are strings...
                df[col] = df[col].cat.rename_categories(lambda x: str(x))

    # Pyarrow can't deal with mixes of types in columns of type object. Just
    # convert them all to str.
    idxnames = df.index.names
    all = df.reset_index()
    for column in all:
        if all[column].dtype=='O':
            all[column] = all[column].astype(str).astype('string[pyarrow]').replace({'nan': None, 'None': None, '<NA>': None})
    if index:
        resolved_idxnames = []
        for i, name in enumerate(idxnames):
            if name is not None:
                resolved_idxnames.append(name)
            else:
                resolved_idxnames.append(all.columns[i])
        df = all.set_index(resolved_idxnames)
        df.index.names = idxnames
    else:
        df = all

    fn = Path(fn)
    if cache_hash is not None:
        import pyarrow as pa
        table = pa.Table.from_pandas(df, preserve_index=index)
        md = dict(table.schema.metadata or {})
        md[_CACHE_HASH_KEY] = cache_hash.encode()
        table = table.replace_schema_metadata(md)
        _atomic_write_table(table, fn)
    else:
        tmp = _tmp_sibling(fn)
        try:
            df.to_parquet(tmp, engine='pyarrow', index=index)
            os.replace(tmp, fn)
        except BaseException:
            try:
                tmp.unlink()
            except OSError:
                pass
            raise

    return df

from collections import UserDict

class RecursiveDict(UserDict):
    """Dict whose ``__getitem__`` transitively dereferences chained values.

    Used to collapse household-ID rewrite chains like ``A → B → C`` into
    a single lookup: ``RecursiveDict({'A': 'B', 'B': 'C'})['A'] == 'C'``.
    Keys that are not themselves values are returned unchanged. Backs
    :func:`id_walk` when applying a country's ``updated_ids`` mapping
    across waves. See the "Panel ID Transitive Chains" section of
    ``CLAUDE.md`` for the downstream consequences of getting this wrong.
    """
    def __init__(self,*arg,**kw):
      super(RecursiveDict, self).__init__(*arg, **kw)

    def __getitem__(self,k):
        try:
            while True:
                k = UserDict.__getitem__(self,k)
        except KeyError:
            return k

def format_id(id: Any, zeropadding: int = 0) -> str | None:
    """Canonical string form for a household, cluster, or person ID.

    Accepts either a numeric value (int, float, numpy scalar) or a
    string and returns a clean string suitable for use as a DataFrame
    index level. Used pervasively by :func:`df_data_grabber` when
    building the canonical ``(t, v, i, pid)`` index, and applied
    automatically to every ``idxvars`` entry (but NOT to ``myvars`` —
    see ``CLAUDE.md`` "``format_id`` and Numeric myvars").

    Rules:

    - Missing or empty input (``NaN``, ``None``, ``""``, ``"."``)
      returns ``None``.
    - Numeric input is cast to int then formatted as a decimal string.
      Float inputs lose their ``.0`` suffix; ``123.0`` → ``"123"``.
    - String input has surrounding whitespace removed.  A trailing
      ``.xxx`` decimal suffix is stripped *only* when the whole
      pre-decimal-point string is numeric (``"123.0"`` → ``"123"``,
      ``"-45.00"`` → ``"-45"``); arbitrary strings with internal or
      trailing periods (``"Mr."``, ``"Citrus, etc."``,
      ``"Coca-Cola, Inc."``) are passed through unchanged.  Leading
      zeros are preserved.
    - The result is left-padded with zeros to ``zeropadding`` width,
      so ``format_id(1, zeropadding=3)`` returns ``"001"``. Pass
      ``0`` (the default) to skip padding.

    Note
    ----
    The numeric-only decimal-strip is narrower than the historic
    ``s.split('.')[0]`` behaviour, which silently truncated any string
    containing a period.  See GH #222 for the regression that motivated
    the narrower form (Malawi ``harmonize_food`` labels ending in
    ``"etc."`` were having the trailing period dropped, breaking the
    rename against data that retained it).

    Parameters
    ----------
    id : Any
        The raw identifier. Typical inputs: a Stata numeric variable
        that should end up as a string index, or a pandas Series
        value during row-wise formatting.
    zeropadding : int, optional
        Target width for zero-padding. Defaults to 0 (no padding).

    Returns
    -------
    str or None
        The canonical string form, or ``None`` for missing/empty
        input.

    Examples
    --------
    >>> format_id(123)
    '123'
    >>> format_id(1, zeropadding=3)
    '001'
    >>> format_id("456.0")
    '456'
    >>> format_id("  007 ")
    '007'
    >>> format_id("-45.00")
    '-45'
    >>> format_id("Citrus, naartje, orange, etc.")
    'Citrus, naartje, orange, etc.'
    >>> format_id("Mr.")
    'Mr.'
    >>> format_id("123.abc")
    '123.abc'
    >>> format_id(float("nan"))
    >>> format_id("")
    """
    if pd.isnull(id) or id in ['','.']: return None

    try:  # If numeric, return as string int
        return ('%d' % id).zfill(zeropadding)
    except TypeError:  # Not numeric
        s = id.strip()
        # Strip a trailing ``.xxx`` decimal suffix only when the string
        # represents a stringified-numeric (``"123.0"``, ``"-45.00"``).
        # Arbitrary labels with internal or trailing periods are left
        # alone; see GH #222.
        if '.' in s:
            head, _, tail = s.partition('.')
            if head.lstrip('-').isdigit() and tail.isdigit():
                s = head
        return s.zfill(zeropadding)
    except ValueError:
        return None

def update_id(d: dict[str, str], id_splits: dict[str, int]) -> tuple[dict[str, str], dict[str, int]]:
    '''
    Update the dictionary d, which maps old ids to new ids, splits are followed by underscore ('_').
    For example:
        old_to_new_ids = {
                            'A': 'X',
                            'B': 'Y',
                            'C': 'X',
                            'D': 'Z',
                            'E': 'Y'
                        }
    would be updated to:
        updated_ids = {'A': 'X', 'C': 'X_1', 'B': 'Y', 'E': 'Y_1', 'D': 'Z'}
    '''
    D_inv = {}
    for k, v in d.items():
        if v not in D_inv:
            D_inv[v] = [k]
        else:
            D_inv[v].append(k)

    # GH #504: a minted suffix ``k_N`` must never equal an id a real household
    # already holds -- a target value carried forward into this wave
    # (``d.values()``) or one already emitted in this call.  The old code minted
    # ``k_N`` from a per-base counter that could re-use a *live* suffix (a
    # ``previous_i`` carried forward as ``k_N``), silently collapsing two
    # distinct lineages onto one canonical id (60 such collisions in Malawi
    # 2019-20; they propagate to anthropometry/sample/plot_features via the
    # groupby().first() collapse).  Guard the mint by bumping past any clash.
    # Single-source and ``it == 0`` passes are unchanged, so non-colliding
    # households keep their original ids (byte-stable for healthy panels).
    live = set(d.values())
    emitted = set()

    updated_id = {}
    for k, v in D_inv.items():
        if len(v)==1:
            updated_id[v[0]] = k
            id_splits[k] = 0
            emitted.add(k)
        else:
            for it,v_element in enumerate(v):
                if it == 0:
                    cand = k
                else:
                    split = id_splits.get(k, 0) + it
                    cand = '%s_%d' % (k, split)
                    while cand in live or cand in emitted:
                        split += 1
                        cand = '%s_%d' % (k, split)
                updated_id[v_element] = cand
                emitted.add(cand)
            id_splits[k] = id_splits.get(k, 0)+len(v)-1

    return updated_id, id_splits


def panel_ids(Waves: dict[str, Any] | pd.DataFrame) -> tuple[RecursiveDict, dict[str, dict[str, str]]]:
    '''
    Input: DataFrame with a MultiIndex that includes a level named 't' representing the wave and 'i' current househod ID'
            And single 'previous_i' column as the previous household ID.
    Output: Wave-specific panel id mapping dictionaires and a recursive dictionary of tuple of (wave, household identifiers)
    '''
    if isinstance(Waves, dict):
        dfs = []
        for wave_year, wave_info in Waves.items():
            if not wave_info:
                continue  # skip empty entries

            file_path = f"../{wave_year}/Data/{wave_info[0]}"
            if isinstance(wave_info[1], list):
                columns = wave_info[1]
            else:
                columns = [wave_info[1], wave_info[2]]

            df = get_dataframe(file_path)[columns]

            # Process mapping when recent_id is a list (list-based mapping)
            if isinstance(wave_info[1], list): #tanzania
                df = wave_info[2](df, wave_info[1])
            else:
                df.loc[:,wave_info[1]] = df[wave_info[1]].apply(format_id)
                df.loc[:,wave_info[2]] = df[wave_info[2]].apply(format_id)
                # If a transformation function is provided (tuple length 4), apply it to the old_id column
                if len(wave_info) == 4:
                    df.loc[:,wave_info[2]] = df[wave_info[2]].apply(wave_info[3])
                df.loc[:,'t'] = wave_year
                df = df.rename(columns={wave_info[1]: 'i', wave_info[2]: 'previous_i'})
                df = df.set_index(['t', 'i'])[['previous_i']]
            dfs.append(df)
        panel_ids_df = pd.concat(dfs, axis=0)
    else:
        # If Waves is not a dictionary, assume it's a DataFrame
        panel_ids_df = Waves.copy()

    updated_wave = {}
    check_id_split = {}
    sorted_waves = sorted(panel_ids_df.index.get_level_values('t').unique())
    recursive_D = RecursiveDict()
    for wave_year in sorted_waves:
        df = panel_ids_df[panel_ids_df.index.get_level_values('t') == wave_year].copy().reset_index()
        wave_matches = df[['i', 'previous_i']].dropna().set_index('i')['previous_i'].to_dict()
        previous_wave = sorted_waves[sorted_waves.index(wave_year) - 1] if sorted_waves.index(wave_year) > 0 else None
        if previous_wave:
            previous_wave_matches = updated_wave[previous_wave]
            # update the current wave matches dictionary values to the previous wave matches
            wave_matches = {k: previous_wave_matches.get(v, v)for k, v in wave_matches.items()}
            recursive_D.update({(wave_year, k): (previous_wave, v) for k, v in wave_matches.items()})
        wave_matches, check_id_split = update_id(wave_matches,  check_id_split)
        updated_wave[wave_year] = wave_matches
    return recursive_D, updated_wave

def _close_id_map(mapping: dict[str, str], wave: str | None = None) -> dict[str, str]:
    """Resolve transitive chains in an id-rename mapping.

    ``pandas.DataFrame.rename(index=mapping)`` performs a single
    substitution per index entry (``x -> mapping.get(x, x)``); it does
    not iterate to a fixed point.  When ``mapping`` contains a chain
    ``{A: B, B: C}`` the rename only advances each entry by one link,
    leaving the result in a non-canonical state.  A *second* application
    then advances the survivors another step, which is how
    already-walked rows can collide with already-canonical rows and
    produce duplicate ``(i, t)`` tuples.  This is the bug class fixed
    in commit ``4db41a27`` (Burkina Faso 2021-22) by patching the
    ``df.attrs`` propagation; closure-resolving the mapping fixes the
    underlying non-idempotence at the source.

    Given ``{A: B, B: C}`` returns ``{A: C, B: C}``: every value in the
    returned mapping is a *terminal* id — it is never itself a key
    (except for self-mappings, which are left intact as harmless
    no-ops).  Applying the closed mapping twice is therefore the same
    as applying it once.

    Cycles (``A -> B -> A``) raise ``ValueError`` since they indicate
    corrupt panel-id data — there is no consistent terminal id.

    Parameters
    ----------
    mapping : dict[str, str]
        One wave's slice of ``updated_ids``.
    wave : str, optional
        Wave label, used only in error messages.

    Returns
    -------
    dict[str, str]
        Closure-resolved mapping with the same domain as ``mapping``.
    """
    closed: dict[str, str] = {}
    for k in mapping:
        # Walk until ``v`` is terminal: not a key, or self-mapped.
        # A self-map is the natural fixed point and not a cycle.  Any
        # other revisit of an id is a true cycle.
        seen = [k]
        v = mapping[k]
        while v in mapping and mapping[v] != v:
            if v in seen:
                chain = ' -> '.join(seen + [v])
                raise ValueError(
                    f"Cycle detected in updated_ids"
                    f"{f' for wave {wave!r}' if wave else ''}: {chain}"
                )
            seen.append(v)
            v = mapping[v]
        closed[k] = v
    return closed


def id_walk(df: pd.DataFrame, updated_ids: dict[str, dict[str, str]], hh_index: str = 'i') -> pd.DataFrame:
    '''
    Updates household IDs in panel data across different waves separately.

    Parameters:
        df (DataFrame): Panel data with a MultiIndex, including 't' for wave and 'i' (default) for household ID.
        updated_ids (dict): A dictionary mapping each wave to another dictionary that maps original household IDs to updated IDs.
            Format:
                {wave_1: {original_id: new_id, ...},
                 wave_2: {original_id: new_id, ...}, ...}
        hh_index (str): Index name for the household ID level (default is 'i').

    Example:
        updated_ids = {
            '2013-14': {'0001-001': '101012150028', '0009-001': '101015620053', '0005-001': '101012150022'},
            '2016-17': {'0001-002': '0001-001', '0003-001': '0005-001', '0005-001': '0009-001'}
        }

        In this example, IDs are updated independently for each wave.
        Because the same original household ID across different waves may not represent the same household.
        Specifically, household '0005-001' in wave '2016-17' corresponds to household '0009-001' from wave '2013-14', not '0005-001' from '2013-14'.

    The function handles these wave-specific mappings separately, ensuring accurate household identification over time.

    Idempotence
    -----------
    Each wave's mapping is closure-resolved via :func:`_close_id_map`
    before application, so ``id_walk(id_walk(df, ui)) == id_walk(df, ui)``
    by construction.  ``df.attrs['id_converted']`` is still set as a
    fast-path hint — the framework checks it in ``_finalize_result`` to
    skip a redundant pass — but losing the flag (e.g. via
    :meth:`.set_index`, :meth:`.merge`) is no longer a correctness
    hazard.  See ``tests/test_id_walk.py`` for the regression cases.
    '''
    # Guard: a frame with no rows (e.g. a wave whose source yielded nothing)
    # or one lacking a 't' level has no waves to split on; the final
    # ``pd.concat([])`` would otherwise raise "No objects to concatenate".
    # Empty-in -> empty-out, and the id_converted flag keeps it idempotent.
    if df.empty or 't' not in (df.index.names or []):
        df.attrs['id_converted'] = True
        return df

    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids and updated_ids[wave]:
            closed = _close_id_map(updated_ids[wave], wave=wave)
            df_wave = df_wave.rename(index=closed, level=hh_index)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    # df= df.rename(index=updated_ids,level=['t', hh_index])
    df.attrs['id_converted'] = True
    return df

        
def conversion_table_matching_global(df: pd.DataFrame, conversions: pd.DataFrame, conversion_label_name: str, num_matches: int = 3, cutoff: float = 0.6) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Returns a Dataframe containing matches and Dictionary mapping top choice
    from a conversion table's labels to item labels from a given df.

    """
    D = defaultdict(dict)
    all_matches = pd.DataFrame(columns=["Conversion Table Label"] +
                               ["Match " + str(n) for n in range(1, num_matches + 1)])
    # difflib.get_close_matches calls len() on every candidate, so any NaN
    # or non-string entry crashes with "TypeError: object of type 'float' has
    # no len()". Coerce both sides to string and drop NA before matching.
    items_unique = df['i'].dropna().astype(str).str.capitalize().unique()
    for l in conversions[conversion_label_name].dropna().astype(str).unique():
        k = difflib.get_close_matches(l.capitalize(), items_unique, n = num_matches, cutoff=cutoff)
        if len(k):
            D[l] = k[0]
            k = [l] + k
            all_matches.loc[len(all_matches.index)] = k + [np.nan] * (num_matches + 1 - len(k))
        else:
            D[l] = l
            all_matches.loc[len(all_matches.index)] = [l] + [np.nan] * num_matches
    return all_matches, D

def category_union(dict_list: list[dict[Any, Any]]) -> tuple[dict[int, Any], ...]:
    """Construct union of a list of dictionaries, preserving unique *values*.

    Returns this union, as well as a list of dictionaries mapping the original
    dicts into the union.

    >>> c1={1:'a',2:'b',3:'c'}
    >>> c2={1:'b',2:'c',3:'d',4:"a'"}
    >>> c0,t1,t2 = category_union([c1,c2])
    >>> c0[t1[2]]==c1[2]
    True
    """
    cv = []
    for i in range(len(dict_list)):
        cv += list(set(dict_list[i].values()))

    cv = list(set(cv))

    c0 = dict(zip(range(len(cv)),cv))

    c0inv = {v:k for k,v in c0.items()}

    t = []
    for i in range(len(dict_list)):
        t.append({k:c0inv[v] for k,v in dict_list[i].items()})

    return c0,*tuple(t)

def category_remap(c: dict[Any, Any], remaps: dict[Any, Any]) -> dict[Any, Any]:
    """
    Return a "remapped" dictionary.

    This is a composition of two dictionaries.
    A dictionary remaps values in dict c into other values in c.
    """
    cinv = {v:k for k,v in c.items()}
    for k,v in remaps.items():
        c[cinv[k]] = v

    return c

def panel_attrition(df: pd.DataFrame, waves: list[str], index: str = 'i', return_ids: bool = False, split_households_new_sample: bool = True) -> pd.DataFrame | tuple[pd.DataFrame, dict[tuple[str, str], set[str]]]:
    """
    Produce an upper-triangular) matrix showing the number of households (j) that
    transition between rounds (t) of df.
            split_households_new_sample (bool): Determines how to count split households:
                                - If True, we assume split_households as new sample. So we
                                     do not count and trace splitted household, only counts 
                                     the primary household in each split. The number represents
                                     how many main (primary) households in previous waves have 
                                     appeared in current round.
                                - If False, counts all split households that can be traced 
                                    back to previous wave households. The number represents how 
                                    many households (including splitted households
                                    round can be traced back to the previous round.
    
    Note: First three rounds used same sample. Splits of the main households may happen in different rounds.
    """
    waves = sorted(waves)
    df_reset = df.reset_index()
    idx_by_wave = df_reset.groupby('t')[index].apply(set).to_dict()

    # Precompute parent ID mappings for all IDs in all waves
    parent_ids_map = {
        t: {i: ['_'.join(i.split('_')[:-n]) for n in range(1, len(i.split('_')))] 
            for i in ids} 
        for t, ids in idx_by_wave.items()
    }

    foo = pd.DataFrame(index=waves, columns=waves)
    IDs = {}

    for i, s in enumerate(waves):
        ids_s = idx_by_wave[s]
        for t in waves[i:]:
            ids_t = idx_by_wave[t]
            common = ids_s & ids_t

            if not split_households_new_sample:
                # Consider split households
                additional = {i for i in ids_t - common
                              for p in parent_ids_map[t].get(i, [])
                              if p in ids_s}
                common |= additional

            IDs[(s, t)] = common
            foo.loc[s, t] = len(common)

    return (foo, IDs) if return_ids else foo

def write_df_to_org(df: pd.DataFrame, table_name: str, filepath: str | Path | None = None) -> str | None:
    '''
    Writes a DataFrame to an Org-mode table format.
    Parameters:
    df (pandas.DataFrame): The DataFrame to be converted and written.
    table_name (str): The name to be assigned to the Org table.
    filepath (str, optional): The file path where the Org table will be written. 
                              If None, the function returns the Org table as a string, used in Emacs Python Block.
    Returns:
    str: The Org table as a string if filepath is None.
    '''

    from cfe.df_utils import df_to_orgtbl  # lazy: heavy import deferred to first use
    if filepath is not None:
        mode = 'a' if Path(filepath).exists() else 'w'
        with open(filepath, mode, encoding="utf-8") as file:
            file.write(f"#+NAME: {table_name}\n")
            file.write(df_to_orgtbl(df))  # Convert label DataFrame to Org table
            file.write("\n\n")  # Add spacing
    else:
        s = f"#+NAME: {table_name}\n"
        s += df_to_orgtbl(df)
        s += "\n\n"
        return s
    
def map_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map index from old parquet file to new index used in data_info.yml
    -- March 11, 2025
    """
    index_names = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]

    mapping_rules = {}
    if 'w' in index_names:
        mapping_rules['w'] = 't'

    if 'u' in df.index.names:
        df = df.rename(index={k: 'unit' for k in ['<NA>', 'nan', pd.NA]}, level='u')

    if mapping_rules:
        df = df.rename_axis(index=mapping_rules)
        index_names = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]

    needs_swap = False
    if 'j' in index_names:
        if 'i' not in index_names:
            # The legacy j->i swap rescues OLD household parquets that stored
            # the household id under 'j'.  A cluster-level item feature
            # (e.g. community_prices, indexed by the cluster 'v' with NO
            # household 'i') legitimately uses 'j' for the food item -- swapping
            # it to 'i' makes 'j' undeclared and the food item gets dropped +
            # collapsed away downstream (silent data loss).  Detect that shape
            # by the presence of 'v' without 'i' and leave 'j' intact.
            needs_swap = 'v' not in index_names
        else:
            try:
                needs_swap = index_names.index('j') < index_names.index('i')
            except ValueError:
                needs_swap = True

    if needs_swap:
        swap_rules = {
            'i': 'temp_j',
            'j': 'i',
            'previous_j': 'previous_i'
        }
        df = df.rename_axis(index=swap_rules)
        df = df.rename_axis(index={'temp_j': 'j'})

    return df


def melt_visit_intervals(df: pd.DataFrame,
                         start_base: str = 'int_start', end_base: str = 'int_end',
                         out_start: str = 'Interview start', out_end: str = 'Interview end',
                         visit_level: str = 'visit') -> pd.DataFrame:
    """Melt per-visit interview ``(start[, end])`` timestamps onto a ``visit`` index.

    Shared ``df_edit``-hook helper for the ``interview_date`` table (an
    application of the grain-aggregation-policy, SkunkWorks/
    grain_aggregation_policy.org).  The number of enumerator visits needed to
    complete a household questionnaire is distinct from the survey *round*
    (post-planting / post-harvest, already carried by ``t``); some surveys
    record a start (and end) timestamp per such visit.  We preserve every
    visit on an ordinal ``visit`` level rather than keeping only the first.

    Input columns (target names produced by the wave ``myvars``; both lower-
    and capitalised first-letter spellings accepted):

    - visit 1 start = ``start_base`` (default ``int_start``); end = ``end_base``
      (default ``int_end``);
    - visit n>=2 start = ``{start_base}_v{n}``; end = ``{end_base}_v{n}``.

    The END columns are OPTIONAL: a survey that records only a start timestamp
    (e.g. a single interview *date*) supplies just the start columns, and the
    output then carries only ``out_start`` -- the all-NaT ``out_end`` column is
    dropped.  Set ``out_start='Int_t'`` (with no end columns present) to
    reproduce the legacy single-datetime-per-visit shape.

    Output: indexed ``(<original idx>, visit)`` with columns ``out_start`` and
    (where any end is recorded) ``out_end``; ``visit`` is an ordinal ``Int64``
    (1, 2, 3, ...).  A visit whose start AND end are both NaT is DROPPED --
    never fabricated -- so a household not visited that many times does not
    surface at that visit number.  Collapsing ``visit`` with the declared
    ``first`` aggregation reproduces the legacy first-visit-only table.
    """
    def _pick(*names):
        return next((n for n in names if n in df.columns), None)

    def _col(base, n):
        cap = base[:1].upper() + base[1:]
        if n == 1:
            return _pick(base, cap)
        return _pick(f'{base}_v{n}', f'{cap}_v{n}')

    # Discover visits 1..N: stop at the first ordinal with neither a start nor
    # an end column declared.
    specs = []
    n = 1
    while True:
        s, e = _col(start_base, n), _col(end_base, n)
        if s is None and e is None:
            break
        specs.append((n, s, e))
        n += 1
    if not specs:
        return df  # nothing to melt; hand back unchanged (defensive)

    idx_names = list(df.index.names)
    flat = df.reset_index()
    pieces = []
    for vnum, s, e in specs:
        part = flat[idx_names].copy()
        part[visit_level] = vnum
        part[out_start] = pd.to_datetime(flat[s], errors='coerce') if s else pd.NaT
        part[out_end] = pd.to_datetime(flat[e], errors='coerce') if e else pd.NaT
        pieces.append(part)

    out = pd.concat(pieces, ignore_index=True)
    # Drop un-made visits (both start and end NaT); never fabricate a visit.
    out = out[out[out_start].notna() | out[out_end].notna()]
    out[visit_level] = out[visit_level].astype('Int64')
    # Start-only surveys: drop the all-NaT end column entirely.
    if out_end in out.columns and out[out_end].isna().all():
        out = out.drop(columns=[out_end])
    return out.set_index(idx_names + [visit_level])


import importlib.util
def get_formatting_functions(mod_path: Path, name: str, general_formatting_functions: dict[str, Any] = {}) -> dict[str, Any]:
    formatting_function = general_formatting_functions.copy()
    if mod_path.exists():
    # Load module dynamically
        spec = importlib.util.spec_from_file_location(name, mod_path)
        formatting_module = importlib.util.module_from_spec(spec)
        if spec.loader is not None:
            spec.loader.exec_module(formatting_module)
        formatting_function.update({
            name: func
            for name, func in vars(formatting_module).items()
            if callable(func)
            })
        return formatting_function
    else:
        formatting_function.update({})
        return formatting_function

def age_handler(age = None, interview_date = None, interview_year = None, format_interv = None, dob = None, format_dob  = None, m = None, d = None, y = None):
    '''
    a function to calculate the age of an individual with the best available information, prioritizes more precise estimates, in cases where day is unknown, the middle of the month is used

    Args:
        interview_date : interview date, can be passed as a list in [y, m, d] format, supports dropping columns from right
        interview_year: year of interview; please enter an estimation in case an interview date is not found
        format_interv: argument to be passed into pd.to_datetime(, format=) for interview_date
        age : age in years
        dob: date of birth, if given in a single column
        format_dob: to be passed into pd.to_datetime(, format=) for date of birth
        m, d, y: month, day, and year of birth respectively, if specified in separate columns

    Returns:
    a float representing the best estimate of the age of an individual
    '''

    final_date_of_birth = None
    final_interview_date = None
    interview_yr = int(interview_year)
    year_born = y

    def is_valid(x):
        """Validate a numeric date component (year, month, day).

        Lower-bounds at 0 so missing-value sentinels common in
        LSMS/EHCVM raw data — ``-1``, ``-99``, ``-97``, ``-98`` —
        are rejected.  Upper-bounds at 2100 to also catch the
        ``9999`` "missing year" sentinel.  Lists must have every
        element pass.
        """
        if isinstance(x, list):
            return all(pd.notna(x)) and all([is_valid(i) for i in x])
        elif pd.notna(x):
            try:
                v = int(float(x))
            except (TypeError, ValueError):
                return False
            return 0 <= v < 2100
        return False

    def _is_plausible_age(x):
        """Validate a reported-age input as a biologically plausible value.

        Distinct from ``is_valid``: ages live in [0, 120], not the
        [0, 2100) range used for year/month/day components.  This
        guard means callers no longer need per-wave ``_clean_age``
        lambdas to filter ``-1`` / ``999`` / ``130`` / ``150``
        sentinels out of reported-age columns.
        """
        if not pd.notna(x):
            return False
        try:
            v = int(float(x))
        except (TypeError, ValueError):
            return False
        return 0 <= v <= 120

    # --- Parse interview date (needed for DOB-based age) ---
    # Note on the ``isinstance(... list)`` short-circuit: ``pd.notna`` on
    # a list returns an ndarray of bools whose truth value is ambiguous,
    # so we must check ``isinstance(... list)`` *before* ``pd.notna``.
    # Without the short-circuit, callers passing ``interview_date=[y,
    # m, d]`` get ``ValueError: The truth value of an array with more
    # than one element is ambiguous`` instead of the documented
    # list-form behaviour.
    if interview_date is not None and (
        isinstance(interview_date, list) or pd.notna(interview_date)
    ):
        if isinstance(interview_date, list):
            if len(interview_date) == 3 and is_valid(interview_date):
                interview_date = str(int(interview_date[1])) + '/' + str(int(interview_date[2])) + '/' + str(int(interview_date[0]))
                format_interv = "%m/%d/%Y"
            elif len(interview_date) >= 2 and is_valid(interview_date):
                interview_date = str(int(interview_date[1])) + '/15/' + str(int(interview_date[0]))
                format_interv = "%m/%d/%Y"
            elif len(interview_date) >= 1 and is_valid(interview_date):
                interview_yr = int(interview_date[0])
        if format_interv:
            final_interview_date = pd.to_datetime(interview_date, format = format_interv)
            interview_yr = final_interview_date.year
        else:
            try:
                final_interview_date = pd.to_datetime(interview_date)
            except (ValueError, TypeError, pd.errors.ParserError):
                final_interview_date = None

    # --- Parse DOB ---
    def _safe_dob(m_, d_, y_):
        """Build a Timestamp from (y, m, d) with graceful downgrade.

        ``is_valid`` only checks each component individually; it does not
        verify that the combination forms a real calendar date.  Without
        this guard, rows with impossible combos (Feb 30, Apr 31, Feb 29
        in a non-leap year, m=0 or m=13 sentinels) crash
        ``pd.to_datetime`` with ``ValueError: day is out of range``.

        Tries the full ``(y, m, d)`` first; if that's not a real date but
        ``(y, m, 15)`` is, returns the month-middle (same precision as
        the year+month-only path).  Returns ``None`` only when the month
        itself is unsalvageable.
        """
        try:
            return pd.Timestamp(date(int(y_), int(m_), int(d_)))
        except (ValueError, TypeError):
            try:
                return pd.Timestamp(date(int(y_), int(m_), 15))
            except (ValueError, TypeError):
                return None

    if dob is not None and pd.notna(dob):
        final_date_of_birth = pd.to_datetime(dob, format = format_dob)
        year_born = final_date_of_birth.year
    elif is_valid([m, d, y]):
        final_date_of_birth = _safe_dob(m, d, y)
    elif is_valid([m, y]):
        try:
            final_date_of_birth = pd.Timestamp(date(int(y), int(m), 15))
        except (ValueError, TypeError):
            # Unsalvageable month (e.g. m=0 or m=13 sentinel); fall
            # through to the year-math path via ``year_born``.
            final_date_of_birth = None

    # --- Compute DOB-derived age if both dates are available ---
    dob_age = None
    if final_interview_date and final_date_of_birth:
        candidate = round((final_interview_date - final_date_of_birth).days / 365.25, 2)
        if 0 <= candidate <= 120:
            dob_age = candidate

    # --- Choose best age estimate ---
    has_reported_age = age is not None and _is_plausible_age(age)

    if has_reported_age and dob_age is not None:
        # Both available: prefer DOB precision when they agree
        # (int(age) truncates, so a 1-year tolerance covers rounding)
        int_age = int(age)
        if abs(int_age - dob_age) <= 1:
            return dob_age
        else:
            return int_age  # disagreement — trust the reported age
    elif dob_age is not None:
        return dob_age
    elif has_reported_age:
        return int(age)
    elif is_valid([year_born, interview_yr]):
        # Year-math fallback: interview_year - year_born.  Clamp to
        # the biological range so swapped-year data errors and
        # implausible chains return NaN rather than a misleading
        # negative or super-centenarian age.
        candidate = int(interview_yr) - int(year_born)
        if 0 <= candidate <= 120:
            return candidate
        return np.nan
    else:
        return np.nan

def age_handler_wrapper(df, interview_date = None, interview_year = None, format_interv = None, age = None, dob = None, format_dob = None, m = None, d = None, y = None):
    '''
    a function that calculates ages for rows in a dataframe by calling age_handler 

    Args:
        df: the dataframe
        interview_date : column interview date
        interview_year: int year of interview or column, should exist for all rows
        format_interv: argument to be passed into pd.to_datetime(, format=) for interview_date
        age : column age
        dob: column date of birth
        format_dob: to be passed into pd.to_datetime(, format=) for date of birth
        m, d, y: columns month, day, and year of birth respectively

    Returns:
    returns a new Series with the calculated ages
    '''
    
    def row_funct(row):
        r_age = row[age] if age else None
        r_interview_date = row[interview_date] if interview_date else None
        if isinstance(interview_year, str):
            iy = row[interview_year]
        else:
            iy = interview_year
        r_dob = row[dob] if dob else None
        r_m = row[m] if m else None
        r_d = row[d] if d else None
        r_y = row[y] if y else None
        return age_handler(age = r_age, interview_date = r_interview_date, interview_year = iy, format_interv = format_interv, dob = r_dob, format_dob  = format_dob, m = r_m, d = r_d, y = r_y)

    return df.apply(row_funct, axis=1)




def all_dfs_from_orgfile(orgfn: str | Path, set_columns: bool = True, to_numeric: bool = True, encoding: str | None = None) -> dict[str, pd.DataFrame]:
    """
    Read all named org-mode tables from a .org file and return as a dictionary of DataFrames.
    
    Parameters:
        orgfn (str): Path to the org file.
        set_columns (bool): Whether to use the first table row as column names.
        to_numeric (bool): Attempt to convert all values to numeric.
    
    Returns:
        dict: {table_name: DataFrame}
    """
    with open(orgfn, 'r', encoding=encoding) as f:
        contents = f.readlines()

    tables = {}
    i = 0
    while i < len(contents):
        line = contents[i].strip()
        
        if line.lower().startswith('#+name:'):
            table_name = line.split(':', 1)[1].strip()
            i += 1
            # Skip any extra #+ lines
            while i < len(contents) and contents[i].strip().startswith('#+'):
                i += 1
            
            # Start reading table
            table_lines = []
            header = None

            if i < len(contents) and contents[i].strip().startswith('|'):
                if set_columns:
                    header = [s.strip() for s in contents[i].strip().split('|')[1:-1]]
                    i += 1

            while i < len(contents) and contents[i].strip().startswith('|'):
                row = contents[i].strip()
                if row.startswith('|-'):  # separator line
                    i += 1
                    continue
                row_data = [s.strip() for s in row.split('|')[1:-1]]
                table_lines.append(row_data)
                i += 1

            df = pd.DataFrame(table_lines, columns=header if set_columns else None)
            df = df.replace({'---': np.nan})

            if to_numeric:
                df = df.apply(_to_numeric)

            tables[table_name] = df
        else:
            i += 1

    return tables
