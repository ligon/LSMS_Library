"""Confined DVC runtime metadata; original country metadata stays live.

The supplied-Repo filesystem seam is documented by DVC. The tmp/site-cache,
SCM-context and lazy filesystem hooks below are explicitly private dependency
adapters (GH #1032), not public DVC configuration. Tested dependency: 3.67.1;
other versions in the project's declared range must satisfy the same hooks.
"""
from __future__ import annotations

import hashlib
import inspect
import os
import shutil
import tempfile
import threading
from functools import cached_property
from importlib.metadata import version
from pathlib import Path

from dvc.api import DVCFileSystem as _PublicDVCFileSystem
from dvc.fs.dvc import DVCFileSystem, _DVCFileSystem
from dvc.repo import Repo
from dvc.repo.scm_context import SCMContext
from packaging.version import Version
from platformdirs import user_cache_path

from .paths import data_root


class DVCMetadataCompatibilityError(Exception):
    """Unsupported runtime adapter; never an unavailable source or recipe."""


def _check_capabilities() -> None:
    """Check construction hooks before allocating or mutating metadata.

    This is not a version-exact pin or a claim that signatures prove all future
    DVC semantics. The source-bound regression controls are also required.
    """
    installed = Version(version("dvc"))
    if not Version("3.67.0") <= installed < Version("4"):
        raise DVCMetadataCompatibilityError(
            f"DVC {installed} is outside the declared >=3.67.0,<4 support range")
    try:
        # dvc.api exports the fsspec INNER class. Our fs hook belongs to the
        # separate dvc_objects OUTER wrapper; the first prototype mixed them.
        if _PublicDVCFileSystem is not _DVCFileSystem:
            raise TypeError("DVC public supplied-Repo API alias changed")
        if RuntimeDVCFileSystem.__bases__ != (DVCFileSystem,):
            raise TypeError("Runtime filesystem is not bound to DVC's outer wrapper")
        fs_hook = inspect.getattr_static(DVCFileSystem, "fs")
        if not hasattr(fs_hook, "__get__"):
            raise TypeError("DVC outer wrapper has no lazy fs descriptor")
        inspect.signature(Repo).bind(root_dir="/", config={}, uninitialized=True)
        inspect.signature(_DVCFileSystem).bind(repo=object(), subrepos=False)
        inspect.signature(SCMContext).bind(object(), {})
        for name in ("tmp_dir", "site_cache_dir", "scm_context"):
            descriptor = inspect.getattr_static(Repo, name)
            if not hasattr(descriptor, "__get__") or callable(descriptor):
                raise TypeError(f"Repo.{name} is no longer a property hook")
        for name in ("ignore", "ignore_remove", "track_file", "add"):
            inspect.signature(getattr(SCMContext, name)).bind(None, "fixture")
        for name in ("close", "repo"):
            inspect.getattr_static(DVCFileSystem, name)
    except (AttributeError, TypeError, ValueError) as exc:
        raise DVCMetadataCompatibilityError(
            f"DVC {installed} lacks the read-only runtime adapter capabilities") from exc


class _RuntimeSCMContext(SCMContext):
    """Retain the real SCM's queries, without runtime Git metadata edits."""

    def ignore(self, path):
        # Output.ignore still performs its original already-tracked check.
        # Ignore registration serves acquisition/maintenance, not a runtime read.
        return None

    def ignore_remove(self, path):
        return None

    def track_file(self, paths=None):
        raise DVCMetadataCompatibilityError("Runtime DVC cannot stage source metadata")

    def add(self, paths):
        raise DVCMetadataCompatibilityError("Runtime DVC cannot modify the Git index")


class RuntimeDVCRepo(Repo):
    """A live-tree Repo with shared private locks and process-owned state.

    Shared locks keep their original cross-process exclusion scope. Only the
    per-instance state/index directory is disposable. No TemporaryDirectory or
    unguarded finalizer can delete an inherited parent's directory after fork.
    """

    def __init__(self, root_dir, *, config=None, uninitialized=False):
        _check_capabilities()
        self._runtime_pid = os.getpid()
        self._runtime_ready = False
        self._runtime_closed = True  # no owned state directory exists yet
        self._runtime_cleanup_failed = False
        root = os.path.abspath(os.fspath(root_dir))
        # Canonicalize only the lock identity; retain DVC's original lexical
        # root for relative paths and a symlinked working-tree override.
        key = hashlib.sha256(os.fsencode(os.path.realpath(root))).hexdigest()
        # Reuse DVC's existing metadata-location override, otherwise standard
        # owner-user cache placement. L1/data_root may legitimately be read-only
        # (GH #1051C); metadata must not require a write there.
        override = os.environ.get("DVC_SITE_CACHE_DIR")
        metadata_root = (Path(override) if override
                         else user_cache_path("lsms_library"))
        self._runtime_namespace = metadata_root / "lsms-runtime" / key
        if self._runtime_namespace.resolve().is_relative_to(Path(root).resolve()):
            raise DVCMetadataCompatibilityError(
                "DVC runtime metadata context must be outside country metadata")
        self._runtime_namespace.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._runtime_state = Path(tempfile.mkdtemp(
            prefix=f"state-{self._runtime_pid}-", dir=self._runtime_namespace))
        self._runtime_closed = False
        # Same L1 override as the existing reader. Other config remains merged
        # by DVC at its original live-tree locations, including config.local.
        effective = {**(config or {})}
        effective["cache"] = {**effective.get("cache", {}),
                              "dir": os.fspath(data_root() / "dvc-cache")}
        try:
            super().__init__(root_dir=root, config=effective,
                             uninitialized=uninitialized)
        except BaseException as cause:
            # State may already own SQLite handles when CacheManager fails.
            # Close only resources actually assigned to this instance; using
            # Repo.close here could initialize a previously unused lazy SCM.
            try:
                self.close()
            except BaseException as cleanup:
                cause.add_note(
                    f"DVC runtime state retained at {self._runtime_state}; "
                    f"construction cleanup failed: {type(cleanup).__name__}")
            raise
        self._runtime_ready = True

    @cached_property
    def tmp_dir(self):
        return os.fspath(self._runtime_namespace / "tmp")

    @cached_property
    def site_cache_dir(self):
        # A new provider must not inherit/reopen a parent's SQLite state.
        # This intentionally uses owned runtime metadata, not global DVC state.
        return os.fspath(self._runtime_state / "site-cache")

    @cached_property
    def scm_context(self):
        return _RuntimeSCMContext(self.scm, self.config)

    def _close_initialized(self):
        errors = []
        seen = set()
        # Preserve installed Repo.close's order without evaluating any lazy
        # property. _scm can hold a supplied/initialized SCM before caching.
        for name in ("scm", "_scm", "state", "dvcfs", "_data_index"):
            resource = self.__dict__.get(name)
            if resource is None or id(resource) in seen:
                continue
            seen.add(id(resource))
            try:
                resource.close()
            except BaseException as error:
                errors.append(error)
        if errors:
            for other in errors[1:]:
                errors[0].add_note(f"Additional DVC close failure: {type(other).__name__}")
            raise errors[0]

    def close(self):
        if (getattr(self, "_runtime_pid", None) != os.getpid()
                or getattr(self, "_runtime_closed", True)):
            return
        try:
            self._close_initialized()
            if (not self._runtime_ready and self.__dict__.get("state") is None
                    and "site_cache_dir" in self.__dict__):
                # DVC's State constructor can itself fail after opening its
                # first cache but before assigning State to Repo. Those inner
                # handles are not ours to discover through lazy properties.
                # Retain the directory rather than delete beneath unknown
                # ownership; the original construction exception is preserved.
                raise DVCMetadataCompatibilityError(
                    "Incomplete DVC State ownership; runtime directory retained")
            shutil.rmtree(self._runtime_state)
        except BaseException:
            # Do not remove files under a live handle or retry invisibly from
            # __del__. An explicit owner close may retry after diagnosis.
            self._runtime_cleanup_failed = True
            raise
        self._runtime_closed = True
        self._runtime_cleanup_failed = False

    def __del__(self):
        # No global registry and no automatic tempfile finalizer. GC may run
        # after fork or interpreter teardown; the owner-PID guard is mandatory.
        try:
            if not getattr(self, "_runtime_cleanup_failed", False):
                self.close()
        except Exception:
            pass


class RuntimeDVCFileSystem(DVCFileSystem):
    """DVC outer wrapper with lazy, per-process supplied-Repo ownership.

    This is not an instance of the public fsspec inner class. The sanctioned
    reader calls delegate to that actual inner API; no new protocol is added.
    """

    def __init__(self, root_dir, *, config=None):
        super().__init__(repo=os.fspath(root_dir), config=config)
        self._runtime_root = os.fspath(root_dir)
        self._runtime_config = config
        self._runtime_pid = os.getpid()
        self._runtime_repo = None
        self._runtime_fs = None
        self._runtime_lock = threading.RLock()
        self._runtime_cleanup_failed = False

    @property
    def fs(self):
        # A fork inherits the Python objects and SQLite handles, not ownership.
        # Drop references without invoking their close methods in the child;
        # RuntimeDVCRepo's own finalizer also guards the original owner PID.
        if self._runtime_pid != os.getpid():
            self._runtime_repo = None
            self._runtime_fs = None
            self._runtime_lock = threading.RLock()
            self._runtime_pid = os.getpid()
            self._runtime_cleanup_failed = False
        with self._runtime_lock:
            if self._runtime_fs is None:
                repo = RuntimeDVCRepo(self._runtime_root,
                                      config=self._runtime_config,
                                      uninitialized=True)
                try:
                    fs = _PublicDVCFileSystem(repo=repo, subrepos=False)
                except BaseException as cause:
                    try:
                        repo.close()
                    except BaseException as cleanup:
                        cause.add_note(
                            f"DVC runtime state retained at {repo._runtime_state}; "
                            f"filesystem cleanup failed: {type(cleanup).__name__}")
                    raise
                self._runtime_repo = repo
                self._runtime_fs = fs
            return self._runtime_fs

    def open(self, path, mode="rb", block_size=None, cache_options=None,
             compression=None, **kwargs):
        # The outer base defaults to text. Preserve public DVC API's binary
        # default and native options for callers that omit mode.
        return self.fs.open(path, mode=mode, block_size=block_size,
                            cache_options=cache_options, compression=compression,
                            **kwargs)

    def exists(self, path, **kwargs):
        # The outer base treats a non-str path as a batch; the existing public
        # API accepts pathlib.Path. Preserve that native interpretation.
        return self.fs.exists(path, **kwargs)

    def close(self):
        if self._runtime_pid != os.getpid():
            return  # never finalize an inherited parent's resources
        with self._runtime_lock:
            try:
                if self._runtime_fs is not None:
                    self._runtime_fs.close()
                if self._runtime_repo is not None:
                    self._runtime_repo.close()
            except BaseException:
                self._runtime_cleanup_failed = True
                raise
            self._runtime_fs = None
            self._runtime_repo = None
            self._runtime_cleanup_failed = False

    def __del__(self):
        try:
            if not getattr(self, "_runtime_cleanup_failed", False):
                self.close()
        except Exception:
            pass
