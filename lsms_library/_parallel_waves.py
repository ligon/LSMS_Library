"""Wave-parallel cold builds (GH #797).

A cold ``Country(c).<table>()`` walks its waves serially in
``Country._aggregate_wave_data``'s nested ``load_from_waves``: for each wave
it calls ``getattr(wave_obj, table)()`` (the YAML extraction, or -- for a
script-path table -- one ``make`` invocation running one ``python <table>.py``)
and then post-processes the result.  The per-wave *build* stage is expensive
and, by construction, wave-independent: each wave writes its own
``{wave_folder}/_/{table}.parquet`` and reads its own sources.  The *post*
stage (``id_walk`` / ``_augment_index_from_related_tables`` /
``_normalize_dataframe_index`` / concat) is cheap and order-sensitive.  This
module fans the build stage out over a fork-context process pool and hands
each wave's result back to the parent, which post-processes in wave order
exactly as before.

Scope, stated as the things that DO NOT change:

* ``LSMS_BUILD_WORKERS=1`` (or a single build target, or a worker process
  re-entering a build) runs today's serial code path -- ``prebuild`` returns
  ``None`` and ``load_from_waves`` calls its per-wave closure inline.
* Every content-determining line stays inside ``load_from_waves``.  This
  module never touches a DataFrame; it only moves one from a child to the
  parent.  That is why every callable here is in
  ``_build_registry._EXCLUDED_CALLABLES``: editing this module must not move
  a single ``lsms_cache_hash``.
* Warm reads, ``assume_cache_fresh`` and the DVC materialize-stage read path
  never enter ``prebuild``.

Correctness hazards the design addresses (numbered as in the GH #797 brief):

2. *Shared make targets.*  Waves that share a folder (Nigeria's PP/PH quarter
   waves, Tanzania's ``2008-15/`` four-round folder) share ONE make target,
   and two workers running it concurrently would race on the same output
   file.  ``group_waves_by_target`` puts every wave of a folder into one
   group, and a group is built by one worker, in wave order -- the first
   wave triggers ``make``, its siblings find the fresh parquet and read it,
   which is precisely what the serial loop does.
4. *Process-local registries.*  ``grain_reports()`` / ``null_read_reports()``
   are module-level ledgers, and the L2-country parquet embeds the grain
   ledger (``lsms_grain_audit``) at write time.  A worker snapshots both
   ledgers before each wave, returns the entries the build appended, and the
   parent files them again -- in wave order, with the same de-dup rule the
   ledgers use -- immediately before that wave's post stage, so the stamped
   audit is byte-identical to a serial build.  Warnings are captured in the
   worker and re-emitted in the parent with their original category, text,
   filename and line, through the emitting module's ``__warningregistry__``
   so the ``default`` action de-duplicates as it would have in-process.
5. *Oversubscription.*  The worker count is the cgroup-visible CPU count
   (``visible_cpus``: Slurm's allocation when set, else ``sched_getaffinity``),
   capped at the number of distinct targets AND by a memory guard: the
   smallest of MemAvailable, the cgroup memory limit and Slurm's memory
   allocation (``visible_memory_bytes``), divided by a per-worker budget
   (``LSMS_BUILD_WORKER_MEM_GB``, default 4 GiB -- the largest wave script
   measured peaks at 3.7 GB plus 100-350 MB of worker overhead).  Inside a
   worker ``make -j`` is cut to ``visible // workers`` (an explicit
   ``LSMS_MAKE_JOBS`` is a ceiling, never a floor), BLAS/OpenMP pools are
   pinned to one thread, and ``LSMS_BUILD_WORKERS=1`` is exported so a wave
   script that itself builds another table does not open a second pool.
   An explicit ``LSMS_BUILD_WORKERS=N`` may exceed the memory guard (the
   user asked; a RuntimeWarning says so once) but never the CPU count.
7. *Failures.*  A worker returns the ORIGINAL exception (pickled) plus its
   traceback text; the parent re-raises that exception object with a note
   naming ``country/wave/table``, in wave order, so ``GrainCollapseError`` /
   ``NullReadError`` keep their type under ``LSMS_*_STRICT``.  The pool is a
   ``concurrent.futures.ProcessPoolExecutor``, whose ``BrokenProcessPool``
   surfaces a worker killed by the OOM killer instead of hanging.

Why ``fork``: the per-wave builder is a closure over the live ``Country``
(not picklable), and the package is already imported; the child inherits
both.  fsspec / s3fs are fork-aware (pid-keyed instance cache, ``reset_lock``
at-fork hook) and DVC's ``get_remote`` builds a fresh remote filesystem per
call, so a child's blob fetch after the parent has used S3 works -- measured,
see ``slurm_logs/gh797/parallel_waves/REPORT.org``.  ``bench/feature_audit/
scan.py`` uses the same pattern for the same reasons.
"""
from __future__ import annotations

import os
import pickle
import sys
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import get_context
from typing import Any, Callable

WORKERS_ENV = "LSMS_BUILD_WORKERS"
MAKE_JOBS_ENV = "LSMS_MAKE_JOBS"
WORKER_MEM_ENV = "LSMS_BUILD_WORKER_MEM_GB"
# Per-worker memory budget.  Measured on the six GH #797 builds: the largest
# single process is the GhanaLSS 2016-17 food_acquired wave script at 3.7 GB
# (ru_maxrss, identical serial or parallel), and a worker adds 100-350 MB of
# its own (PSS delta / workers).  4 GiB covers the worst case seen with a
# little headroom; every other measured script peaked under 0.9 GB.
DEFAULT_WORKER_MEM_GB = 4.0
_GiB = 1024 ** 3

# Thread-pool pins for a worker.  ``setdefault`` in the worker: the values
# propagate to ``make`` and the wave scripts through ``build_env()`` (an
# ``os.environ.copy()``), which is where a script-path wave does its work.
# For the in-process (YAML-path) case the already-loaded BLAS ignores a late
# env change, so ``threadpoolctl`` is used as well when it is importable.
_THREAD_ENV = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")

# Fork-inherited state.  Set in the parent immediately before the first
# ``submit`` (a fork-context executor launches every worker on that call, from
# the main thread), cleared in ``finally``.  A worker reads it as a global.
_GROUP_BUILDER: Callable[[str], tuple[bool, Any]] | None = None
_IN_WORKER = False


# ---------------------------------------------------------------------------
# CPU accounting (shared with ``Country._make_jobs_flag`` -- GH #764)
# ---------------------------------------------------------------------------

def _env_int(env: dict, key: str) -> int | None:
    raw = env.get(key)
    if raw is None or not str(raw).strip():
        return None
    try:
        value = int(str(raw).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def visible_cpus(env: dict | None = None) -> int:
    """CPUs this process may actually use -- the cgroup, not the node.

    ``os.cpu_count()`` reports the physical node (56 on savio4_htc) even when
    Slurm handed the job a 4-core slice; ``os.sched_getaffinity(0)`` reports
    the cgroup on Linux.  Slurm's own accounting (``SLURM_CPUS_PER_TASK`` when
    ``--cpus-per-task`` was given, else ``SLURM_CPUS_ON_NODE``) is honoured as
    a CEILING on the affinity count -- never as a way to exceed it, because an
    allocation is not a pin.  Always >= 1.
    """
    env = os.environ if env is None else env
    try:
        n = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):      # not Linux
        n = os.cpu_count() or 1
    n = max(1, n)
    slurm = _env_int(env, "SLURM_CPUS_PER_TASK") or _env_int(env, "SLURM_CPUS_ON_NODE")
    if slurm is not None:
        n = min(n, slurm)
    return max(1, n)


def _read_meminfo_available(path: str = "/proc/meminfo") -> int | None:
    """``MemAvailable`` in bytes, or ``None`` when unreadable (not Linux)."""
    try:
        with open(path) as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _physical_memory() -> int | None:
    try:
        return int(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, ValueError, OSError):
        return None


# cgroup v1 reports ~2**63 for "unlimited"; anything this large is not a limit.
_ABSURD_MEMORY_LIMIT = 1 << 60


def _cgroup_limit_paths() -> list[str]:
    """Candidate limit files: the process's own cgroup-v2 node and its
    ancestors (a Slurm step lives in a nested scope, and the limit sits on
    the job node, not the root), then the two root-level spellings."""
    paths: list[str] = []
    try:
        with open("/proc/self/cgroup") as fh:
            for line in fh:
                if line.startswith("0::"):
                    node = line.split(":", 2)[2].strip()
                    while node and node != "/":
                        paths.append(f"/sys/fs/cgroup{node}/memory.max")
                        node = node.rsplit("/", 1)[0]
                    break
    except OSError:
        pass
    paths += ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
    return paths


def _read_cgroup_limit(paths: list[str] | None = None) -> int | None:
    """The tightest readable cgroup memory limit in bytes; ``None`` when none
    is set (``max``), readable, or plausible."""
    limits: list[int] = []
    for path in (paths if paths is not None else _cgroup_limit_paths()):
        try:
            with open(path) as fh:
                raw = fh.read().strip()
        except OSError:
            continue
        if raw == "max":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if 0 < value < _ABSURD_MEMORY_LIMIT:
            limits.append(value)
    return min(limits) if limits else None


def _slurm_memory(env: dict) -> int | None:
    """Slurm's memory allocation in bytes: ``SLURM_MEM_PER_NODE`` (MB), else
    ``SLURM_MEM_PER_CPU`` (MB) times the visible CPU count."""
    per_node = _env_int(env, "SLURM_MEM_PER_NODE")
    if per_node is not None:
        return per_node * 2 ** 20
    per_cpu = _env_int(env, "SLURM_MEM_PER_CPU")
    if per_cpu is not None:
        return per_cpu * 2 ** 20 * visible_cpus(env)
    return None


def visible_memory_bytes(env: dict | None = None) -> int | None:
    """Memory this build may actually use: the smallest of MemAvailable (or
    physical memory when /proc/meminfo is unreadable), the cgroup memory
    limit, and Slurm's allocation.  ``None`` only when nothing is readable."""
    env = os.environ if env is None else env
    readings = [
        _read_meminfo_available() or _physical_memory(),
        _read_cgroup_limit(),
        _slurm_memory(env),
    ]
    found = [r for r in readings if r]
    return min(found) if found else None


def worker_memory_budget(env: dict | None = None) -> int:
    """Per-worker memory budget in bytes (``LSMS_BUILD_WORKER_MEM_GB``, a
    positive number of GiB; default :data:`DEFAULT_WORKER_MEM_GB`)."""
    env = os.environ if env is None else env
    raw = env.get(WORKER_MEM_ENV)
    gb = DEFAULT_WORKER_MEM_GB
    if raw is not None and str(raw).strip():
        try:
            candidate = float(str(raw).strip())
            if candidate > 0:
                gb = candidate
        except ValueError:
            pass
    return max(1, int(gb * _GiB))


def memory_worker_cap(env: dict | None = None) -> int | None:
    """How many workers the memory guard allows (``visible_memory //
    budget``, at least 1), or ``None`` when memory is unmeasurable."""
    env = os.environ if env is None else env
    mem = visible_memory_bytes(env)
    if mem is None:
        return None
    return max(1, mem // worker_memory_budget(env))


_MEMORY_OVERRIDE_WARNED = False


def _warn_memory_override(requested: int, granted: int, cap: int, env: dict) -> None:
    global _MEMORY_OVERRIDE_WARNED
    if _MEMORY_OVERRIDE_WARNED:
        return
    _MEMORY_OVERRIDE_WARNED = True
    mem = visible_memory_bytes(env) or 0
    warnings.warn(
        f"{WORKERS_ENV}={requested} overrides the wave-build memory guard: "
        f"{mem / _GiB:.1f} GiB visible / {worker_memory_budget(env) / _GiB:.1f} GiB per worker "
        f"({WORKER_MEM_ENV}) allows {cap} worker(s); running {granted} as asked.  A worker "
        f"killed by the OOM killer fails the build loudly (BrokenProcessPool).",
        RuntimeWarning, stacklevel=3)


def _in_daemonic_process() -> bool:
    """True inside a daemonic child (a ``multiprocessing.Pool`` worker, or a
    pre-3.9 ``ProcessPoolExecutor`` worker).  Such a process may not fork
    children -- ``multiprocessing`` raises ``AssertionError: daemonic
    processes are not allowed to have children`` -- so a build running there
    stays serial.  Measured 2026-09-07: a corpus re-warm driven from a
    ``Pool(8)`` failed 75 of 127 (country, table) builds on exactly that
    assertion before this check existed.  ``concurrent.futures``' workers
    are non-daemonic on 3.9+, so a build inside one of THOSE still fans
    out (subject to the CPU / memory caps)."""
    try:
        import multiprocessing
        return bool(multiprocessing.current_process().daemon)
    except Exception:  # noqa: BLE001 -- be conservative: no pool if unsure
        return True


def build_workers(n_targets: int, env: dict | None = None) -> int:
    """How many worker processes a cold build with ``n_targets`` distinct
    build targets should use.

    * ``LSMS_BUILD_WORKERS`` unset / not a positive integer -> the visible CPU
      count.
    * ``LSMS_BUILD_WORKERS=1`` -> ``1``: the serial path, unchanged.
    * ``LSMS_BUILD_WORKERS=N`` -> ``N``.

    Whatever the source, the result never exceeds ``visible_cpus()`` (an
    override cannot oversubscribe the cgroup) nor ``n_targets`` (a worker with
    nothing to build is a wasted fork), is at least 1, and is 1 inside a
    worker process (no nested pools) or any daemonic process (which may
    not fork at all -- see ``_in_daemonic_process``).

    The MEMORY guard (:func:`memory_worker_cap`) caps the default at
    ``visible_memory_bytes() // worker_memory_budget()``: a build that fits
    serially must not be turned into an OOM by the pool.  An explicit
    ``LSMS_BUILD_WORKERS=N`` MAY exceed that cap -- the user asked -- and a
    ``RuntimeWarning`` says so once per process.
    """
    if _IN_WORKER or _in_daemonic_process():
        return 1
    env = os.environ if env is None else env
    n_targets = max(0, int(n_targets))
    if n_targets <= 1:
        return 1
    cpu_cap = visible_cpus(env)
    mem_cap = memory_worker_cap(env)
    requested = _env_int(env, WORKERS_ENV)
    if requested is None:
        n = min(cpu_cap, n_targets)
        if mem_cap is not None:
            n = min(n, mem_cap)
        return max(1, n)
    n = max(1, min(requested, cpu_cap, n_targets))
    if mem_cap is not None and n > mem_cap:
        _warn_memory_override(requested, n, mem_cap, env)
    return n


def worker_make_jobs(workers: int, env: dict | None = None) -> int:
    """The ``make -j`` budget of ONE worker when ``workers`` run side by side:
    ``visible_cpus // workers``, so ``workers x jobs <= visible_cpus``.  An
    explicit ``LSMS_MAKE_JOBS`` caps it further (a user asking for ``-j1`` to
    serialise flaky S3 fetches keeps ``-j1``); it never raises it."""
    env = os.environ if env is None else env
    budget = max(1, visible_cpus(env) // max(1, int(workers)))
    explicit = _env_int(env, MAKE_JOBS_ENV)
    if explicit is not None:
        budget = min(budget, explicit)
    return max(1, budget)


# ---------------------------------------------------------------------------
# Target grouping (constraint 2)
# ---------------------------------------------------------------------------

def group_waves_by_target(waves: list[str], wave_folder_map: dict[str, str] | None) -> list[list[str]]:
    """Partition ``waves`` into build groups, one per resolved wave folder.

    Two waves that map to the same folder (``wave_folder_map.get(w, w)``)
    share one on-disk make target -- ``{folder}/_/{table}.parquet`` -- and
    must be built by one worker, serially, in wave order.  Groups are ordered
    by the first appearance of their folder in ``waves``, and a group lists
    its waves in ``waves`` order, so a serial walk over the groups visits the
    waves in exactly the original order.

    >>> group_waves_by_target(['2010Q3', '2011Q1', '2012Q3'],
    ...                       {'2010Q3': '2010-11', '2011Q1': '2010-11'})
    [['2010Q3', '2011Q1'], ['2012Q3']]
    """
    wave_folder_map = wave_folder_map or {}
    groups: dict[str, list[str]] = {}
    for w in waves:
        groups.setdefault(str(wave_folder_map.get(w, w)), []).append(w)
    return list(groups.values())


# ---------------------------------------------------------------------------
# Worker side
# ---------------------------------------------------------------------------

def _ledgers() -> tuple[dict, dict]:
    # Function-level imports: this is a leaf module (country.py imports it),
    # and the ledgers are only needed inside a worker / at relay time.
    from .country import _GRAIN_LEDGER
    from .null_read_audit import _NULL_READ_LEDGER
    return _GRAIN_LEDGER, _NULL_READ_LEDGER


def _snapshot(ledger: dict) -> dict:
    return {k: len(v) for k, v in ledger.items()}


def _delta(ledger: dict, before: dict) -> list[tuple[tuple, dict]]:
    """Entries appended to ``ledger`` since ``before`` (a ``_snapshot``), as
    ``(key, report)`` pairs in filing order."""
    out: list[tuple[tuple, dict]] = []
    for key, reports in ledger.items():
        for report in reports[before.get(key, 0):]:
            out.append((key, report))
    return out


def _pack_warning(msg: warnings.WarningMessage) -> tuple:
    # (category, text, filename, lineno): ``warnings.warn(text, category)``
    # constructs ``category(text)`` itself, so re-emitting from the text
    # reproduces the message object the serial path would have produced.
    return (msg.category, str(msg.message), msg.filename, msg.lineno)


def _pack_exception(exc: BaseException) -> tuple:
    """``(exception_or_None, type_name, text, traceback)``.  The exception
    object travels whole when it pickles (the normal case, including the
    library's own ``GrainCollapseError`` / ``NullReadError``); otherwise the
    parent rebuilds a ``RuntimeError`` from the text."""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    try:
        pickle.dumps(exc)
        carried: BaseException | None = exc
    except Exception:  # noqa: BLE001 -- un-picklable payload; carry the text
        carried = None
    return (carried, type(exc).__name__, str(exc), tb)


def _pin_worker(make_jobs: int) -> None:
    """One-time per-worker environment: BLAS pins, the reduced make -j budget,
    and ``LSMS_BUILD_WORKERS=1`` for any nested build a wave script starts."""
    global _IN_WORKER
    _IN_WORKER = True
    for var in _THREAD_ENV:
        os.environ.setdefault(var, "1")
    os.environ[MAKE_JOBS_ENV] = str(int(make_jobs))
    os.environ[WORKERS_ENV] = "1"
    try:
        import threadpoolctl
        threadpoolctl.threadpool_limits(limits=1)
    except Exception:  # noqa: BLE001 -- optional dependency / no pools loaded
        pass
    try:
        import pyarrow as pa
        pa.set_cpu_count(max(1, int(make_jobs)))
    except Exception:  # noqa: BLE001
        pass


def _build_captured(build_one: Callable[[str], tuple[bool, Any]], wave: str) -> dict:
    """Run one wave's build stage, capturing everything the serial path would
    have left behind in-process: warnings, ledger entries, or an exception."""
    grain, null = _ledgers()
    g0, n0 = _snapshot(grain), _snapshot(null)
    payload: dict[str, Any] = {"wave": wave, "has_table": False, "result": None,
                               "warnings": [], "grain": [], "null": [], "error": None}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            has_table, result = build_one(wave)
            payload["has_table"] = bool(has_table)
            payload["result"] = result
        except Exception as exc:  # noqa: BLE001 -- relayed to the parent whole
            payload["error"] = _pack_exception(exc)
    payload["warnings"] = [_pack_warning(m) for m in caught]
    payload["grain"] = _delta(grain, g0)
    payload["null"] = _delta(null, n0)
    return payload


def _run_group(waves: list[str], make_jobs: int) -> dict[str, dict]:
    """Worker entry point: build every wave of one target group, in order."""
    _pin_worker(make_jobs)
    builder = _GROUP_BUILDER
    if builder is None:
        raise RuntimeError("_parallel_waves._run_group called with no builder installed "
                           "(the executor must use the fork start method)")
    return {w: _build_captured(builder, w) for w in waves}


# ---------------------------------------------------------------------------
# Parent side
# ---------------------------------------------------------------------------

_REGISTRY_FALLBACK: dict[str, dict] = {}
_FILE_TO_MODULE: dict[str, str] = {}


def _module_for(filename: str):
    """The loaded module whose ``__file__`` is ``filename``, or ``None``."""
    name = _FILE_TO_MODULE.get(filename)
    if name is None:
        for mod_name, mod in list(sys.modules.items()):
            if getattr(mod, "__file__", None) == filename:
                _FILE_TO_MODULE[filename] = name = mod_name
                break
    return sys.modules.get(name) if name else None


def _emit_warning(packed: tuple) -> None:
    category, text, filename, lineno = packed
    mod = _module_for(filename)
    if mod is not None:
        registry = mod.__dict__.setdefault("__warningregistry__", {})
        module_name = mod.__name__
    else:
        registry = _REGISTRY_FALLBACK.setdefault(filename, {})
        module_name = None
    warnings.warn_explicit(text, category, filename, lineno,
                           module=module_name, registry=registry)


def _file_reports(ledger: dict, entries: list[tuple[tuple, dict]]) -> None:
    # Same rule as ``_record_grain_report`` / ``_record_null_read_report``:
    # append unless an identical report is already filed.  NOT via those
    # functions -- they also emit, and the warning was already relayed above.
    for key, report in entries:
        existing = ledger.setdefault(tuple(key), [])
        if report not in existing:
            existing.append(report)


def relay(payload: dict, *, country: str, table: str) -> tuple[bool, Any]:
    """Replay one wave's captured side effects in the parent, then return
    ``(wave_has_table, wave_result)`` -- or raise the wave's exception."""
    for packed in payload["warnings"]:
        _emit_warning(packed)
    grain, null = _ledgers()
    _file_reports(grain, payload["grain"])
    _file_reports(null, payload["null"])
    err = payload.get("error")
    if err is not None:
        exc, type_name, text, tb = err
        where = f"{country}/{payload['wave']}/{table}"
        if exc is None:
            exc = RuntimeError(f"{type_name}: {text}")
        note = f"raised while building {where} in a parallel wave worker; worker traceback:\n{tb}"
        if hasattr(exc, "add_note"):        # 3.11+
            exc.add_note(note)
        raise exc
    return payload["has_table"], payload["result"]


_FORK_UNSAFE_MARKERS = ("not fork-safe",)


def _is_fork_unsafe(payload: dict) -> bool:
    """A worker died on an object that refuses to run in a forked child --
    fsspec's ``AsyncFileSystem.loop`` (``RuntimeError: This class is not
    fork-safe``) reached through DVC's filesystem when a blob is missing from
    the local cache and ``get_dataframe`` falls back to streaming it.  The
    parent created that object before forking, so no child can use it; the
    only correct place to build that wave is the parent."""
    err = payload.get("error")
    if not err:
        return False
    exc, type_name, text, tb = err
    return any(m in (text or "") or m in (tb or "") for m in _FORK_UNSAFE_MARKERS)


def prebuild(build_one: Callable[[str], tuple[bool, Any]], waves: list[str],
             wave_folder_map: dict[str, str] | None, *, country: str, table: str) -> dict[str, dict] | None:
    """Run the per-wave build stage of ``waves`` in a fork pool.

    Returns ``{wave: payload}`` for every wave in ``waves`` (feed each to
    :func:`relay`, in wave order), or ``None`` when the build should stay
    serial -- one target, ``LSMS_BUILD_WORKERS=1``, a single visible CPU, or
    a re-entrant call from inside a worker.  ``None`` is the signal for the
    caller to run ``build_one`` inline: that path is byte-for-byte the code
    that ran before this module existed.
    """
    global _GROUP_BUILDER
    groups = group_waves_by_target(list(waves), wave_folder_map)
    workers = build_workers(len(groups))
    if workers <= 1:
        return None
    make_jobs = worker_make_jobs(workers)
    payloads: dict[str, dict] = {}
    _GROUP_BUILDER = build_one
    try:
        with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("fork")) as pool:
            futures = [(group, pool.submit(_run_group, group, make_jobs)) for group in groups]
            for group, fut in futures:
                try:
                    payloads.update(fut.result())
                except BrokenProcessPool as exc:
                    # A worker died without returning (OOM kill, segfault).
                    # Attribute it to every wave of the group so the wave-
                    # ordered relay names the first casualty.
                    for w in group:
                        payloads[w] = {"wave": w, "has_table": False, "result": None,
                                       "warnings": [], "grain": [], "null": [],
                                       "error": (None, type(exc).__name__,
                                                 f"worker process died while building "
                                                 f"{country}/{w}/{table} (group {group}): {exc}",
                                                 "")}
    finally:
        _GROUP_BUILDER = None
    # A wave whose worker hit a fork-unsafe object (see _is_fork_unsafe) is
    # rebuilt HERE, in the parent, through the same capture so relay() sees
    # an ordinary payload.  Measured 2026-09-07 on CI's cold cache: Mali
    # cluster_features' four waves all failed this way in workers because the
    # S3 pull ran inside the child; the parent build succeeds.
    fallback = [w for w in waves if w in payloads and _is_fork_unsafe(payloads[w])]
    if fallback:
        warnings.warn(
            f"{country}/{table}: {len(fallback)} wave(s) rebuilt serially in the "
            f"parent after a parallel worker hit a fork-unsafe filesystem object "
            f"({', '.join(fallback)}); this happens when a source blob is not in "
            f"the local DVC cache yet -- warm the cache once and the build "
            f"parallelises fully.", RuntimeWarning, stacklevel=2)
        for w in fallback:
            payloads[w] = _build_captured(build_one, w)
    return payloads
