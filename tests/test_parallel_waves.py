"""Wave-parallel cold builds (GH #797) and the cgroup-aware ``make -j`` (GH #764).

Three things are pinned here, none of which needs S3:

1. the worker-count / jobs rule -- never above the cgroup-visible CPU count,
   honours ``LSMS_BUILD_WORKERS`` and the Slurm variables, and
   ``Country._make_jobs_flag`` no longer reads the physical node;
2. the target-dedup grouping on synthetic Nigeria- and Tanzania-shaped maps;
3. the relay: a worker that files a grain report, files a null-read report,
   emits a warning and returns a frame leaves the PARENT in the same state --
   same warning multiset, same ledger content and order, same frame -- as the
   serial path, and a worker that raises re-raises the ORIGINAL exception type
   in the parent, named by wave.

Plus the cache-hash invariant: nothing in ``_parallel_waves`` is folded into
any build fingerprint (the same pin ``null_read_audit`` has).
"""
from __future__ import annotations

import os
import warnings

import pandas as pd
import pytest

from lsms_library import _build_registry as R
from lsms_library import _parallel_waves as pw
from lsms_library import country as C
from lsms_library import null_read_audit as N


def _affinity() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count() or 1


# ---------------------------------------------------------------------------
# 1. worker count and make -j
# ---------------------------------------------------------------------------

class TestCpuAccounting:
    def test_visible_cpus_never_exceeds_affinity(self):
        assert 1 <= pw.visible_cpus({}) <= _affinity()

    def test_slurm_vars_are_a_ceiling_not_a_floor(self):
        aff = _affinity()
        assert pw.visible_cpus({"SLURM_CPUS_ON_NODE": "1"}) == 1
        assert pw.visible_cpus({"SLURM_CPUS_ON_NODE": str(aff * 100)}) == aff
        # CPUS_PER_TASK wins over CPUS_ON_NODE when both are set
        assert pw.visible_cpus({"SLURM_CPUS_PER_TASK": "1",
                                "SLURM_CPUS_ON_NODE": str(aff)}) == 1
        # garbage is ignored, not fatal
        assert pw.visible_cpus({"SLURM_CPUS_ON_NODE": "lots"}) == pw.visible_cpus({})

    def test_build_workers_default_is_visible_capped_at_targets(self, monkeypatch):
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)   # CPU rule alone
        vis = pw.visible_cpus({})
        assert pw.build_workers(1000, {}) == vis
        assert pw.build_workers(1, {}) == 1
        assert pw.build_workers(0, {}) == 1
        if vis > 1:
            assert pw.build_workers(2, {}) == 2

    def test_build_workers_honours_env_override_within_affinity(self, monkeypatch):
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
        vis = pw.visible_cpus({})
        assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "1"}) == 1
        assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "0"}) == vis      # invalid -> auto
        assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "x"}) == vis      # invalid -> auto
        assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "100000"}) == vis  # capped
        if vis > 1:
            assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "2"}) == 2

    def test_build_workers_default_on_this_host_respects_both_caps(self):
        vis = pw.visible_cpus({})
        cap = pw.memory_worker_cap({})
        got = pw.build_workers(1000, {})
        assert 1 <= got <= vis
        if cap is not None:
            assert got <= cap

    def test_build_workers_honours_slurm(self):
        assert pw.build_workers(50, {"SLURM_CPUS_ON_NODE": "1"}) == 1
        assert pw.build_workers(50, {"SLURM_CPUS_PER_TASK": "1"}) == 1

    def test_build_workers_is_serial_inside_a_worker(self, monkeypatch):
        monkeypatch.setattr(pw, "_IN_WORKER", True)
        assert pw.build_workers(50, {}) == 1

    def test_worker_jobs_times_workers_never_exceeds_visible(self):
        for env in ({}, {"SLURM_CPUS_ON_NODE": "4"}, {"SLURM_CPUS_ON_NODE": "1"}):
            vis = pw.visible_cpus(env)
            for workers in (1, 2, 3, 7, 16, 1000):
                jobs = pw.worker_make_jobs(workers, env)
                assert jobs >= 1
                assert min(workers, vis) * jobs <= max(vis, min(workers, vis)), (env, workers, jobs)
                if workers <= vis:
                    assert workers * jobs <= vis

    def test_explicit_make_jobs_is_a_ceiling(self):
        env = {"LSMS_MAKE_JOBS": "1"}
        assert pw.worker_make_jobs(1, env) == 1
        env = {"LSMS_MAKE_JOBS": "1000000"}
        assert pw.worker_make_jobs(1, env) == pw.visible_cpus(env)  # cannot raise it


class TestMemoryGuard:
    """The default worker count is capped by visible memory / per-worker
    budget so a build that fits serially cannot be turned into an OOM by the
    pool; an explicit LSMS_BUILD_WORKERS may exceed it (warns once), never the
    CPU count."""

    GiB = pw._GiB

    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch):
        monkeypatch.setattr(pw, "_MEMORY_OVERRIDE_WARNED", False)
        monkeypatch.delenv("SLURM_MEM_PER_NODE", raising=False)
        monkeypatch.delenv("SLURM_MEM_PER_CPU", raising=False)

    def _fix(self, monkeypatch, meminfo=None, cgroup=None, physical=None):
        monkeypatch.setattr(pw, "_read_meminfo_available", lambda path="/proc/meminfo": meminfo)
        monkeypatch.setattr(pw, "_read_cgroup_limit", lambda paths=None: cgroup)
        monkeypatch.setattr(pw, "_physical_memory", lambda: physical)

    def test_visible_memory_is_the_smallest_reading(self, monkeypatch):
        self._fix(monkeypatch, meminfo=64 * self.GiB, cgroup=24 * self.GiB)
        assert pw.visible_memory_bytes({}) == 24 * self.GiB
        assert pw.visible_memory_bytes({"SLURM_MEM_PER_NODE": "8192"}) == 8 * self.GiB
        self._fix(monkeypatch, meminfo=64 * self.GiB, cgroup=None)
        assert pw.visible_memory_bytes({}) == 64 * self.GiB

    def test_slurm_mem_per_cpu_scales_with_visible_cpus(self, monkeypatch):
        self._fix(monkeypatch, meminfo=1000 * self.GiB)
        env = {"SLURM_MEM_PER_CPU": "2048", "SLURM_CPUS_ON_NODE": "1"}
        assert pw.visible_memory_bytes(env) == 2 * self.GiB * pw.visible_cpus(env)

    def test_fallbacks(self, monkeypatch):
        self._fix(monkeypatch, meminfo=None, cgroup=None, physical=32 * self.GiB)
        assert pw.visible_memory_bytes({}) == 32 * self.GiB           # /proc/meminfo unreadable
        self._fix(monkeypatch, meminfo=None, cgroup=None, physical=None)
        assert pw.visible_memory_bytes({}) is None                    # nothing readable
        assert pw.memory_worker_cap({}) is None

    def test_cgroup_limit_reader_ignores_max_and_absurd(self, tmp_path):
        files = {}
        for name, content in {"max": "max", "absurd": str(1 << 63), "junk": "lots",
                              "real": str(24 * self.GiB), "tighter": str(8 * self.GiB)}.items():
            f = tmp_path / name
            f.write_text(content + "\n")
            files[name] = str(f)
        assert pw._read_cgroup_limit([files["max"], files["absurd"], files["junk"]]) is None
        assert pw._read_cgroup_limit([files["max"], files["real"]]) == 24 * self.GiB
        assert pw._read_cgroup_limit([files["real"], files["tighter"]]) == 8 * self.GiB
        assert pw._read_cgroup_limit([str(tmp_path / "missing")]) is None

    def test_budget_env(self):
        assert pw.worker_memory_budget({}) == int(pw.DEFAULT_WORKER_MEM_GB * self.GiB)
        assert pw.worker_memory_budget({"LSMS_BUILD_WORKER_MEM_GB": "2.5"}) == int(2.5 * self.GiB)
        for bad in ("0", "-1", "x", ""):
            assert pw.worker_memory_budget({"LSMS_BUILD_WORKER_MEM_GB": bad}) == int(pw.DEFAULT_WORKER_MEM_GB * self.GiB)

    def test_default_worker_count_is_memory_capped(self, monkeypatch):
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 32)
        self._fix(monkeypatch, meminfo=16 * self.GiB)
        assert pw.memory_worker_cap({}) == 4
        assert pw.build_workers(7, {}) == 4
        self._fix(monkeypatch, meminfo=8 * self.GiB)
        assert pw.build_workers(7, {}) == 2
        self._fix(monkeypatch, meminfo=2 * self.GiB)
        assert pw.build_workers(7, {}) == 1                            # never below 1
        self._fix(monkeypatch, meminfo=16 * self.GiB)
        assert pw.build_workers(7, {"LSMS_BUILD_WORKER_MEM_GB": "2"}) == 7   # 8 allowed, 7 targets
        assert pw.build_workers(7, {"SLURM_MEM_PER_NODE": str(6 * 1024)}) == 1   # 6 GiB // 4

    def test_unknown_memory_means_cpu_rule_only(self, monkeypatch):
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 6)
        self._fix(monkeypatch, meminfo=None, cgroup=None, physical=None)
        assert pw.build_workers(7, {}) == 6

    def test_explicit_override_may_exceed_memory_but_not_cpus_and_warns_once(self, monkeypatch):
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 8)
        self._fix(monkeypatch, meminfo=8 * self.GiB)                  # guard says 2
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pw.build_workers(7, {"LSMS_BUILD_WORKERS": "7"}) == 7    # over the guard, asked for
            assert pw.build_workers(7, {"LSMS_BUILD_WORKERS": "20"}) == 7   # still capped by targets
            assert pw.build_workers(50, {"LSMS_BUILD_WORKERS": "20"}) == 8  # ...and by CPUs
        msgs = [m for m in w if issubclass(m.category, RuntimeWarning) and "memory guard" in str(m.message)]
        assert len(msgs) == 1, "the override warning must fire exactly once per process"
        assert "LSMS_BUILD_WORKERS=7" in str(msgs[0].message)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pw.build_workers(7, {"LSMS_BUILD_WORKERS": "2"}) == 2    # within the guard: silent
        assert not [m for m in w if "memory guard" in str(m.message)]


class TestMakeJobsFlagGH764:
    """``_make_jobs_flag`` used ``os.cpu_count() // 2`` -- the physical node.
    A 4-core condo slice on a 56-core node ran every script build ``-j28``."""

    @staticmethod
    def _jobs(flag: str | None) -> int:
        return 1 if flag is None else int(flag[2:])

    def test_default_reads_the_cgroup_not_the_node(self, monkeypatch):
        monkeypatch.delenv("LSMS_MAKE_JOBS", raising=False)
        monkeypatch.delenv("SLURM_CPUS_PER_TASK", raising=False)
        monkeypatch.delenv("SLURM_CPUS_ON_NODE", raising=False)
        monkeypatch.setattr(os, "cpu_count", lambda: 56 * 100)   # the "node"
        assert self._jobs(C._make_jobs_flag()) <= max(1, _affinity() // 2)

    def test_slurm_slice_is_honoured(self, monkeypatch):
        monkeypatch.delenv("LSMS_MAKE_JOBS", raising=False)
        monkeypatch.delenv("SLURM_CPUS_PER_TASK", raising=False)
        monkeypatch.setenv("SLURM_CPUS_ON_NODE", "4")
        assert C._make_jobs_flag() in ("-j2", None)   # None only if affinity < 4
        assert self._jobs(C._make_jobs_flag()) <= 2

    def test_explicit_override_still_wins(self, monkeypatch):
        monkeypatch.setenv("LSMS_MAKE_JOBS", "1")
        assert C._make_jobs_flag() is None
        monkeypatch.setenv("LSMS_MAKE_JOBS", "3")
        assert C._make_jobs_flag() == "-j3"

    def test_run_stage_twin_uses_the_same_rule(self, monkeypatch):
        from lsms_library.util import run_stage
        monkeypatch.delenv("LSMS_MAKE_JOBS", raising=False)
        monkeypatch.delenv("SLURM_CPUS_PER_TASK", raising=False)
        monkeypatch.setenv("SLURM_CPUS_ON_NODE", "2")
        assert run_stage._compute_make_jobs() is None          # 2 // 2 == 1 -> no flag
        monkeypatch.setenv("SLURM_CPUS_ON_NODE", "6")
        expected = max(1, pw.visible_cpus() // 2)
        assert run_stage._compute_make_jobs() == (expected if expected > 1 else None)


# ---------------------------------------------------------------------------
# 2. grouping by resolved target
# ---------------------------------------------------------------------------

class TestGrouping:
    NIGERIA = {"2010Q3": "2010-11", "2011Q1": "2010-11",
               "2012Q3": "2012-13", "2013Q1": "2012-13",
               "2015Q3": "2015-16", "2016Q1": "2015-16"}
    TANZANIA = {"2008-09": "2008-15", "2010-11": "2008-15",
                "2012-13": "2008-15", "2014-15": "2008-15",
                "2019-20": "2019-20", "2020-21": "2020-21"}

    def test_nigeria_shaped_quarter_waves_share_one_group(self):
        waves = ["2010Q3", "2011Q1", "2012Q3", "2013Q1", "2015Q3", "2016Q1"]
        assert pw.group_waves_by_target(waves, self.NIGERIA) == [
            ["2010Q3", "2011Q1"], ["2012Q3", "2013Q1"], ["2015Q3", "2016Q1"]]

    def test_tanzania_shaped_multi_round_folder_is_one_group(self):
        waves = ["2008-09", "2010-11", "2012-13", "2014-15", "2019-20", "2020-21"]
        assert pw.group_waves_by_target(waves, self.TANZANIA) == [
            ["2008-09", "2010-11", "2012-13", "2014-15"], ["2019-20"], ["2020-21"]]

    def test_identity_map_gives_one_group_per_wave_in_order(self):
        waves = ["2005-06", "2009-10", "2010-11"]
        assert pw.group_waves_by_target(waves, {}) == [[w] for w in waves]
        assert pw.group_waves_by_target(waves, None) == [[w] for w in waves]

    def test_group_order_follows_first_appearance_and_flattens_to_wave_order(self):
        waves = ["2019-20", "2008-09", "2020-21", "2010-11"]     # deliberately unsorted
        groups = pw.group_waves_by_target(waves, self.TANZANIA)
        assert groups == [["2019-20"], ["2008-09", "2010-11"], ["2020-21"]]
        assert [w for g in groups for w in g] == ["2019-20", "2008-09", "2010-11", "2020-21"]

    def test_every_wave_lands_in_exactly_one_group(self):
        waves = list(self.NIGERIA)
        groups = pw.group_waves_by_target(waves, self.NIGERIA)
        flat = [w for g in groups for w in g]
        assert sorted(flat) == sorted(waves) and len(flat) == len(set(flat))


# ---------------------------------------------------------------------------
# 3. the relay -- parent-side state identical to the serial path
# ---------------------------------------------------------------------------

_KEY = ("Relayland", "relay_table")
_NULL_KEY = ("Relayland", "?")


def _builder(w: str):
    """A stand-in for ``load_from_waves``'s ``build_wave`` closure that does
    everything a real wave build can do to process-global state."""
    import warnings as _w
    from lsms_library.country import _record_grain_report
    from lsms_library.null_read_audit import _record_null_read_report
    df = pd.DataFrame({"x": [1, 2]}, index=pd.MultiIndex.from_tuples(
        [(w, "h1"), (w, "h2")], names=["t", "i"]))
    _w.warn(f"plain warning from {w}", UserWarning)
    _record_grain_report({
        "country": _KEY[0], "table": _KEY[1], "wave": w,
        "levels": ["t", "i"], "rows": 3, "dropped": 1, "destroyed": 1,
        "conflicting_groups": 1, "nan_key_rows": 0, "missing_levels": [],
    })
    _record_null_read_report({
        "site": "R", "country": _NULL_KEY[0], "table": None, "wave": w,
        "source": f"{w}.dta", "rows": 10, "n_columns": 3, "all_null_columns": ["a"],
        "null_fraction": 1 / 3, "columns_null": ["a"],
    })
    if w == "boom":
        raise C.GrainCollapseError(f"strict failure in {w}")
    return True, df


def _reset_ledgers():
    C._GRAIN_LEDGER.pop(_KEY, None)
    N._NULL_READ_LEDGER.pop(_NULL_KEY, None)


def _packed(msgs):
    return sorted((m.category.__name__, str(m.message), m.filename, m.lineno) for m in msgs
                  if m.category is not ResourceWarning)


@pytest.fixture
def ledgers():
    _reset_ledgers()
    yield
    _reset_ledgers()


needs_two_cpus = pytest.mark.skipif(pw.visible_cpus() < 2,
                                    reason="a parallel build needs >= 2 visible CPUs")


class TestRelay:
    def test_serial_signal_when_workers_is_one(self, monkeypatch):
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
        assert pw.prebuild(_builder, ["a", "b"], {}, country=_KEY[0], table=_KEY[1]) is None

    def test_serial_signal_for_a_single_target(self, monkeypatch):
        monkeypatch.delenv("LSMS_BUILD_WORKERS", raising=False)
        assert pw.prebuild(_builder, ["a", "b"], {"a": "f", "b": "f"},
                           country=_KEY[0], table=_KEY[1]) is None

    @needs_two_cpus
    def test_parent_state_matches_serial(self, monkeypatch, ledgers):
        waves = ["w1", "w2", "w3"]
        # -- serial reference ------------------------------------------------
        with warnings.catch_warnings(record=True) as serial_w:
            warnings.simplefilter("always")
            serial_frames = {w: _builder(w)[1] for w in waves}
        serial_grain = list(C._GRAIN_LEDGER.get(_KEY, []))
        serial_null = list(N._NULL_READ_LEDGER.get(_NULL_KEY, []))
        assert len(serial_grain) == 3 and len(serial_null) == 3
        _reset_ledgers()
        # -- parallel ----------------------------------------------------------
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "2")
        payloads = pw.prebuild(_builder, waves, {}, country=_KEY[0], table=_KEY[1])
        assert payloads is not None and set(payloads) == set(waves)
        # nothing is filed in the parent until relay
        assert _KEY not in C._GRAIN_LEDGER and _NULL_KEY not in N._NULL_READ_LEDGER
        with warnings.catch_warnings(record=True) as par_w:
            warnings.simplefilter("always")
            par_frames = {}
            for w in waves:
                has_table, df = pw.relay(payloads[w], country=_KEY[0], table=_KEY[1])
                assert has_table is True
                par_frames[w] = df
        # -- identical ---------------------------------------------------------
        assert _packed(par_w) == _packed(serial_w)
        assert C._GRAIN_LEDGER[_KEY] == serial_grain          # content AND order
        assert N._NULL_READ_LEDGER[_NULL_KEY] == serial_null
        for w in waves:
            pd.testing.assert_frame_equal(par_frames[w], serial_frames[w], check_exact=True)

    @needs_two_cpus
    def test_relay_is_wave_ordered_not_completion_ordered(self, monkeypatch, ledgers):
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "3")
        waves = ["w3", "w1", "w2"]
        payloads = pw.prebuild(_builder, waves, {}, country=_KEY[0], table=_KEY[1])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for w in waves:
                pw.relay(payloads[w], country=_KEY[0], table=_KEY[1])
        assert [r["wave"] for r in C._GRAIN_LEDGER[_KEY]] == waves

    @needs_two_cpus
    def test_worker_exception_keeps_its_type_and_names_the_wave(self, monkeypatch, ledgers):
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "2")
        payloads = pw.prebuild(_builder, ["ok", "boom"], {}, country=_KEY[0], table=_KEY[1])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pw.relay(payloads["ok"], country=_KEY[0], table=_KEY[1])
            with pytest.raises(C.GrainCollapseError) as info:
                pw.relay(payloads["boom"], country=_KEY[0], table=_KEY[1])
        notes = "\n".join(getattr(info.value, "__notes__", []))
        assert "Relayland/boom/relay_table" in notes
        assert "strict failure in boom" in str(info.value)
        assert "Traceback" in notes                      # the worker's traceback travels

    @needs_two_cpus
    def test_worker_environment_is_pinned(self, monkeypatch, ledgers):
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "2")
        monkeypatch.delenv("LSMS_MAKE_JOBS", raising=False)

        def probe(w):
            return True, {"in_worker": pw._IN_WORKER,
                          "nested_workers": pw.build_workers(50),
                          "env_workers": os.environ.get(pw.WORKERS_ENV),
                          "make_jobs": os.environ.get(pw.MAKE_JOBS_ENV),
                          "omp": os.environ.get("OMP_NUM_THREADS"),
                          "pid": os.getpid()}

        payloads = pw.prebuild(probe, ["a", "b"], {}, country="X", table="y")
        seen = {w: pw.relay(payloads[w], country="X", table="y")[1] for w in ("a", "b")}
        for w, s in seen.items():
            assert s["in_worker"] is True
            assert s["nested_workers"] == 1                 # no nested pools
            assert s["env_workers"] == "1"                  # ...even in a wave script
            assert int(s["make_jobs"]) == pw.worker_make_jobs(2)
            assert s["omp"] == "1"
            assert s["pid"] != os.getpid()
        # parent untouched
        assert pw._IN_WORKER is False
        assert os.environ.get(pw.WORKERS_ENV) == "2"
        assert pw._GROUP_BUILDER is None


# ---------------------------------------------------------------------------
# cache-hash invariants
# ---------------------------------------------------------------------------

def test_grain_ledger_contents_do_not_move_the_build_fingerprint():
    """``_aggregate_wave_data`` names ``_GRAIN_LEDGER``; the constant walk used
    to fold its CONTENTS into every fingerprint, so the hash a build stamped
    depended on when in the process the fingerprint was first memoised.  The
    parallel path computes the parent's first fingerprint AFTER the relayed
    wave-stage reports, which would have stamped an unreproducible hash."""
    key = ("Fingerprintland", "t")
    C._GRAIN_LEDGER.pop(key, None)
    R.clear_caches()
    try:
        before = R.build_transforms_fingerprint("household_roster")
        C._GRAIN_LEDGER[key] = [{"country": key[0], "table": key[1], "wave": "w",
                                 "levels": ["t", "i"], "rows": 2, "dropped": 1,
                                 "destroyed": 1, "conflicting_groups": 1,
                                 "nan_key_rows": 0, "missing_levels": []}]
        R.clear_caches()
        after = R.build_transforms_fingerprint("household_roster")
    finally:
        C._GRAIN_LEDGER.pop(key, None)
        R.clear_caches()
    assert before == after, "a filed grain report moved the build fingerprint"
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split("=")[0] for p in parts
              if p.split("=")[0].endswith(("._GRAIN_LEDGER", "._NULL_READ_LEDGER"))]
    assert not leaked, f"a runtime ledger is folded into the fingerprint: {leaked}"


def test_parallel_waves_is_not_folded_into_any_build_fingerprint():
    """Editing the pool must not cold-rebuild the corpus.  Structural, like
    the ``null_read_audit`` pin: no ``_parallel_waves`` source in any part."""
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    # A part is ``<qualname>=<normalised source>``; the qualname says whose
    # source it is.  (``_aggregate_wave_data``'s own source legitimately
    # MENTIONS ``_parallel_waves.prebuild`` -- that reference is what must
    # not be followed.)
    leaked = [p.split("=")[0] for p in parts
              if p.split("=")[0].startswith("lsms_library._parallel_waves")]
    assert not leaked, (
        "_parallel_waves source leaked into the build fingerprint: "
        f"{leaked}. Add the callable to _build_registry._EXCLUDED_CALLABLES.")
    for name in ("prebuild", "relay", "visible_cpus"):
        assert f"lsms_library._parallel_waves.{name}" in R._EXCLUDED_CALLABLES


class TestDaemonicFallback:
    """A build inside a daemonic process (a ``multiprocessing.Pool`` worker)
    must stay serial: such a process cannot fork, and before this check the
    pool raised ``AssertionError: daemonic processes are not allowed to have
    children`` for every multi-target country (75 of 127 builds in a corpus
    re-warm driven from ``Pool(8)``, 2026-09-07)."""

    def test_daemonic_process_gets_one_worker(self, monkeypatch):
        import multiprocessing
        from lsms_library import _parallel_waves as pw
        monkeypatch.setattr(pw, "_IN_WORKER", False)
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 16)
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
        proc = multiprocessing.current_process()
        monkeypatch.setattr(type(proc), "daemon", property(lambda self: True), raising=False)
        assert pw._in_daemonic_process() is True
        assert pw.build_workers(7, env={}) == 1
        assert pw.prebuild(lambda w: (True, None), ["a", "b", "c"], None,
                           country="X", table="t") is None

    def test_non_daemonic_process_is_unaffected(self, monkeypatch):
        import multiprocessing
        from lsms_library import _parallel_waves as pw
        monkeypatch.setattr(pw, "_IN_WORKER", False)
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 16)
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
        proc = multiprocessing.current_process()
        monkeypatch.setattr(type(proc), "daemon", property(lambda self: False), raising=False)
        assert pw._in_daemonic_process() is False
        assert pw.build_workers(7, env={}) == 7

    def test_real_pool_worker_builds_serially(self):
        """End to end: inside a real ``multiprocessing.Pool`` worker the pool
        declines (returns None) instead of raising."""
        import multiprocessing
        with multiprocessing.get_context("fork").Pool(1) as pool:
            daemon, workers = pool.apply(_daemon_probe, (0,))
        assert daemon is True and workers == 1


def _daemon_probe(_):
    # Module-level so Pool.apply can pickle it.
    from lsms_library import _parallel_waves as pw
    return pw._in_daemonic_process(), pw.build_workers(7, env={})


class TestForkUnsafeFallback:
    """A worker that dies on a fork-unsafe object (fsspec's async loop reached
    through DVC when a blob must be streamed) does not fail the build: the
    wave is rebuilt in the parent.  Reproduced on CI's cold cache,
    2026-09-07 (Mali cluster_features x4)."""

    def test_wave_is_rebuilt_in_the_parent_with_a_warning(self, monkeypatch):
        import warnings
        import pandas as pd
        from lsms_library import _parallel_waves as pw
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 4)
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "2")
        payloads = pw.prebuild(_fork_unsafe_in_worker, ["a", "b"], None, country="X", table="t")
        assert payloads is not None
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # prebuild already warned; relay must now succeed for BOTH waves
            for w in ("a", "b"):
                has, df = pw.relay(payloads[w], country="X", table="t")
                assert has and isinstance(df, pd.DataFrame) and df["wave"].iloc[0] == w
        # the payloads carry no error and were built in the parent
        assert all(p.get("error") is None for p in payloads.values())

    def test_a_different_worker_error_still_raises(self, monkeypatch):
        import pytest
        from lsms_library import _parallel_waves as pw
        monkeypatch.setattr(pw, "visible_cpus", lambda env=None: 4)
        monkeypatch.setattr(pw, "memory_worker_cap", lambda env=None: None)
        monkeypatch.setenv("LSMS_BUILD_WORKERS", "2")
        payloads = pw.prebuild(_value_error_in_worker, ["a", "b"], None, country="X", table="t")
        with pytest.raises(ValueError, match="genuine"):
            pw.relay(payloads["a"], country="X", table="t")


def _fork_unsafe_in_worker(w):
    # Module-level so the fork-context pool can run it.  In a worker, mimic
    # fsspec; in the parent, build normally.
    import os, pandas as pd
    from lsms_library import _parallel_waves as pw
    if pw._IN_WORKER:
        raise RuntimeError("This class is not fork-safe")
    return True, pd.DataFrame({"wave": [w], "pid": [os.getpid()]})


def _value_error_in_worker(w):
    import pandas as pd
    from lsms_library import _parallel_waves as pw
    if pw._IN_WORKER:
        raise ValueError("genuine build failure")
    return True, pd.DataFrame({"wave": [w]})
