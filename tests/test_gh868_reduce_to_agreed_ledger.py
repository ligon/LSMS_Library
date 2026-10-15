"""GH #868 -- ``reduce_to_agreed(on_conflict='na')`` files a grain report, so
the signal survives the cache.

Before this, the sanctioned explicit reducer (``build_transforms.reduce_to_agreed``,
GH #323) disclosed a blanking with ``warnings.warn()`` and nothing else: no
ledger entry, no ``lsms_grain_audit`` stamp, so the warning fired once on the
cold build that ran the reducer and every warm read of the same parquet was
silent -- ``grain_reports()`` empty, the audit harness with nothing to drain.
That is the exact failure #323 fixed for core's own collapse ("the signal must
survive the cache"), one site over.

What is pinned here:

1. the ``'na'`` branch files ONE report of its own kind (``cells_blanked``)
   naming groups and columns, with ``destroyed`` / ``nan_key_rows`` both zero
   -- so a consumer filtering the ledger on rows LOST (Uganda's
   ``test_no_grain_collapse_report`` does exactly that) is untouched;
2. the reducer's own cold-path signal (``GrainConflictWarning``, and
   ``GrainConflict`` under ``LSMS_GRAIN_STRICT``) is unchanged;
3. the report round-trips through a real parquet (JSON: group keys are
   tuples of numpy scalars, which ``json.dumps`` refuses) and REPLAYS on the
   warm read through the one emit choke point;
4. the FRAMEWORK injects ``(country, table, wave)`` around a wave-module
   ``df_edit`` hook, so a bare alias files under the key the cache writer
   stamps; explicit kwargs win; an out-of-process wave script passes them;
5. the wave-level parquet carries the stamp and the L2-wave HIT replays it --
   a country rebuilt from warm wave parquets (the #891 union rebuild) never
   runs the hook, so without this the country stamp would be empty;
6. every Uganda ``crop_production`` wave script stamps its ledger into the
   wave parquet, and the de-dup names its cell (static, credential-free).
"""
from __future__ import annotations

import ast
import pathlib
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import local_tools as lt
from lsms_library.build_transforms import (
    GrainConflict,
    GrainConflictWarning,
    build_context,
    collapse_to_cluster_grain,
    reduce_to_agreed,
)
from lsms_library.country import (
    GrainCollapseError,
    GrainCollapseWarning,
    _GRAIN_LEDGER,
    _replay_grain_audit,
    _replay_wave_stamp,
    _wave_grain_audit,
    grain_reports,
)

REPO = pathlib.Path(__file__).resolve().parents[1]
UGANDA = REPO / "lsms_library" / "countries" / "Uganda"


@pytest.fixture(autouse=True)
def _clean_ledger():
    _GRAIN_LEDGER.clear()
    yield
    _GRAIN_LEDGER.clear()


def _conflicting(regions=("Dakar", "Diourbel", "Dakar", "Thies", "Thies", "Thies")):
    """Six households in two clusters; cluster 01 disagrees on Region."""
    idx = pd.MultiIndex.from_tuples(
        [("2018-19", "01")] * 3 + [("2018-19", "02")] * 3, names=["t", "v"])
    return pd.DataFrame({"Region": list(regions), "Rural": [0, 0, 0, 1, 1, 1]}, index=idx)


def _na_reduce(df, **kw):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = reduce_to_agreed(df, on_conflict="na", **kw)
    return out, caught


# --------------------------------------------------------------------------
# 1 + 2.  The report, and the unchanged cold-path signal
# --------------------------------------------------------------------------

def test_na_branch_files_one_cells_blanked_report_under_the_named_cell():
    out, caught = _na_reduce(_conflicting(), country="Testland", table="cluster_features")

    # what the reducer RETURNS is unchanged (the fix is disclosure)
    assert len(out) == 2
    assert pd.isna(out.loc[("2018-19", "01"), "Region"])
    assert out.loc[("2018-19", "02"), "Region"] == "Thies"

    # the cold-path signal is still the reducer's own class, exactly once
    assert [w.category for w in caught] == [GrainConflictWarning]

    (rep,) = grain_reports("Testland", "cluster_features")
    assert rep["site"] == "reduce_to_agreed"
    assert rep["kind"] == "cells_blanked"
    assert rep["levels"] == ["t", "v"]
    assert rep["conflicting_groups"] == 1 and rep["groups"] == 2
    assert rep["cells_blanked"] == 1
    assert "dropped" not in rep
    assert rep["collapsed_rows"] == rep["rows"] - rep["groups"] == 4
    assert rep["columns"] == {"Region": 1}          # the clean column is not accused
    assert rep["examples"] == ["('2018-19', '01')"]
    assert rep["wave"] == "2018-19"                  # from the frame's single t
    assert rep["country"] == "Testland" and rep["table"] == "cluster_features"


def test_the_report_is_not_a_row_loss_so_a_loss_filter_ignores_it():
    """``tests/test_uganda_323_grain.py::test_no_grain_collapse_report`` keeps
    reports where ``destroyed or nan_key_rows`` is truthy.  A blanking is
    neither: the reducer drops no row (``dropna=False``, one row per tuple).
    Stated as zeros, not omitted, so the filter reads honestly."""
    _na_reduce(_conflicting(), country="Testland", table="cluster_features")
    reports = grain_reports("Testland", "cluster_features")
    assert reports
    assert [r for r in reports if r.get("destroyed") or r.get("nan_key_rows")] == []
    assert reports[0]["destroyed"] == 0 and reports[0]["nan_key_rows"] == 0


def test_agreement_files_nothing():
    reduce_to_agreed(_conflicting(regions=("Dakar",) * 3 + ("Thies",) * 3),
                     on_conflict="na", country="Testland", table="cluster_features")
    assert grain_reports() == []


def test_strict_still_raises_the_reducers_own_error_and_files_nothing(monkeypatch):
    """One lever, one behaviour -- but the cold-path exception type is the
    reducer's (``GrainConflict`` is a ``ValueError``; pinned by
    ``test_gh323_explicit_reducers``).  Nothing is filed: the build aborts, and
    a ledger entry for a parquet that is never written would be re-stamped
    into whatever the same process builds next."""
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    with pytest.raises(GrainConflict, match="LSMS_GRAIN_STRICT"):
        reduce_to_agreed(_conflicting(), on_conflict="na",
                         country="Testland", table="cluster_features")
    assert grain_reports() == []


def test_default_raise_files_nothing():
    with pytest.raises(GrainConflict):
        reduce_to_agreed(_conflicting(), country="Testland", table="cluster_features")
    assert grain_reports() == []


def test_a_report_is_filed_once_per_distinct_finding():
    """Append-unless-identical, like the parallel-wave relay: running the same
    reduction twice in one process is one finding, not two."""
    for _ in range(2):
        _na_reduce(_conflicting(), country="Testland", table="cluster_features")
    assert len(grain_reports("Testland", "cluster_features")) == 1


# --------------------------------------------------------------------------
# 3.  Round trip + replay
# --------------------------------------------------------------------------

def test_report_round_trips_through_a_parquet_and_replays_on_the_warm_read(tmp_path):
    """THE property the issue is about.  The reduced frame is unique, so no
    detector standing at the reducer can fire on a warm read; the only way the
    blanking is still reported is by replaying the stamp."""
    out, _ = _na_reduce(_conflicting(), country="Testland", table="cluster_features")
    audit = grain_reports("Testland", "cluster_features")

    fn = tmp_path / "cluster_features.parquet"
    lt.to_parquet(out, fn, absolute_path=True, cache_hash="deadbeef", grain_audit=audit)
    assert lt.read_parquet_grain_audit(fn) == audit

    _GRAIN_LEDGER.clear()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _replay_wave_stamp(fn, "Testland", "cluster_features", "2018-19")

    (w,) = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    msg = str(w.message)
    assert "Testland/cluster_features/2018-19" in msg
    assert "reduce_to_agreed(on_conflict='na')" in msg
    assert "BLANKED 1 cell" in msg
    assert "Region: 1" in msg and "('2018-19', '01')" in msg
    assert "No row was dropped" in msg
    assert "Do NOT declare a reducer" not in msg, (
        "that advice is for core's collapse; this report was filed BY the "
        "sanctioned reducer")

    (rep,) = grain_reports("Testland", "cluster_features")
    assert rep["from_cache"] is True
    assert rep["kind"] == "cells_blanked"


def test_replay_is_fatal_under_strict_through_the_shared_lever(monkeypatch):
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    stamped = [{"site": "reduce_to_agreed", "kind": "cells_blanked",
                "levels": ["t", "v"], "rows": 6, "groups": 2,
                "conflicting_groups": 1, "cells_blanked": 1,
                "columns": {"Region": 1}, "examples": ["('2018-19', '01')"],
                "collapsed_rows": 4, "destroyed": 0, "nan_key_rows": 0, "wave": "2018-19"}]
    with pytest.raises(GrainCollapseError, match="BLANKED"):
        _replay_grain_audit(stamped, "Testland", "cluster_features")


def test_group_keys_that_json_refuses_still_stamp(tmp_path):
    """Index keys off a MultiIndex are numpy scalars / ``pd.NA``; the stamp is
    ``json.dumps``-ed by ``to_parquet``.  The report must be plain from birth."""
    idx = pd.MultiIndex.from_arrays(
        [np.array([2018, 2018, 2018, 2018]), pd.array([1, 1, pd.NA, pd.NA], dtype="Int64")],
        names=["t", "v"])
    df = pd.DataFrame({"Region": ["a", "b", "c", "d"]}, index=idx)
    out, _ = _na_reduce(df, country="Testland", table="x")
    audit = grain_reports("Testland", "x")
    assert audit[0]["conflicting_groups"] == 2       # the NaN-keyed group counts too
    fn = tmp_path / "x.parquet"
    lt.to_parquet(out, fn, absolute_path=True, grain_audit=audit)   # would TypeError on a tuple key
    assert lt.read_parquet_grain_audit(fn) == audit


def test_wave_is_filled_on_replay_only_where_missing():
    """A wave parquet's stamp may already name its round (Nigeria PP/PH holds
    two per folder, and names a round only where the script passed ``wave=``
    explicitly -- the framework context files the FOLDER name); the parquet's
    wave fills only a report without one."""
    stamped = [{"kind": "cells_blanked", "levels": ["t"], "rows": 2, "groups": 1,
                "conflicting_groups": 1, "cells_blanked": 1, "columns": {"x": 1},
                "examples": ["'a'"], "collapsed_rows": 1, "destroyed": 0, "nan_key_rows": 0},
               {"kind": "cells_blanked", "levels": ["t"], "rows": 2, "groups": 1,
                "conflicting_groups": 1, "cells_blanked": 1, "columns": {"x": 1},
                "examples": ["'b'"], "collapsed_rows": 1, "destroyed": 0, "nan_key_rows": 0,
                "wave": "2010Q3"}]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _replay_grain_audit(stamped, "Nigeria", "t", wave="2010-11")
    waves = sorted(r["wave"] for r in grain_reports("Nigeria", "t"))
    assert waves == ["2010-11", "2010Q3"]


def test_wave_grain_audit_stamps_only_this_waves_in_build_reports():
    """What goes into a WAVE parquet's stamp: this wave's reports from a site
    that fires inside the wave build.  Not another wave's; not a replayed
    (``from_cache``) copy, which the cold rebuild supersedes; and not a Site
    1 / Site 2 report, which is filed AFTER grab_data and re-audited on every
    rebuild -- frozen into a wave parquet it could outlive the loss."""
    mine = {"site": "reduce_to_agreed", "kind": "cells_blanked", "wave": "2018-19", "cells_blanked": 1}
    _GRAIN_LEDGER[("Testland", "cluster_features")] = [
        {**mine, "from_cache": True},
        mine,
        {"site": "reduce_to_agreed", "kind": "cells_blanked", "wave": "2019-20", "cells_blanked": 2},
        {"levels": ["t", "v"], "destroyed": 3, "wave": "2018-19"},                 # Site 1
        {"site": "Wave.cluster_features", "destroyed": 1, "wave": "2018-19"},      # Site 2
    ]
    assert _wave_grain_audit("Testland", "cluster_features", "2018-19") == [mine]
    assert _wave_grain_audit("Testland", "cluster_features", "2005-06") is None
    assert _wave_grain_audit("Testland", "cluster_features", None) is None


# --------------------------------------------------------------------------
# 4.  Who is reducing what: the framework context
# --------------------------------------------------------------------------

def test_framework_context_names_the_cell_for_a_bare_alias():
    hook = collapse_to_cluster_grain                    # the documented df_edit form
    with build_context("Testland", "cluster_features", "2020"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            hook(_conflicting(), on_conflict="na")
    (rep,) = grain_reports("Testland", "cluster_features")
    assert rep["wave"] == "2020"                        # the context, not the frame's t
    assert ("Testland", "cluster_features") in _GRAIN_LEDGER


def test_explicit_kwargs_win_over_the_context():
    with build_context("Testland", "cluster_features", "2020"):
        _na_reduce(_conflicting(), country="Elsewhere", table="crop_production", wave="w")
    assert grain_reports("Testland", "cluster_features") == []
    (rep,) = grain_reports("Elsewhere", "crop_production")
    assert rep["wave"] == "w"


def test_the_context_is_scoped():
    with build_context("Testland", "cluster_features", "2020"):
        pass
    _na_reduce(_conflicting())
    assert grain_reports("Testland") == []
    (rep,) = grain_reports()
    assert rep["country"] is None and rep["table"] is None
    assert rep["wave"] == "2018-19"                     # the frame still names the wave


def test_the_context_does_not_move_any_build_fingerprint():
    """Runtime state, not a build input: the ContextVar is not a constant the
    fingerprint walk folds, and setting it changes no hash."""
    from lsms_library._build_registry import build_transforms_fingerprint
    build_transforms_fingerprint.cache_clear()
    a = build_transforms_fingerprint("cluster_features")
    build_transforms_fingerprint.cache_clear()
    with build_context("X", "cluster_features", "2020"):
        b = build_transforms_fingerprint("cluster_features")
    assert a == b


# --------------------------------------------------------------------------
# 5.  End to end through Wave.grab_data on a synthetic country: the hook runs
#     under the framework's context, the wave parquet carries the stamp, and
#     the L2-wave HIT replays it.
# --------------------------------------------------------------------------

_SCHEME = """\
Waves:
  - '2020'
Data Scheme:
  cluster_features:
    index: (t, v)
    Region: str
"""

_DATA_INFO = """\
cluster_features:
  file: main.csv
  idxvars:
    v: ea
  myvars:
    Region: reg
"""

# A wave-module hook calling the SHIPPED reducer with on_conflict='na' and NO
# cell of its own: the framework has to say which cell this is.
_HOOK = """\
from lsms_library.build_transforms import collapse_to_cluster_grain

def cluster_features(df):
    return collapse_to_cluster_grain(df, on_conflict='na')
"""


@pytest.fixture
def testland(tmp_path, monkeypatch, request):
    from lsms_library.paths import countries_root, data_root

    croot = tmp_path / "countries"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)   # the L2-wave cache must be live
    countries_root.cache_clear()
    data_root.cache_clear()

    c = croot / "Testland"
    (c / "_").mkdir(parents=True)
    (c / "2020" / "_").mkdir(parents=True)
    (c / "2020" / "Data").mkdir(parents=True)
    # cluster c1's two households DISAGREE on Region; c2's agree.
    pd.DataFrame({"hid": ["h1", "h2", "h3", "h4"],
                  "ea": ["c1", "c1", "c2", "c2"],
                  "reg": ["N", "S", "S", "S"]}
                 ).to_csv(c / "2020" / "Data" / "main.csv", index=False)
    (c / "_" / "data_scheme.yml").write_text(_SCHEME)
    (c / "2020" / "_" / "data_info.yml").write_text(_DATA_INFO)
    hook = _HOOK
    if getattr(request, "param", None):
        hook = hook.replace("on_conflict='na'", "on_conflict='na', wave='2020Q3'")
    (c / "2020" / "_" / "mapping.py").write_text(hook)
    try:
        yield C.Country("Testland")["2020"], data_root("Testland") / "2020" / "_" / "cluster_features.parquet"
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def test_grab_data_injects_the_cell_stamps_the_wave_parquet_and_replays_on_the_hit(testland):
    wave, wave_parquet = testland

    # COLD: the hook runs under the framework's context.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = wave.grab_data("cluster_features")
    assert list(df.index.names) == ["t", "v"] and len(df) == 2
    assert pd.isna(df.loc[("2020", "c1"), "Region"]) and df.loc[("2020", "c2"), "Region"] == "S"
    assert any(issubclass(w.category, GrainConflictWarning) for w in caught)
    (rep,) = grain_reports("Testland", "cluster_features")
    assert rep["wave"] == "2020" and rep["kind"] == "cells_blanked"
    assert "from_cache" not in rep

    # ... and the wave parquet carries it.
    assert wave_parquet.exists()
    (stamped,) = lt.read_parquet_grain_audit(wave_parquet)
    assert stamped["kind"] == "cells_blanked" and stamped["wave"] == "2020"
    assert stamped["columns"] == {"Region": 1}

    # WARM: the hook does not run; the stamp is replayed and refiled under the
    # key the L2-country writer stamps.
    _GRAIN_LEDGER.clear()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df2 = wave.grab_data("cluster_features")
    pd.testing.assert_frame_equal(df2, df, check_dtype=False)
    assert not any(issubclass(w.category, GrainConflictWarning) for w in caught), \
        "the hook must not have run on the L2-wave hit"
    (w,) = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert "Testland/cluster_features/2020" in str(w.message)
    (rep,) = grain_reports("Testland", "cluster_features")
    assert rep["from_cache"] is True and rep["kind"] == "cells_blanked"


@pytest.mark.parametrize("testland", ["explicit-round"], indirect=True)
def test_explicit_round_survives_wave_folder_stamp_and_warm_replay(testland):
    """R8: the frame carries folder t=2020, but the hook names round 2020Q3.

    Filtering reports by wave (or by the frame's t) loses this finding.
    A second build folder's report with the same round must stay excluded.
    """
    wave, wave_parquet = testland
    with build_context("Testland", "cluster_features", "another-folder"):
        _na_reduce(_conflicting(), wave="2020Q3")
    with pytest.warns(GrainConflictWarning):
        cold = wave.grab_data("cluster_features")
    assert cold.index.get_level_values("t").unique().tolist() == ["2020"]
    (stamped,) = lt.read_parquet_grain_audit(wave_parquet)
    assert stamped["wave"] == "2020Q3"
    assert stamped["wave_folder"] == "2020"
    assert stamped["columns"] == {"Region": 1}

    _GRAIN_LEDGER.clear()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warm = wave.grab_data("cluster_features")
    pd.testing.assert_frame_equal(warm, cold, check_dtype=False)
    assert not any(issubclass(w.category, GrainConflictWarning) for w in caught)
    (warning,) = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert "Testland/cluster_features/2020Q3" in str(warning.message)
    (report,) = grain_reports("Testland", "cluster_features")
    assert report["from_cache"] is True
    assert report["wave"] == "2020Q3" and report["wave_folder"] == "2020"


# --------------------------------------------------------------------------
# 6.  Uganda's out-of-process path, pinned statically (no credentials needed)
# --------------------------------------------------------------------------

def _calls(tree: ast.AST, name: str):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            fname = f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)
            if fname == name:
                yield node


@pytest.mark.parametrize("script", sorted(UGANDA.glob("2*/_/crop_production.py")),
                         ids=lambda p: p.parts[-3])
def test_every_uganda_wave_script_stamps_its_ledger_into_the_wave_parquet(script):
    """The wave script runs out of process (``make``), so its reducer's report
    exists only in the stamp it writes: ``to_parquet(..., grain_audit=
    grain_reports('Uganda', 'crop_production'))``."""
    tree = ast.parse(script.read_text())
    (call,) = list(_calls(tree, "to_parquet"))
    kw = {k.arg: k.value for k in call.keywords}
    assert "grain_audit" in kw, f"{script}: to_parquet passes no grain_audit="
    src = ast.unparse(kw["grain_audit"])
    assert src == "grain_reports('Uganda', 'crop_production')", src


def test_uganda_de_dup_names_its_cell_for_the_out_of_process_reducer():
    tree = ast.parse((UGANDA / "_" / "uganda.py").read_text())
    calls = [c for c in _calls(tree, "reduce_to_agreed")]
    named = [c for c in calls
             if {k.arg for k in c.keywords} >= {"country", "table", "wave", "on_conflict"}]
    assert len(named) == 1, "crop_production_for_wave's reduce_to_agreed must pass country/table/wave"
    kw = {k.arg: ast.unparse(k.value) for k in named[0].keywords}
    assert kw["country"] == "'Uganda'" and kw["table"] == "'crop_production'" and kw["wave"] == "t"
