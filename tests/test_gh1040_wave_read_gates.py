"""GH #1040: the wave-tier cache READ GATES, branch by branch.

Three gates decide whether an EARLIER build's parquet is served instead of
building:

- **Site 1** -- the YAML-path L2-wave read at the top of ``Wave.grab_data``
  (``{wave}/_/{table}.parquet``): content-hash grade, the GH #675 degraded
  grade, trust-once-and-stamp for a hashless parquet.
- **Site 2** -- the script-path candidate check in ``Wave.grab_data``'s Make
  branch: a non-stale candidate is read without running Make.  (#1040 calls it
  "the ``cache_freshness`` call inside ``run_make_target``"; it is in
  ``grab_data`` -- ``run_make_target`` grades nothing.)
- **Site 3** -- the ``assume_cache_fresh`` serve at the top of
  ``Country._aggregate_wave_data``: hash NOT checked by contract; coverage
  from the GH #891 manifest only.

#1040 moves each out of the ``@build_transform``-tagged closure so that
editing a gate stops moving every table's build fingerprint.  That move must
change NO behaviour, so these tests were written against the pre-extraction
code, run green there, and must stay green after it.  They go through the
public entry points (``Wave.grab_data`` / ``Country._aggregate_wave_data``),
never through a helper's name, so the same file runs on both sides.

Hermetic: every test plants a parquet in a private, empty data root.  The
rebuild descent is made observable with a sentinel raised from the first
build step past the gate -- ``df_data_grabber`` (site 1), the wave-script
runner ``_build_wave_script_target`` (site 2), and the L2-country read gate
``_read_l2_country_cache`` that the ordinary path reaches next (site 3).  No
source data is read and no ``make`` runs.
"""
from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library import local_tools as LT
from lsms_library.paths import data_root


class _Rebuilt(Exception):
    """The gate fell through into the build descent."""


YAML_COUNTRY, YAML_TABLE = "Albania", "household_roster"   # YAML path, no dfs:
SCRIPT_COUNTRY, SCRIPT_WAVE, SCRIPT_TABLE = "Uganda", "2019-20", "food_acquired"
CF_COUNTRY, CF_TABLE = "Albania", "household_roster"


@pytest.fixture
def root(tmp_path, monkeypatch):
    """An empty private data root, and the process state a fresh process has."""
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LSMS_ISSUES_LOG", str(tmp_path / "issues.log"))
    for var in ("LSMS_NO_CACHE", "LSMS_GRAIN_STRICT", "LSMS_BUILD_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    data_root.cache_clear()

    def reset():
        C._DEGRADED_ATTEMPTED.clear()
        C._SOURCE_PROBES.clear()
        C._BUILD_DROP_LEDGER.clear()
        C._GRAIN_LEDGER.clear()
    reset()
    yield tmp_path / "data"
    reset()
    data_root.cache_clear()


def _drop(wave, file="Albania/2002/Data/geo.dta"):
    """A drop record that makes a parquet DEGRADED (GH #675)."""
    return {"wave": wave, "sub": "df_geo", "file": file, "kind": "unavailable",
            "error": "FileNotFoundError: gone", "missing_required": ["Latitude"]}


def _plant(path, frame, **meta):
    path.parent.mkdir(parents=True, exist_ok=True)
    LT.to_parquet(frame, path, absolute_path=True, **meta)
    return path


def _garbage(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"this is not a parquet file")
    return path


def _degraded_warnings(caught):
    return [w for w in caught if issubclass(w.category, C.DegradedCacheWarning)]


# ---------------------------------------------------------------------------
# Site 1 -- the YAML-path L2-wave read in Wave.grab_data
# ---------------------------------------------------------------------------

@pytest.fixture
def yaml_wave(root, monkeypatch):
    c = C.Country(YAML_COUNTRY, preload_panel_ids=False)
    w = c[c.waves[0]]
    info = w.resources.get(YAML_TABLE)
    assert info and not info.get("dfs"), "fixture needs a YAML-path table without dfs:"
    # Prime the memoised fingerprint BEFORE installing the fake: the fake
    # replaces a module global the build closure reaches (see the civ_root
    # fixture in test_gh675_degraded_build.py for the same move).
    expected = w._input_hash(YAML_TABLE)
    assert expected

    def grabber(*_a, **_k):
        raise _Rebuilt
    monkeypatch.setattr(C, "df_data_grabber", grabber)
    assert w._input_hash(YAML_TABLE) == expected, "the fake leaked into the hash"
    path = root / YAML_COUNTRY / w.year / "_" / f"{YAML_TABLE}.parquet"
    frame = pd.DataFrame({"Sex": ["SENTINEL"], "Age": [1.0]},
                         index=pd.MultiIndex.from_tuples(
                             [(w.year, "h1", "p1")], names=["t", "i", "pid"]))
    return w, path, expected, frame


def test_site1_absent_rebuilds(yaml_wave):
    w, path, _, _ = yaml_wave
    assert not path.exists()
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)


def test_site1_fresh_is_served_verbatim(yaml_wave):
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected)
    got = w.grab_data(YAML_TABLE)
    pd.testing.assert_frame_equal(got, pd.read_parquet(path))
    assert list(got["Sex"]) == ["SENTINEL"]
    assert LT.read_parquet_cache_hash(path) == expected


def test_site1_no_cache_bypasses_a_fresh_parquet(yaml_wave, monkeypatch):
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected)
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)


def test_site1_no_cache_is_read_by_truthiness(yaml_wave, monkeypatch):
    """The WAVE gate reads ``LSMS_NO_CACHE`` by truthiness -- ``"0"`` bypasses
    it -- where the COUNTRY gate accepts only ``{1,true,yes}``.  Pinned as
    found, so an extraction cannot "harmonise" it silently; changing it is a
    decision, not a refactor."""
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected)
    monkeypatch.setenv("LSMS_NO_CACHE", "0")
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)
    monkeypatch.setenv("LSMS_NO_CACHE", "")
    assert list(w.grab_data(YAML_TABLE)["Sex"]) == ["SENTINEL"]


def test_site1_stale_rebuilds_and_keeps_its_hash(yaml_wave):
    w, path, _, frame = yaml_wave
    _plant(path, frame, cache_hash="not-the-current-hash")
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)
    assert LT.read_parquet_cache_hash(path) == "not-the-current-hash"


def test_site1_legacy_is_trusted_once_and_stamped(yaml_wave):
    """The wave tier keeps the v0.8.0 trust-once-and-stamp (GH #809 changed
    only the COUNTRY tier)."""
    w, path, expected, frame = yaml_wave
    _plant(path, frame)
    assert LT.read_parquet_cache_hash(path) is None
    got = w.grab_data(YAML_TABLE)
    assert list(got["Sex"]) == ["SENTINEL"]
    assert LT.read_parquet_cache_hash(path) == expected


def test_site1_unverifiable_is_served_and_not_stamped(yaml_wave, monkeypatch):
    w, path, _, frame = yaml_wave
    _plant(path, frame)
    monkeypatch.setattr(C.Wave, "_input_hash", lambda self, table: None)
    assert list(w.grab_data(YAML_TABLE)["Sex"]) == ["SENTINEL"]
    assert LT.read_parquet_cache_hash(path) is None


def test_site1_read_error_rebuilds(yaml_wave):
    """A file that cannot be parsed grades ``legacy`` (no readable hash), fails
    the read, and falls through to the build -- no exception escapes."""
    w, path, _, _ = yaml_wave
    _garbage(path)
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)


def test_site1_degraded_and_healable_rebuilds_once_then_serves(yaml_wave, monkeypatch):
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected, build_drops=[_drop(w.year)])
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)
    assert probes == ["Albania/2002/Data/geo.dta"]
    assert (YAML_COUNTRY, YAML_TABLE, w.year) in C._DEGRADED_ATTEMPTED
    # The retry did not heal it (the fake build raised): serve, never loop.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert list(w.grab_data(YAML_TABLE)["Sex"]) == ["SENTINEL"]
    assert len(_degraded_warnings(caught)) == 1


def test_site1_degraded_and_unhealable_serves_loudly(yaml_wave, monkeypatch):
    w, path, expected, frame = yaml_wave
    _plant(path, frame, cache_hash=expected, build_drops=[_drop(w.year)])
    monkeypatch.setattr(C, "_probe_source", lambda rel: False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        got = w.grab_data(YAML_TABLE)
    assert list(got["Sex"]) == ["SENTINEL"]
    (rec,) = _degraded_warnings(caught)
    msg = str(rec.message)
    assert msg.startswith(f"{YAML_COUNTRY}/{w.year}/{YAML_TABLE}: serving a cached table")
    assert "Latitude" in msg and str(path) in msg
    # The warning names the CALLER of grab_data (stacklevel), not a frame
    # inside the library -- a gate that gains a frame must keep this.
    assert Path(rec.filename).resolve() == Path(__file__).resolve(), rec.filename
    # The drop record is carried forward to the country stamp (#868 replay).
    filed = C._BUILD_DROP_LEDGER[(YAML_COUNTRY, YAML_TABLE)]
    assert filed == [{**_drop(w.year), "from_cache": True}]


def test_site1_stale_and_degraded_grades_the_drop_first(yaml_wave, monkeypatch):
    """The degraded verdict is computed BEFORE the freshness grade, so a stale
    AND healable parquet still spends its one retry for this key.  Pinned so
    a refactor that short-circuits on ``stale`` is seen to change it."""
    w, path, _, frame = yaml_wave
    _plant(path, frame, cache_hash="not-the-current-hash", build_drops=[_drop(w.year)])
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    with pytest.raises(_Rebuilt):
        w.grab_data(YAML_TABLE)
    assert probes and (YAML_COUNTRY, YAML_TABLE, w.year) in C._DEGRADED_ATTEMPTED


def test_site1_hit_replays_the_wave_grain_stamp(yaml_wave):
    """GH #868: a report filed inside the wave build is replayed on a hit."""
    w, path, expected, frame = yaml_wave
    report = {"kind": "cells_blanked", "site": "reduce_to_agreed", "wave": w.year,
              "levels": ["t", "i", "pid"], "columns": {"Sex": 1},
              "examples": ["h1"], "conflicting_groups": 1, "groups": 2,
              "cells_blanked": 1}
    _plant(path, frame, cache_hash=expected, grain_audit=[report])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        w.grab_data(YAML_TABLE)
    (got,) = C.grain_reports(YAML_COUNTRY, YAML_TABLE)
    assert got["from_cache"] is True and got["cells_blanked"] == 1
    assert got["wave"] == w.year


# ---------------------------------------------------------------------------
# Site 2 -- the script-path candidate check in Wave.grab_data
# ---------------------------------------------------------------------------

@pytest.fixture
def script_wave(root, monkeypatch):
    c = C.Country(SCRIPT_COUNTRY, preload_panel_ids=False)
    w = c[SCRIPT_WAVE]
    assert SCRIPT_TABLE in w.data_scheme and not w.resources.get(SCRIPT_TABLE), \
        "fixture needs a script-path wave table"
    expected = w._input_hash(SCRIPT_TABLE)
    assert expected
    makes = []

    def runner(*a, **k):
        makes.append(a)
        raise _Rebuilt
    monkeypatch.setattr(C, "_build_wave_script_target", runner)
    (path,) = w._script_parquet_candidates(SCRIPT_TABLE)
    frame = pd.DataFrame({"Expenditure": [123.0]},
                         index=pd.MultiIndex.from_tuples(
                             [(w.year, "h1", "Matooke", "Kg", "purchased")],
                             names=["t", "i", "j", "u", "s"]))
    return w, path, expected, frame, makes


def test_site2_absent_runs_make(script_wave):
    w, path, _, _, makes = script_wave
    assert not path.exists()
    with pytest.raises(_Rebuilt):
        w.grab_data(SCRIPT_TABLE)
    assert len(makes) == 1


def test_site2_fresh_is_read_without_make(script_wave):
    w, path, expected, frame, makes = script_wave
    _plant(path, frame, cache_hash=expected)
    got = w.grab_data(SCRIPT_TABLE)
    assert list(got["Expenditure"]) == [123.0] and makes == []
    assert LT.read_parquet_cache_hash(path) == expected


def test_site2_stale_runs_make(script_wave):
    w, path, _, frame, makes = script_wave
    _plant(path, frame, cache_hash="not-the-current-hash")
    with pytest.raises(_Rebuilt):
        w.grab_data(SCRIPT_TABLE)
    assert len(makes) == 1


def test_site2_legacy_is_read_and_NOT_stamped(script_wave):
    """Unlike site 1: script-written wave parquets are hashless by design, and
    stamping one we merely read would mask staleness and defeat Make's mtime
    check (v0.8.0 CRITICAL-2)."""
    w, path, _, frame, makes = script_wave
    _plant(path, frame)
    assert list(w.grab_data(SCRIPT_TABLE)["Expenditure"]) == [123.0]
    assert makes == [] and LT.read_parquet_cache_hash(path) is None


def test_site2_unverifiable_is_read_without_make(script_wave, monkeypatch):
    w, path, _, frame, makes = script_wave
    _plant(path, frame, cache_hash="anything")
    monkeypatch.setattr(C.Wave, "_input_hash", lambda self, table: None)
    assert list(w.grab_data(SCRIPT_TABLE)["Expenditure"]) == [123.0]
    assert makes == []


def test_site2_read_error_raises_without_make(script_wave):
    """A file that cannot be parsed grades ``legacy`` and is SELECTED; the read
    that follows raises (it is not wrapped).  Pinned as found -- unlike site 1,
    a corrupt script-path wave parquet does not fall through to a rebuild."""
    w, path, _, _, makes = script_wave
    _garbage(path)
    with pytest.raises(Exception) as info:
        w.grab_data(SCRIPT_TABLE)
    assert not isinstance(info.value, _Rebuilt) and makes == []


def test_site2_does_not_grade_degraded_or_honour_no_cache(script_wave, monkeypatch):
    """Site 2 consults neither the #675 drop record (script parquets carry
    none) nor ``LSMS_NO_CACHE`` (CLAUDE.md: "soft" for script-path waves)."""
    w, path, expected, frame, makes = script_wave
    _plant(path, frame, cache_hash=expected, build_drops=[_drop(w.year)])
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert list(w.grab_data(SCRIPT_TABLE)["Expenditure"]) == [123.0]
    assert makes == [] and probes == [] and not _degraded_warnings(caught)


# ---------------------------------------------------------------------------
# Site 3 -- the assume_cache_fresh serve in Country._aggregate_wave_data
# ---------------------------------------------------------------------------

class _FellThrough(Exception):
    """The short-circuit did not serve; the ordinary read path was entered."""


@pytest.fixture
def acf(root, monkeypatch):
    """``Country(..., assume_cache_fresh=True)`` with the ordinary path stopped
    at its first step and ``_finalize_result`` reduced to the identity (both
    excluded from every fingerprint, so neither fake moves a hash)."""
    finalized = []

    def finalize(self, df, *a, **k):
        finalized.append(df)
        return df

    def ordinary(self, *a, **k):
        raise _FellThrough
    monkeypatch.setattr(C.Country, "_finalize_result", finalize)
    monkeypatch.setattr(C.Country, "_read_l2_country_cache", ordinary)

    def make(assume=True):
        c = C.Country(CF_COUNTRY, preload_panel_ids=False, assume_cache_fresh=assume)
        c._panel_ids_attempted = True
        return c
    c = make()
    path = root / CF_COUNTRY / "var" / f"{CF_TABLE}.parquet"
    frame = pd.DataFrame(
        {"Sex": [f"S{k}" for k in range(len(c.waves))]},
        index=pd.MultiIndex.from_tuples([(t, f"h{k}", "1") for k, t in enumerate(c.waves)],
                                        names=["t", "i", "pid"]))
    return make, path, frame, finalized


def _ts(df):
    return sorted(map(str, df.index.get_level_values("t")))


def test_site3_off_never_short_circuits(acf):
    make, path, frame, _ = acf
    c = make(assume=False)
    _plant(path, frame, cache_hash="x", cache_waves=list(c.waves))
    with pytest.raises(_FellThrough):
        c._aggregate_wave_data(list(c.waves), CF_TABLE)


def test_site3_absent_falls_through(acf):
    make, path, _, _ = acf
    c = make()
    assert not path.exists()
    with pytest.raises(_FellThrough):
        c._aggregate_wave_data(list(c.waves), CF_TABLE)


def test_site3_manifest_covering_a_subset_serves_restricted(acf):
    make, path, frame, finalized = acf
    c = make()
    _plant(path, frame, cache_hash="x", cache_waves=list(c.waves))
    one = c.waves[-1]
    got = c._aggregate_wave_data([one], CF_TABLE)
    assert _ts(got) == [str(one)] and len(finalized) == 1


def test_site3_manifest_covering_all_serves_every_wave(acf):
    make, path, frame, _ = acf
    c = make()
    _plant(path, frame, cache_hash="x", cache_waves=list(c.waves))
    assert _ts(c._aggregate_wave_data(list(c.waves), CF_TABLE)) == sorted(map(str, c.waves))


def test_site3_manifest_not_covering_falls_through(acf):
    make, path, frame, _ = acf
    c = make()
    _plant(path, frame, cache_hash="x", cache_waves=list(c.waves[:-1]))
    with pytest.raises(_FellThrough):
        c._aggregate_wave_data([c.waves[-1]], CF_TABLE)


def test_site3_no_manifest_full_request_serves_whole(acf):
    """Pre-#891 parquet: trusted for a FULL request only, served whole --
    ``_restrict_to_waves`` is not applied, so a row outside ``self.waves``
    survives."""
    make, path, frame, _ = acf
    c = make()
    extra = pd.DataFrame({"Sex": ["X"]}, index=pd.MultiIndex.from_tuples(
        [("1900", "hx", "1")], names=["t", "i", "pid"]))
    _plant(path, pd.concat([frame, extra]), cache_hash="x")
    assert LT.read_parquet_cache_waves(path) is None
    got = c._aggregate_wave_data(list(c.waves), CF_TABLE)
    assert "1900" in _ts(got) and len(got) == len(frame) + 1


def test_site3_no_manifest_subset_request_falls_through(acf):
    make, path, frame, _ = acf
    c = make()
    _plant(path, frame, cache_hash="x")
    with pytest.raises(_FellThrough):
        c._aggregate_wave_data([c.waves[-1]], CF_TABLE)


def test_site3_ignores_the_hash_and_the_degraded_grade(acf, monkeypatch):
    """By contract (CLAUDE.md limitation (c)): a stale, hashless or degraded
    parquet is served -- no probe, no warning."""
    make, path, frame, _ = acf
    c = make()
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    for meta in ({"cache_hash": "stale"}, {},
                 {"cache_hash": "x", "build_drops": [_drop(c.waves[0])]}):
        _plant(path, frame, cache_waves=list(c.waves), **meta)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            got = c._aggregate_wave_data(list(c.waves), CF_TABLE)
        assert len(got) == len(frame) and not _degraded_warnings(caught)
    assert probes == []


def test_site3_read_error(acf):
    """A file that cannot be parsed has no readable manifest.  A subset request
    falls through.  A FULL request is served -- and what is served is an EMPTY
    frame, with no error: ``get_dataframe`` does not raise on a non-parquet
    ``.parquet`` but parses its bytes as a CSV header.  Pinned as found (GH
    #1040 changes no behaviour); it is a defect in its own right, unlike
    site 1 where a read error falls through to a rebuild."""
    make, path, _, finalized = acf
    c = make()
    _garbage(path)
    with pytest.raises(_FellThrough):
        c._aggregate_wave_data([c.waves[-1]], CF_TABLE)
    got = c._aggregate_wave_data(list(c.waves), CF_TABLE)
    assert isinstance(got, pd.DataFrame) and got.empty and len(finalized) == 1


def test_site3_json_methods_never_short_circuit(acf, root):
    make, path, frame, _ = acf
    c = make()
    pq = _plant(root / CF_COUNTRY / "var" / "updated_ids.parquet", frame,
                cache_hash="x", cache_waves=list(c.waves))
    js = root / CF_COUNTRY / "_" / "updated_ids.json"
    js.parent.mkdir(parents=True, exist_ok=True)
    js.write_text('{"2002": {"a": "b"}}')
    assert pq.exists()
    got = c._aggregate_wave_data(list(c.waves), "updated_ids")
    assert got == {"2002": {"a": "b"}}
