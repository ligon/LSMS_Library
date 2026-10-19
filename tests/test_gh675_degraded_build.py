"""GH #675: a build degraded by an UNAVAILABLE source must not cache as fresh.

The content hash covers the inputs a build DECLARED, not what it READ.  The
GH #515 fallback drops a secondary ``dfs:`` sub-df whose file cannot be fetched
(rightly: a partial-data checkout must not hard-fail), and before #675 the
parquet was then written without that sub-df's columns under a valid hash and
served by every later warm read, at both tiers.

Two tiers of test:

- config-only: the stamp round-trips; the verdict is ``complete`` / ``serve`` /
  ``rebuild`` exactly when it should be, retries at most once per key per
  process, and never for a key whose degraded parquet this process wrote; the
  wave-parallel relay carries a drop to the parent; the mechanism stays out of
  every build fingerprint.
- data-gated: the CotedIvoire ``cluster_features`` repro, in a private data
  root, through the real API -- degrade, stay down (no rebuild, a warning),
  heal (one rebuild, both tiers complete), and the wave-tier-only path.
"""
from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import lsms_library
from lsms_library import _build_registry as R
from lsms_library import _parallel_waves as PW
from lsms_library import country as C
from lsms_library import local_tools as LT


@pytest.fixture(autouse=True)
def _fresh_process_state():
    """Each test starts as a fresh process would: no memo, no ledger."""
    def reset():
        C._DEGRADED_ATTEMPTED.clear()
        C._SOURCE_PROBES.clear()
        C._BUILD_DROP_LEDGER.clear()
        C._REBUILD_HISTORY.clear()
    real_reader, real_probe = LT.get_dataframe, LT.source_fetchable_fast
    reset()
    yield
    # a failed data-gated test must not leak its fakes
    LT.get_dataframe, LT.source_fetchable_fast = real_reader, real_probe
    reset()


def _drop(wave="w1", missing=("Latitude",), file="X/w1/Data/geo.dta",
          kind="unavailable", optional=None):
    d = {"wave": wave, "sub": "df_geo", "file": file, "kind": kind,
         "error": "FileNotFoundError: gone", "missing_required": list(missing)}
    if optional is not None:
        d["missing_optional"] = list(optional)
    return d


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

def test_stamp_round_trips_and_absent_means_none(tmp_path):
    df = pd.DataFrame({"a": [1, 2]}, index=pd.Index(["x", "y"], name="v"))
    p = tmp_path / "t.parquet"
    LT.to_parquet(df, p, absolute_path=True, cache_hash="h", build_drops=[_drop()])
    assert LT.read_parquet_build_drops(p) == [_drop()]
    assert LT.read_parquet_cache_hash(p) == "h"
    q = tmp_path / "u.parquet"
    LT.to_parquet(df, q, absolute_path=True, cache_hash="h")
    assert LT.read_parquet_build_drops(q) is None
    pd.testing.assert_frame_equal(pd.read_parquet(p), pd.read_parquet(q))


def test_a_drop_that_cost_no_required_column_is_not_degraded(monkeypatch):
    """Burkina Faso's shape: a config KeyError on an OPTIONAL column.  Recorded,
    never graded -- else every process would probe and warn forever over a
    column the author signed away."""
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    assert C._degraded_cache_verdict([_drop(missing=(), kind="config")], ("X", "t")) \
        == ("complete", None)
    assert C._degraded_cache_verdict(None, ("X", "t")) == ("complete", None)
    assert probes == []


def test_still_unavailable_serves_and_does_not_rebuild(monkeypatch):
    monkeypatch.setattr(C, "_probe_source", lambda rel: False)
    for _ in range(3):
        assert C._degraded_cache_verdict([_drop()], ("X", "t")) == ("serve", None)


def test_readable_again_rebuilds_once_per_process(monkeypatch):
    monkeypatch.setattr(C, "_probe_source", lambda rel: True)
    verdict, reason = C._degraded_cache_verdict([_drop()], ("X", "t"))
    assert verdict == "rebuild"
    assert reason.startswith("degraded:") and "Latitude" in reason and "geo.dta" in reason
    assert not reason.startswith(C._QUIET_REBUILD_REASONS)  # the rebuild is loud
    # The retry did not heal it (flaky source): serve, never loop.
    assert C._degraded_cache_verdict([_drop()], ("X", "t")) == ("serve", None)
    # Keys are independent.
    assert C._degraded_cache_verdict([_drop()], ("X", "t", "w1"))[0] == "rebuild"


def test_a_key_this_process_wrote_degraded_is_not_retried(monkeypatch):
    """The build that just failed to fetch must not immediately rebuild again
    (that would be a GH #920 'repeat')."""
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    C._mark_degraded_written(("X", "t"), [_drop()])
    assert C._degraded_cache_verdict([_drop()], ("X", "t")) == ("serve", None)
    assert probes == []
    C._mark_degraded_written(("X", "u"), [_drop(missing=())])  # not degraded
    assert ("X", "u") not in C._DEGRADED_ATTEMPTED


def test_probe_is_memoised_and_uses_only_the_fast_tiers(monkeypatch):
    """Item 2 of the #675 follow-up: the probe asks the FAST tiers only.
    Through ``get_dataframe`` an unfetchable source fell through to the WB
    download and cost 47-64 s on a warm read; the probe must never reach it."""
    calls = []

    def fast(path):
        calls.append(Path(path))
        return False

    def never(*a, **k):
        raise AssertionError("the probe must not run the full reader chain")
    monkeypatch.setattr(LT, "source_fetchable_fast", fast)
    monkeypatch.setattr(LT, "get_dataframe", never)
    assert C._probe_source("X/w1/Data/geo.dta") is False
    assert C._probe_source("X/w1/Data/geo.dta") is False
    assert len(calls) == 1
    assert calls[0] == lsms_library.paths.countries_root() / "X/w1/Data/geo.dta"


def test_fast_tiers_local_sidecar_and_nowhere(tmp_path, monkeypatch):
    """``source_tier`` / ``source_fetchable_fast`` on real files: an untracked
    local file is fetchable; a sidecar is fetchable exactly when its L1 blob is
    (``_ensure_dvc_pulled`` stubbed -- no remote here); no file and no sidecar
    is not, and nothing (not WB, not DVCFS) is consulted."""
    def never(*a, **k):
        raise AssertionError("no WB / DVCFS fall-through")
    import lsms_library.data_access as DA
    monkeypatch.setattr(DA, "get_data_file", never)
    monkeypatch.setattr(LT, "_try_get_data_file", never, raising=False)
    monkeypatch.setattr(LT, "_ensure_dvc_pulled", lambda p: None)

    local = tmp_path / "local.csv"
    local.write_text("a\n1\n")
    tracked = tmp_path / "tracked.dta"
    md5 = "ab" + "c" * 30
    (tmp_path / "tracked.dta.dvc").write_text(
        f"outs:\n- md5: {md5}\n  path: tracked.dta\n")
    nowhere = tmp_path / "nowhere.dta"

    assert LT.source_tier(local) == "local" and LT.source_fetchable_fast(local)
    assert LT.source_tier(nowhere) is None and not LT.source_fetchable_fast(nowhere)
    assert LT.source_tier(tracked) == "dvc"
    blobs = tmp_path / "blobs"
    monkeypatch.setattr(LT, "_DVC_CACHE_DIR", blobs)
    assert not LT.source_fetchable_fast(tracked)          # blob absent
    (blobs / md5[:2]).mkdir(parents=True)
    (blobs / md5[:2] / md5[2:]).write_bytes(b"x")
    assert LT.source_fetchable_fast(tracked)              # blob present
    # A sidecar wins over a workspace copy, as in get_dataframe.
    tracked.write_text("pollution")
    assert LT.source_tier(tracked) == "dvc"
    # Nothing was written beside the sources (GH #831).
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "blobs", "local.csv", "tracked.dta", "tracked.dta.dvc"]


def test_served_degraded_parquet_warns_with_columns_and_file():
    with pytest.warns(C.DegradedCacheWarning) as rec:
        C._warn_degraded_serve("X", "t", [_drop(missing=("Latitude", "Longitude"))], "/p")
    w = rec[0].message
    assert w.missing == ("Latitude", "Longitude") and w.files == ("X/w1/Data/geo.dta",)
    assert "Latitude" in str(w) and "geo.dta" in str(w)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        C._warn_degraded_serve("X", "t", [_drop(missing=())], "/p")  # silent


def test_wave_records_are_rebuilt_not_accumulated():
    C._file_build_drop("X", "t", _drop(wave="w1"))
    C._replay_build_drops([_drop(wave="w2")], "X", "t")
    C._forget_wave_build_drops("X", "t", "w1")
    assert C._wave_build_drops("X", "t", "w1") is None
    # a replayed (from_cache) record survives, and reaches the country stamp clean
    assert C._stampable_build_drops("X", "t") == [_drop(wave="w2")]


def test_parallel_relay_carries_a_drop_to_the_parent():
    """Lost at the fork, a degraded country parquet would grade fresh again."""
    def build_one(w):
        C._file_build_drop("X", "t", _drop(wave=w))
        return True, None
    payload = PW._build_captured(build_one, "w1")
    C._BUILD_DROP_LEDGER.clear()            # the parent never saw the filing
    PW.relay(payload, country="X", table="t")
    assert C.build_drop_reports("X", "t") == [_drop(wave="w1")]


_MECHANISM = [
    "lsms_library.local_tools.read_parquet_build_drops",
    "lsms_library.local_tools.source_tier",
    "lsms_library.local_tools.source_fetchable_fast",
    "lsms_library.country._drop_record",
    "lsms_library.country._file_wave_drops",
    "lsms_library.country._optional_scheme_columns",
    "lsms_library.country._drop_missing",
    "lsms_library.country._describe_missing",
    "lsms_library.country._file_build_drop",
    "lsms_library.country._forget_wave_build_drops",
    "lsms_library.country._wave_build_drops",
    "lsms_library.country._stampable_build_drops",
    "lsms_library.country._degraded_drops",
    "lsms_library.country._mark_degraded_written",
    "lsms_library.country._probe_source",
    "lsms_library.country._degraded_cache_verdict",
    "lsms_library.country._warn_degraded_serve",
    "lsms_library.country._replay_build_drops",
]


def test_mechanism_is_excluded_from_every_build_fingerprint():
    for qn in _MECHANISM:
        assert qn in R._EXCLUDED_CALLABLES, qn
    for name in ("_BUILD_DROP_LEDGER", "_DEGRADED_ATTEMPTED", "_SOURCE_PROBES"):
        assert name in R._EXCLUDED_CONSTANTS, name


def test_editing_the_mechanism_moves_no_hash(monkeypatch):
    """Measured, as #908's pin is: a no-op edit to the verdict, the filer,
    the probe and its fast-tier seam, and the build-time drop classifier moves
    no fingerprint and no CotedIvoire / Uganda table hash."""
    import inspect

    def probe():
        R.clear_caches()
        btf = {t: R.build_transforms_fingerprint(t)
               for t in (None, "cluster_features", "household_roster", "sample")}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tch = {}
            for c in ("CotedIvoire", "Uganda"):
                co = C.Country(c, preload_panel_ids=False)
                for t in ("cluster_features", "household_roster"):
                    tch[(c, t)] = co._table_cache_hash(t, co.waves)
        return btf, tch

    before = probe()
    real = inspect.getsource
    targets = {C._degraded_cache_verdict, C._file_build_drop, C._probe_source,
               C._drop_record, C._file_wave_drops, LT.source_fetchable_fast,
               LT.source_tier}
    monkeypatch.setattr(inspect, "getsource",
                        lambda o: real(o) + "\n_probe = 1\n"
                        if inspect.unwrap(o) in targets else real(o))
    after = probe()
    monkeypatch.undo()
    R.clear_caches()
    assert before == after


# ---------------------------------------------------------------------------
# data-gated: the CotedIvoire cluster_features repro, through the real API
# ---------------------------------------------------------------------------

GEO = "grappe_gps_CIV2018.dta"
GEO_REL = f"CotedIvoire/2018-19/Data/Menage/{GEO}"


@pytest.fixture
def civ_root(tmp_path, monkeypatch):
    """A private, empty data root sharing the real L1 blob store (read-only)."""
    real_root = lsms_library.paths.data_root()
    blobs = real_root / "dvc-cache"
    if not blobs.exists():
        pytest.skip("no L1 blob store")
    if not C._probe_source(GEO_REL):
        pytest.skip(f"{GEO} is not readable here")
    C._SOURCE_PROBES.clear()
    (tmp_path / "dvc-cache").symlink_to(blobs)
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    # Restoring an env var does not clear its cached paths (see the #866 tests).
    lsms_library.paths.data_root.cache_clear()
    # Prime the memoised fingerprints BEFORE simulating the failure: the fake
    # is a local_tools global the build closure reaches, so installing it first
    # would fold its source into the hash and look like an input change.
    co = C.Country("CotedIvoire", preload_panel_ids=False)
    for t in ("cluster_features", "sample"):
        co._table_cache_hash(t, co.waves)
    yield tmp_path
    lsms_library.paths.data_root.cache_clear()


class _GeoUnavailable:
    """Context: the geo file cannot be read (build AND probe both see it)."""

    def __enter__(self):
        self.real = real = LT.get_dataframe
        self.real_fast = real_fast = LT.source_fetchable_fast

        def fake(fn, *a, **k):
            if GEO in str(fn):
                raise FileNotFoundError(f"simulated fetch failure: {fn}")
            return real(fn, *a, **k)

        def fake_fast(path):
            return False if GEO in str(path) else real_fast(path)
        LT.get_dataframe = fake
        LT.source_fetchable_fast = fake_fast
        return self

    def __exit__(self, *exc):
        LT.get_dataframe = self.real
        LT.source_fetchable_fast = self.real_fast


def _read(root):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        df = C.Country("CotedIvoire", preload_panel_ids=False).cluster_features()
    kinds = [m.message for m in w]
    return (df,
            [m for m in kinds if isinstance(m, C.DegradedCacheWarning)],
            [m for m in kinds if isinstance(m, C.CacheRebuildWarning)])


def _stamps(root):
    return {tier: LT.read_parquet_build_drops(p) for tier, p in (
        ("country", root / "CotedIvoire" / "var" / "cluster_features.parquet"),
        ("wave", root / "CotedIvoire" / "2018-19" / "_" / "cluster_features.parquet"))}


def _new_process():
    C._DEGRADED_ATTEMPTED.clear()
    C._SOURCE_PROBES.clear()
    C._BUILD_DROP_LEDGER.clear()
    C._REBUILD_HISTORY.clear()


def test_civ_degrade_stay_down_then_heal(civ_root, monkeypatch):
    # 1. the degraded build: stamped at BOTH tiers (it used to be silent)
    down = _GeoUnavailable().__enter__()
    df, degraded, rebuilt = _read(civ_root)
    assert "Latitude" not in df.columns
    stamps = _stamps(civ_root)
    for tier in ("country", "wave"):
        assert stamps[tier] and stamps[tier][0]["file"] == GEO_REL, tier
        assert stamps[tier][0]["missing_required"] == ["Latitude", "Longitude"]
    # same process, second read: served, warned, NOT rebuilt (no #920 repeat)
    df, degraded, rebuilt = _read(civ_root)
    assert degraded and not rebuilt

    # 2. a new process, source still down: warned, never rebuilt
    _new_process()
    for _ in range(2):
        df, degraded, rebuilt = _read(civ_root)
        assert "Latitude" not in df.columns
        assert len(degraded) == 1 and not rebuilt
        assert degraded[0].missing == ("Latitude", "Longitude")
        assert degraded[0].files == (GEO_REL,)

    # 3. a new process, source back: ONE loud rebuild, both tiers complete
    down.__exit__()
    _new_process()
    df, degraded, rebuilt = _read(civ_root)
    assert {"Latitude", "Longitude"} <= set(df.columns)
    assert df["Latitude"].notna().any()
    assert not degraded
    assert len(rebuilt) == 1 and rebuilt[0].reason.startswith("degraded:")
    assert _stamps(civ_root) == {"country": None, "wave": None}
    df, degraded, rebuilt = _read(civ_root)
    assert not degraded and not rebuilt


def test_civ_degraded_wave_parquet_reaches_a_rebuilt_country_parquet(civ_root, monkeypatch):
    """The L2-wave hole: with the country parquet gone, the rebuild used to
    re-serve the degraded wave parquet under a fresh country hash."""
    down = _GeoUnavailable().__enter__()
    _read(civ_root)
    (civ_root / "CotedIvoire" / "var" / "cluster_features.parquet").unlink()
    _new_process()
    df, degraded, rebuilt = _read(civ_root)
    assert "Latitude" not in df.columns
    assert degraded and degraded[0].missing == ("Latitude", "Longitude")
    assert _stamps(civ_root)["country"][0]["file"] == GEO_REL

    down.__exit__()
    (civ_root / "CotedIvoire" / "var" / "cluster_features.parquet").unlink()
    _new_process()
    df, degraded, rebuilt = _read(civ_root)
    assert df["Latitude"].notna().any() and not degraded
    assert _stamps(civ_root) == {"country": None, "wave": None}


# ---------------------------------------------------------------------------
# 675-b (@ligon 2026-10-18): an OPTIONAL column lost to an UNAVAILABLE source
# is graded; a CONFIG drop never is.
# ---------------------------------------------------------------------------

def test_an_unavailable_drop_of_an_optional_column_is_graded(monkeypatch):
    monkeypatch.setattr(C, "_probe_source", lambda rel: False)
    d = _drop(missing=(), optional=("Latitude",))
    assert C._degraded_cache_verdict([d], ("X", "t")) == ("serve", None)
    with pytest.warns(C.DegradedCacheWarning) as rec:
        C._warn_degraded_serve("X", "t", [d], "/p")
    w = rec[0].message
    assert w.missing == () and w.missing_optional == ("Latitude",)
    assert "optional column(s) ['Latitude']" in str(w)

    C._SOURCE_PROBES.clear()
    monkeypatch.setattr(C, "_probe_source", lambda rel: True)
    verdict, reason = C._degraded_cache_verdict([d], ("X", "u"))
    assert verdict == "rebuild" and "optional column(s) ['Latitude']" in reason


@pytest.mark.parametrize("missing,optional", [((), ("Latitude",)),
                                              (("Latitude",), ())])
def test_a_config_drop_is_never_graded(monkeypatch, missing, optional):
    """A config drop re-drops identically on every rebuild: a probe would find
    the file readable, rebuild, drop again and trip the #920 repeat detector
    every process (Burkina Faso, #1050).  Recorded, never graded -- whichever
    kind of column it cost (a required one cannot reach a stamp: grab_data
    raises first; see the ordering test below)."""
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or True)
    d = _drop(missing=missing, optional=optional, kind="config")
    assert C._degraded_cache_verdict([d], ("X", "t")) == ("complete", None)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        C._warn_degraded_serve("X", "t", [d], "/p")
    assert probes == []


def test_drop_record_classifies_by_cause(tmp_path):
    tracked = tmp_path / "geo.dta"
    (tmp_path / "geo.dta.dvc").write_text("outs:\n- md5: 0\n")
    local = tmp_path / "local.csv"
    local.write_text("a\n")
    typo = tmp_path / "sub" / ".." / "goe.dta"          # `..` spellings are live
    fnf = FileNotFoundError("gone")

    r = C._drop_record("w", "df_geo", KeyError("lat"), [tracked], {"Latitude": "lat"})
    assert (r["kind"], r["cause"]) == ("config", "missing-column")
    r = C._drop_record("w", "df_geo", fnf, [tracked], {"Latitude": "lat"})
    assert (r["kind"], r["cause"]) == ("unavailable", None)
    assert r["supplies"] == ["Latitude"]
    r = C._drop_record("w", "df_geo", fnf, [local], {})
    assert r["kind"] == "unavailable"
    r = C._drop_record("w", "df_geo", fnf, [typo], {})
    assert (r["kind"], r["cause"]) == ("config", "no-such-source")
    assert r["file"] == str(tmp_path / "goe.dta")          # normalised


def test_file_wave_drops_counts_declared_columns_only():
    scheme = {"index": "(t, v)", "Region": "str", "Latitude": "float",
              "Elevation": {"type": "float", "optional": True}}
    df = pd.DataFrame({"Region": ["N"]}, index=pd.MultiIndex.from_tuples(
        [("2020", "c1")], names=["t", "v"]))
    rec = {"wave": "2020", "sub": "df_geo", "file": "f", "kind": "unavailable",
           "cause": None, "error": "e",
           "supplies": ["Latitude", "Elevation", "_tmp", "Region"]}
    C._file_wave_drops("X", "t", [rec], df, scheme)
    (got,) = C.build_drop_reports("X", "t")
    assert got["missing_required"] == ["Latitude"]        # Region is present
    assert got["missing_optional"] == ["Elevation"]       # _tmp is undeclared
    assert "supplies" not in got


# ---------------------------------------------------------------------------
# Through Wave.grab_data, on a synthetic country: item 3 (a `file:` naming no
# source is a config bug) and item 5 (a config drop that costs a REQUIRED
# column raises before anything is written).
# ---------------------------------------------------------------------------

_SCHEME_REQ = """\
Waves:
  - '2020'
Data Scheme:
  cluster_features:
    index: (t, v)
    Region: str
    Latitude: float
"""

_SCHEME_OPT = _SCHEME_REQ.replace(
    "    Latitude: float\n",
    "    Latitude:\n      type: float\n      optional: true\n")


def _info(geo_file="geo.csv", lat_col="lat"):
    return f"""\
cluster_features:
  dfs:
    - df_main
    - df_geo
  merge_on:
    - v
  final_index:
    - t
    - v
  df_main:
    file: main.csv
    idxvars:
      v: ea
    myvars:
      Region: reg
  df_geo:
    file: {geo_file}
    idxvars:
      v: ea
    myvars:
      Latitude: {lat_col}
"""


@pytest.fixture
def synth(tmp_path, monkeypatch):
    from lsms_library.paths import countries_root, data_root

    croot, droot = tmp_path / "countries", tmp_path / "data"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(droot))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    # The synthetic country's raw files sit in the config tree with no .dvc
    # sidecar -- new data awaiting `dvc add`, which GH #831 refuses unless
    # the caller opts in.  This fixture IS that case.
    monkeypatch.setenv("LSMS_ALLOW_INTREE_SOURCES", "1")
    countries_root.cache_clear()
    data_root.cache_clear()

    def build(scheme, data_info):
        c = croot / "Synthland"
        for d in (c / "_", c / "2020" / "_", c / "2020" / "Data"):
            d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"ea": ["c1", "c2"], "reg": ["N", "S"]}).to_csv(
            c / "2020" / "Data" / "main.csv", index=False)
        pd.DataFrame({"ea": ["c1", "c2"], "lat": [1.0, 2.0]}).to_csv(
            c / "2020" / "Data" / "geo.csv", index=False)
        (c / "_" / "data_scheme.yml").write_text(scheme)
        (c / "2020" / "_" / "data_info.yml").write_text(data_info)
        return C.Country("Synthland", preload_panel_ids=False)

    try:
        yield build, droot
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def _parquets(root):
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*.parquet")) \
        if root.exists() else []


def test_a_file_path_naming_no_source_is_a_config_drop(synth, monkeypatch):
    """Verdict risk 4: a typo'd `file:` used to be classed `unavailable` --
    probed every process (through the WB fall-through) and served with "will
    be rebuilt automatically".  It is a config bug: recorded, ungraded, and
    the build-time warning says to check the path."""
    build, droot = synth
    probes = []
    monkeypatch.setattr(C, "_probe_source", lambda rel: probes.append(rel) or False)
    # The reader would otherwise try the WB download for a path with no sidecar.
    real = LT.get_dataframe

    def fnf(fn, *a, **k):
        if "gps_typo.csv" in str(fn):
            raise FileNotFoundError(fn)
        return real(fn, *a, **k)
    monkeypatch.setattr(LT, "get_dataframe", fnf)
    wave = build(_SCHEME_OPT, _info(geo_file="gps_typo.csv"))["2020"]
    with pytest.warns(UserWarning, match=r"no such source.*check the file: path"):
        df = wave.grab_data("cluster_features")
    assert "Latitude" not in df.columns
    (rec,) = C.build_drop_reports("Synthland", "cluster_features")
    assert (rec["kind"], rec["cause"]) == ("config", "no-such-source")
    assert rec["file"] == "Synthland/2020/Data/gps_typo.csv"
    assert rec["missing_optional"] == ["Latitude"]
    # stamped into the wave parquet, and graded complete there: no probe, no warning
    stamp = LT.read_parquet_build_drops(
        droot / "Synthland" / "2020" / "_" / "cluster_features.parquet")
    assert stamp and stamp[0]["cause"] == "no-such-source"
    assert C._degraded_cache_verdict(stamp, ("Synthland", "cluster_features", "2020")) \
        == ("complete", None)
    assert probes == []


def test_a_file_path_naming_no_source_costing_a_required_column_raises(synth, monkeypatch):
    build, droot = synth
    real = LT.get_dataframe
    monkeypatch.setattr(LT, "get_dataframe", lambda fn, *a, **k: (
        (_ for _ in ()).throw(FileNotFoundError(fn)) if "gps_typo.csv" in str(fn)
        else real(fn, *a, **k)))
    wave = build(_SCHEME_REQ, _info(geo_file="gps_typo.csv"))["2020"]
    with pytest.raises(RuntimeError, match=r"no such source.*check the file: path"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            wave.grab_data("cluster_features")
    assert _parquets(droot) == []


def test_a_config_drop_costing_a_required_column_raises_and_writes_nothing(synth):
    """Verdict optional item / follow-up item 5: the ordering the design rests
    on.  A KeyError drop that costs a REQUIRED column raises in grab_data
    BEFORE any drop is filed or any parquet written -- so a required-costing
    config drop can never reach a stamp, and "config drops are never graded"
    cannot silently serve a table without a required column."""
    build, droot = synth
    co = build(_SCHEME_REQ, _info(lat_col="LAT_NOT_HERE"))
    with pytest.raises(RuntimeError, match=r"required declared column\(s\) \['Latitude'\]"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            co["2020"].grab_data("cluster_features")
    assert _parquets(droot) == []
    assert C.build_drop_reports("Synthland") == []
    # ... and through the Country API, which owns the L2-country write
    with pytest.raises(RuntimeError, match=r"required declared column"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            build(_SCHEME_REQ, _info(lat_col="LAT_NOT_HERE")).cluster_features()
    assert _parquets(droot) == []
    assert C.build_drop_reports("Synthland") == []


def test_an_unavailable_optional_drop_is_stamped_and_graded(synth, monkeypatch):
    """675-b end to end: a tracked (sidecar) source that cannot be fetched,
    supplying only an OPTIONAL column, now degrades the parquet like a
    required one -- served with a warning while down, rebuilt once healed."""
    build, droot = synth
    co = build(_SCHEME_OPT, _info(geo_file="geo_tracked.csv"))
    data = co["2020"].file_path / "Data"
    (data / "geo_tracked.csv.dvc").write_text("outs:\n- md5: 0\n")
    real = LT.get_dataframe
    monkeypatch.setattr(LT, "get_dataframe", lambda fn, *a, **k: (
        (_ for _ in ()).throw(FileNotFoundError(fn)) if "geo_tracked.csv" in str(fn)
        else real(fn, *a, **k)))
    monkeypatch.setattr(LT, "source_fetchable_fast", lambda p: False)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = co["2020"].grab_data("cluster_features")
    assert "Latitude" not in df.columns
    wave_pq = droot / "Synthland" / "2020" / "_" / "cluster_features.parquet"
    (rec,) = LT.read_parquet_build_drops(wave_pq)
    assert (rec["kind"], rec["missing_required"], rec["missing_optional"]) == \
        ("unavailable", [], ["Latitude"])
    # a new process, still down: served, warned, not rebuilt
    _new_process()
    with pytest.warns(C.DegradedCacheWarning, match="optional column"):
        C.Country("Synthland", preload_panel_ids=False)["2020"].grab_data(
            "cluster_features")
    # a new process, source back: graded a miss
    _new_process()
    monkeypatch.setattr(LT, "source_fetchable_fast", lambda p: True)
    verdict, reason = C._degraded_cache_verdict(
        LT.read_parquet_build_drops(wave_pq), ("Synthland", "cluster_features", "2020"))
    assert verdict == "rebuild" and "optional column(s) ['Latitude']" in reason
