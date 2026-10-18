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
    real_reader = LT.get_dataframe
    reset()
    yield
    LT.get_dataframe = real_reader  # a failed data-gated test must not leak its fake
    reset()


def _drop(wave="w1", missing=("Latitude",), file="X/w1/Data/geo.dta",
          kind="unavailable"):
    return {"wave": wave, "sub": "df_geo", "file": file, "kind": kind,
            "error": "FileNotFoundError: gone", "missing_required": list(missing)}


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


def test_probe_is_memoised_and_uses_the_real_reader(monkeypatch):
    calls = []

    def fake(path, *a, **k):
        calls.append(Path(path))
        raise FileNotFoundError(path)
    monkeypatch.setattr(LT, "get_dataframe", fake)
    assert C._probe_source("X/w1/Data/geo.dta") is False
    assert C._probe_source("X/w1/Data/geo.dta") is False
    assert len(calls) == 1
    assert calls[0] == lsms_library.paths.countries_root() / "X/w1/Data/geo.dta"


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
    """Measured, as #908's pin is: a no-op edit to the verdict and to the
    filer moves no fingerprint and no CotedIvoire / Uganda table hash."""
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
    targets = {C._degraded_cache_verdict, C._file_build_drop}
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

        def fake(fn, *a, **k):
            if GEO in str(fn):
                raise FileNotFoundError(f"simulated fetch failure: {fn}")
            return real(fn, *a, **k)
        LT.get_dataframe = fake
        return self

    def __exit__(self, *exc):
        LT.get_dataframe = self.real


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
