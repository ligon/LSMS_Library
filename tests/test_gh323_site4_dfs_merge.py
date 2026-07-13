"""GH #323, site 4 --- the ``dfs:`` merge in ``Wave.grab_data``.

Sites 1-3 of #323 all LOSE data: a ``groupby().first()`` collapses a
non-unique index and the dropped rows vanish.  Site 4 is upstream of all of
them and is the opposite failure --- it MANUFACTURES data.  ``dfs:`` blocks
outer-merged their sub-frames on the declared ``merge_on`` keys with no
cardinality guard, so when both sub-frames were finer-grained than the merge
key (two HOUSEHOLD-grain frames joined on the CLUSTER key ``v``) the join was
many-to-many and exploded into a cartesian product.  The collapse downstream
then mopped it up, and the table looked clean.  Ethiopia's ``cluster_features``
alone fabricated 65,508 rows in 2013-14 and 57,786 in 2015-16 --- from tables
that should have had 433 and 432 rows.

Three guarantees are tested here:

1. **The cartesian test is exact.**  ``_cartesian_keys`` fires on m:m and stays
   silent on 1:1 / 1:m / m:1 --- sound *and* complete, not a row-count
   heuristic.  Null keys count, because ``pd.merge`` matches them.
2. **``merge_how:`` is honoured.**  A new YAML key must not be dead config.
3. **A dropped sub-df that owned a REQUIRED column is a hard error.**  The GH
   #515 optional-sub-df fallback swallowed a ``KeyError`` and silently served
   the table with a required column 100% absent.  A config bug (the file does
   not carry the column the YAML names) now raises; a genuinely unavailable
   file still degrades softly, because no config edit can fix that one.
"""
from __future__ import annotations


import warnings

import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library.country import _required_scheme_columns


# --------------------------------------------------------------------------
# _cartesian_keys / _merge_subframes --- unit level, no config, no microdata
# --------------------------------------------------------------------------

def _wave_stub(name: str = "Testland/2020") -> C.Wave:
    """A Wave with only the attributes the merge guard touches."""
    w = object.__new__(C.Wave)
    w.name = name
    return w


def test_cartesian_keys_detects_many_to_many():
    left = pd.DataFrame({"v": ["c1", "c1", "c2"], "Region": ["N", "N", "S"]})
    right = pd.DataFrame({"v": ["c1", "c1", "c2"], "Lat": [1.0, 1.1, 2.0]})
    bad = _wave_stub()._cartesian_keys(left, right, ["v"])
    assert bad is not None
    assert list(bad.index) == ["c1"]          # only c1 is duplicated on BOTH sides
    assert bad.loc["c1", "n"] == 2 and bad.loc["c1", "m"] == 2


@pytest.mark.parametrize("right_v", [
    pytest.param(["c1", "c2"], id="one_to_one"),
    pytest.param(["c1", "c2", "c3"], id="one_to_many_right_unique"),
])
def test_cartesian_keys_silent_when_one_side_is_unique(right_v):
    """1:1, 1:m and m:1 are honest joins and must not be flagged.

    The left frame is deliberately DUPLICATED on the key.  A guard that merely
    asked "is either side non-unique?" would false-positive here -- which is
    why the test is the INTERSECTION of the two duplicate sets, not the union.
    """
    left = pd.DataFrame({"v": ["c1", "c1", "c2"], "Region": ["N", "N", "S"]})
    right = pd.DataFrame({"v": right_v, "Lat": [1.0] * len(right_v)})
    assert _wave_stub()._cartesian_keys(left, right, ["v"]) is None


def test_cartesian_keys_counts_null_keys():
    """``pd.merge`` MATCHES null keys to each other, so a null key duplicated
    on both sides is a cartesian like any other -- and a common one, since a
    failed upstream extraction dumps every row onto the same null key."""
    left = pd.DataFrame({"v": [None, None, "c2"], "Region": ["N", "N", "S"]})
    right = pd.DataFrame({"v": [None, None, "c2"], "Lat": [1.0, 1.1, 2.0]})
    bad = _wave_stub()._cartesian_keys(left, right, ["v"])
    assert bad is not None and len(bad) == 1
    # ... and pandas really does explode on it, which is what makes it a bug:
    assert len(pd.merge(left, right, on="v", how="outer")) == 5   # 2x2 + 1x1


def test_cartesian_keys_no_keys_is_none():
    assert _wave_stub()._cartesian_keys(pd.DataFrame(), pd.DataFrame(), []) is None


def test_merge_subframes_warns_with_exact_phantom_count():
    left = pd.DataFrame({"v": ["c1", "c1", "c2", "c2"], "Region": list("NNSS")})
    right = pd.DataFrame({"v": ["c1", "c1", "c2", "c2"], "Lat": [1.0, 1.1, 2.0, 2.1]})
    with pytest.warns(UserWarning, match="CARTESIAN PRODUCT"):
        out = _wave_stub()._merge_subframes(
            left, right, ["v"], "cluster_features", "df_main", "df_geo")
    # 2 clusters x (2x2) = 8 rows out of two 4-row frames.
    assert len(out) == 8
    # Phantoms = n*m - max(n,m) per bad key = (4-2) + (4-2) = 4.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _wave_stub()._merge_subframes(
            left, right, ["v"], "cluster_features", "df_main", "df_geo")
    assert "MANUFACTURING 4 phantom rows" in str(caught[0].message)


def test_merge_subframes_fatal_under_grain_strict(monkeypatch):
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    left = pd.DataFrame({"v": ["c1", "c1"], "Region": ["N", "N"]})
    right = pd.DataFrame({"v": ["c1", "c1"], "Lat": [1.0, 1.1]})
    with pytest.raises(ValueError, match="CARTESIAN PRODUCT"):
        _wave_stub()._merge_subframes(
            left, right, ["v"], "cluster_features", "df_main", "df_geo")


def test_merge_subframes_clean_merge_is_silent_and_unchanged():
    """The guard must not perturb a healthy merge -- byte-identical to the
    ``pd.merge`` it replaced."""
    left = pd.DataFrame({"i": ["h1", "h2"], "Region": ["N", "S"]})
    right = pd.DataFrame({"i": ["h1", "h3"], "Lat": [1.0, 3.0]})
    with warnings.catch_warnings():
        warnings.simplefilter("error")   # any warning fails the test
        out = _wave_stub()._merge_subframes(
            left, right, ["i"], "cluster_features", "df_main", "df_geo")
    pd.testing.assert_frame_equal(out, pd.merge(left, right, on=["i"], how="outer"))


def test_merge_subframes_honours_how():
    """``merge_how: left`` keeps the PRIMARY sub-df authoritative for which
    rows exist; the default stays 'outer' (historical behaviour)."""
    left = pd.DataFrame({"i": ["h1", "h2"], "Region": ["N", "S"]})
    right = pd.DataFrame({"i": ["h1", "h9"], "Lat": [1.0, 9.0]})   # h9 is an orphan
    outer = _wave_stub()._merge_subframes(
        left, right, ["i"], "cluster_features", "df_main", "df_geo")
    lefty = _wave_stub()._merge_subframes(
        left, right, ["i"], "cluster_features", "df_main", "df_geo", how="left")
    assert set(outer["i"]) == {"h1", "h2", "h9"}   # orphan admitted
    assert set(lefty["i"]) == {"h1", "h2"}         # orphan refused


# --------------------------------------------------------------------------
# _required_scheme_columns --- the shared required-vs-optional reading
# --------------------------------------------------------------------------

def test_required_scheme_columns_skips_metadata_and_optionals():
    entry = {
        "index": "(t, v)",          # structural, not a column
        "materialize": "make",      # structural
        "aggregation": {"x": "first"},   # historical reservation, never a column
        "Region": "str",
        "Latitude": "float",
        "Elevation": {"type": "float", "optional": True},
    }
    assert sorted(_required_scheme_columns(entry)) == ["Latitude", "Region"]


def test_required_scheme_columns_tolerates_non_dict():
    assert _required_scheme_columns(None) == []
    assert _required_scheme_columns("nonsense") == []


# --------------------------------------------------------------------------
# End-to-end through Wave.grab_data, on a synthetic country config tree
# --------------------------------------------------------------------------

_SCHEME = """\
Waves:
  - '2020'
Data Scheme:
  cluster_features:
    index: (t, v)
    Region: str
    Latitude: float
"""

_SCHEME_OPTIONAL_LAT = """\
Waves:
  - '2020'
Data Scheme:
  cluster_features:
    index: (t, v)
    Region: str
    Latitude:
      type: float
      optional: true
"""


def _data_info(geo_lat_col: str = "lat", geo_file: str = "geo.csv",
               merge_how: str | None = None) -> str:
    lines = [
        "cluster_features:",
        "  dfs:",
        "    - df_main",
        "    - df_geo",
        "  merge_on:",
        "    - v",
    ]
    if merge_how:
        lines.append(f"  merge_how: {merge_how}")
    lines += [
        "  final_index:",
        "    - t",
        "    - v",
        "  df_main:",
        "    file: main.csv",
        "    idxvars:",
        "      v: ea",
        "    myvars:",
        "      Region: reg",
        "  df_geo:",
        f"    file: {geo_file}",
        "    idxvars:",
        "      v: ea",
        "    myvars:",
        f"      Latitude: {geo_lat_col}",
        "",
    ]
    return "\n".join(lines)


@pytest.fixture
def testland(tmp_path, monkeypatch):
    """A one-wave synthetic country whose two sub-frames are BOTH
    household-grain but are merged on the cluster key -- Ethiopia in miniature.

    Returns a builder: ``testland(scheme=..., data_info=..., geo=...)`` writes
    the config tree and hands back the ``Wave``.
    """
    from lsms_library.paths import countries_root, data_root

    croot = tmp_path / "countries"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    countries_root.cache_clear()
    data_root.cache_clear()

    def build(scheme: str = _SCHEME, data_info: str | None = None,
              geo: pd.DataFrame | None = None, geo_file: str = "geo.csv"):
        c = croot / "Testland"
        (c / "_").mkdir(parents=True, exist_ok=True)
        (c / "2020" / "_").mkdir(parents=True, exist_ok=True)
        (c / "2020" / "Data").mkdir(parents=True, exist_ok=True)
        # 4 households in 2 clusters, on BOTH sides -> merging on `ea` is 2x2
        # cartesian within each cluster.
        pd.DataFrame({"hid": ["h1", "h2", "h3", "h4"],
                      "ea": ["c1", "c1", "c2", "c2"],
                      "reg": ["N", "N", "S", "S"]}
                     ).to_csv(c / "2020" / "Data" / "main.csv", index=False)
        if geo is None:
            geo = pd.DataFrame({"hid": ["h1", "h2", "h3", "h4"],
                                "ea": ["c1", "c1", "c2", "c2"],
                                "lat": [1.0, 1.1, 2.0, 2.1]})
        geo.to_csv(c / "2020" / "Data" / geo_file, index=False)
        (c / "_" / "data_scheme.yml").write_text(scheme)
        (c / "2020" / "_" / "data_info.yml").write_text(
            data_info if data_info is not None else _data_info())
        return C.Country("Testland")["2020"]

    try:
        yield build
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def test_grab_data_warns_on_cartesian_dfs_merge(testland):
    """The Ethiopia shape, in miniature: two household-grain sub-frames merged
    on the cluster key.  8 rows come back where the cluster table has 2."""
    wave = testland()
    with pytest.warns(UserWarning, match="CARTESIAN PRODUCT"):
        df = wave.grab_data("cluster_features")
    assert len(df) == 8          # 2 clusters x 2 x 2 -- the fabrication itself


def test_grab_data_merge_how_left_is_honoured(testland):
    """``merge_how:`` must not be dead config: declaring ``left`` refuses the
    geo file's orphan cluster, ``outer`` (the default) admits it."""
    geo = pd.DataFrame({"hid": ["h1", "h3", "h9"], "ea": ["c1", "c2", "c9"],
                        "lat": [1.0, 2.0, 9.0]})   # c9 is in no cover page
    outer = testland(geo=geo).grab_data("cluster_features")
    assert "c9" in outer.index.get_level_values("v")

    lefty = testland(data_info=_data_info(merge_how="left"),
                     geo=geo).grab_data("cluster_features")
    assert "c9" not in lefty.index.get_level_values("v")


def test_dropped_subdf_costing_a_required_column_is_a_hard_error(testland):
    """GH #515's fallback swallowed the KeyError and served the table with
    Latitude 100% absent.  Ethiopia lost Lat/Lon from 3 of 5 waves this way --
    the YAML said ``lat_dd_mod``, the file had ``LAT_DD_MOD``."""
    wave = testland(data_info=_data_info(geo_lat_col="LAT_NOT_HERE"))
    with pytest.raises(RuntimeError, match=r"required declared column\(s\) \['Latitude'\]"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            wave.grab_data("cluster_features")


def test_dropped_subdf_costing_only_an_optional_column_stays_soft(testland):
    """The escalation is keyed on ``optional: true``, not on "a sub-df was
    dropped" -- a wave that genuinely lacks the data is allowed to say so."""
    wave = testland(scheme=_SCHEME_OPTIONAL_LAT,
                    data_info=_data_info(geo_lat_col="LAT_NOT_HERE"))
    with pytest.warns(UserWarning, match="could not load"):
        df = wave.grab_data("cluster_features")
    assert "Latitude" not in df.columns
    assert "Region" in df.columns          # the primary sub-df still delivered


def test_missing_file_stays_soft_even_for_a_required_column(testland):
    """A file that is not AVAILABLE is not a config bug -- no YAML edit fixes
    it, and hard-failing would break every legitimate partial-data checkout.
    Only the KeyError kind (file loaded, column absent) escalates."""
    wave = testland(data_info=_data_info(geo_file="absent.csv"), geo_file="geo.csv")
    with pytest.warns(UserWarning, match="could not load"):
        df = wave.grab_data("cluster_features")
    assert "Latitude" not in df.columns
    assert "Region" in df.columns
