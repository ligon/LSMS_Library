"""Burkina Faso 2021-22 ``cluster_features`` covers BOTH sample components (GH #1276).

The 2021-22 wave is two disjoint World Bank entries of one EHCVM-2 round: the
panel (6224, grappes 2-585, ``Data/``) and the cross-section (6277, grappes
586-1185, ``Data/Cross_Section/``).  ``sample()`` and ``household_roster()``
read both (#1322), but ``cluster_features`` read only the panel cover.  So the
600 cross-section grappes had no cluster row, and every ``market=`` lookup --
which falls back to ``cluster_features`` because Burkina's ``sample()`` has no
Region column -- silently dropped their households: measured on the warm cache
at development ``3a33f2869``, ``household_roster(market='Region')`` served
22,362 of 69,452 2021-22 rows and ``sample(market='Region')`` 3,227 of 10,403.

The cross-section cover carries household GPS (« Coordonnées GPS du ménage »,
7,128 of 7,176 populated) that differs within 578 of its 600 grappes.  That is
a household position, not a cluster attribute, so it is deliberately not
mapped: the (t, v) projection would select one household's point.  GH #1057
carries a PANEL grappe's 2018-19 fix through a second sub-df; a cross-section
grappe matches nothing in that file and stays NA.

Config and invented-input tests run anywhere; the data tests are cache/S3-gated.
"""
import importlib.util
import os
import pathlib

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
PANEL_COVER = "s00_me_bfa2021.dta"
CS_COVER = "Cross_Section/s00_me_bfa2021.dta"
GPS_2018 = "grappe_gps_bfa2018.dta"
ROOT = countries_root() / COUNTRY


def _cluster_spec():
    """The covers' sub-df.  Re-pinned for GH #1057, which added df_geo beside it."""
    with open(ROOT / "2021-22" / "_" / "data_info.yml") as f:
        return yaml.safe_load(f)["cluster_features"]["df_main"]


def _file_names(spec):
    names = []
    for entry in spec["file"] if isinstance(spec["file"], list) else [spec["file"]]:
        names.append(next(iter(entry)) if isinstance(entry, dict) else entry)
    return names


def test_2021_22_cluster_spec_reads_both_covers():
    spec = _cluster_spec()
    assert _file_names(spec) == [PANEL_COVER, CS_COVER]
    assert spec["idxvars"]["v"] == "grappe"
    assert set(spec["myvars"]) == {"Region", "District", "Rural"}


def test_2021_22_cluster_spec_does_not_map_household_gps():
    """Household positions must not become a cluster coordinate."""
    myvars = _cluster_spec()["myvars"]
    assert "gps" not in yaml.safe_dump(myvars).lower()
    assert not {"Latitude", "Longitude"} & set(myvars)


@pytest.fixture
def raw_covers(monkeypatch):
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    frames = {
        PANEL_COVER: pd.DataFrame({
            "grappe": [2, 2, 3], "menage": [1, 2, 1],
            "s00q01": ["Centre", "Centre", "Sahel"],
            "s00q02": ["Kadiogo", "Kadiogo", "Séno"],
            "s00q04": ["Urbain", "Urbain", "Rural"]}),
        CS_COVER: pd.DataFrame({
            "grappe": [586, 586, 900], "menage": [1, 2, 126],
            "s00q01": ["Est", "Est", "Nord"],
            "s00q02": ["Gourma", "Gourma", "Yatenga"],
            "s00q04": ["Rural", "Rural", "Urbain"]}),
        # GH #1057: the df_geo read, one row per 2018-19 grappe.  None reaches 586.
        GPS_2018: pd.DataFrame({
            "grappe": [2, 3, 4],
            "coordonnes_gps__Latitude": [12.5, float("nan"), 13.5],
            "coordonnes_gps__Longitude": [-1.5, float("nan"), -0.5]}),
    }
    calls = []

    def read(path, **kwargs):
        key = str(path).split("/Data/")[-1]
        assert key in frames, key
        calls.append(key)
        return frames[key].copy()

    monkeypatch.setattr(local_tools, "get_dataframe", read)
    return calls


def test_cross_section_grappes_get_cluster_rows(raw_covers):
    df = ll.Country(COUNTRY)["2021-22"].cluster_features()
    assert sorted(raw_covers) == sorted([PANEL_COVER, CS_COVER, GPS_2018])
    flat = df.reset_index()
    flat["v"] = flat["v"].astype(str)
    assert sorted(flat["v"]) == ["2", "3", "586", "900"]
    assert flat["v"].is_unique
    assert flat.set_index("v")["Region"].to_dict() == {
        "2": "Centre", "3": "Sahel", "586": "Est", "900": "Nord"}
    assert flat.set_index("v")["Rural"].to_dict() == {
        "2": "Urban", "3": "Rural", "586": "Rural", "900": "Urban"}
    assert "i" not in df.index.names and "i" not in df.columns
    # GH #1057: a left merge, so grappe 4 (not in either cover) is not served,
    # and only the panel grappe with a fix carries one.
    assert flat.set_index("v")["Latitude"].dropna().to_dict() == {"2": 12.5}


# --------------------------------------------------------------------------
# Data (cache/S3-gated)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _non_strict():
    """2014 keeps a source-property GrainCollapseWarning on 2 tied zd
    (GH #1060) that LSMS_GRAIN_STRICT=1 would turn into an abort."""
    prior = os.environ.pop("LSMS_GRAIN_STRICT", None)
    yield
    if prior is not None:
        os.environ["LSMS_GRAIN_STRICT"] = prior


@requires_s3
def test_cs_cover_attributes_are_grappe_constants_and_gps_is_not():
    path = ROOT / "2021-22" / "Data" / CS_COVER
    df = local_tools.get_dataframe(str(path))
    assert len(df) == 7176
    assert df["grappe"].between(586, 1185).all() and df["grappe"].nunique() == 600
    g = df.groupby("grappe")
    for col in ("s00q01", "s00q02", "s00q04"):
        assert (g[col].nunique(dropna=False) == 1).all(), col
    # Household positions, not a cluster fix: they vary within most grappes.
    assert (g["GPS__Latitude"].nunique() > 1).sum() > 500


@requires_s3
def test_every_2021_22_sample_cluster_has_a_cluster_row(_non_strict):
    c = ll.Country(COUNTRY)
    s = c.sample(waves=["2021-22"])
    cf = c.cluster_features(waves=["2021-22"])
    served = set(cf.index.get_level_values("v"))
    assert set(s["v"]) <= served
    assert len(cf) == 1150


@requires_s3
def test_market_join_keeps_cross_section_households(_non_strict):
    c = ll.Country(COUNTRY)
    plain = c.sample(waves=["2021-22"])
    market = c.sample(waves=["2021-22"], market="Region")
    assert len(market) == len(plain) == 10403
    assert market.index.get_level_values("m").notna().all()
    cs = pd.Series(market.index.get_level_values("i")).str.startswith("CS_")
    assert cs.sum() == 7176
