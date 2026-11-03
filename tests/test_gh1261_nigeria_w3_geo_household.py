"""Nigeria W3: the one geovariables household on neither W3 cover (GH #1261).

``NGA_HouseholdGeovars_Y3.dta`` lists exactly one household that neither W3
household cover lists, hhid ``320201``, with blank admin codes.  The survey's
own files account for it (``Nigeria/_/CONTENTS.org``, GH #1261 entry):

* ``HHTrack.dta`` records it as a W1/W2 panel household in Rivers / Bonny /
  EA 1464 and "3. Not found" in BOTH W3 visits, with no W3 weight;
* the PH roster nevertheless lists two members at that EA;
* the GH #1120 D1 roster supplement therefore serves it in 2016Q1 ``sample``
  at ``v = 32/3207/1464`` with NA weights, and the LEFT geo join gives its
  household row a coordinate -- nothing invented, nothing dropped.

Data-gated (source files through the ordinary reader); no cache is read.
"""
from __future__ import annotations

import importlib.util
import sys

import pandas as pd
import pytest

from lsms_library.paths import countries_root

NIGERIA = countries_root() / "Nigeria"
W3 = NIGERIA / "2015-16" / "Data"
HH = "320201"

pytestmark = pytest.mark.requires_s3


def _read(fn, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(W3 / fn), **kw)


def _ids(df):
    return set(df["hhid"].astype(int).astype(str))


@pytest.fixture(scope="module")
def ng():
    spec = importlib.util.spec_from_file_location("nigeria", NIGERIA / "_" / "nigeria.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("nigeria", mod)
    spec.loader.exec_module(mod)
    return mod


def test_exactly_one_geo_household_is_on_neither_cover():
    geo = _read("NGA_HouseholdGeovars_Y3.dta", convert_categoricals=False)
    pp = _read("secta_plantingw3.dta", convert_categoricals=False)
    ph = _read("secta_harvestw3.dta", convert_categoricals=False)
    assert _ids(geo) - _ids(pp) - _ids(ph) == {HH}
    row = geo[geo["hhid"].astype(int).astype(str) == HH]
    assert row[["zone", "state", "lga", "ea"]].isna().all(axis=None)
    assert row["LAT_DD_MOD"].notna().all()


def test_hhtrack_records_it_as_not_found_in_both_w3_visits():
    tr = _read("HHTrack.dta", convert_categoricals=True)
    row = tr[tr["hhid"].astype(int).astype(str) == HH].iloc[0]
    for v in ("hhstatus_w1v1", "hhstatus_w1v2", "hhstatus_w2v1", "hhstatus_w2v2"):
        assert str(row[v]).startswith("1."), (v, row[v])
    assert str(row["hhstatus_w3v1"]) == "3. Not found"
    assert str(row["hhstatus_w3v2"]) == "3. Not found"
    assert pd.isna(row["wt_wave3"])
    assert int(row["ea"]) == 1464


def test_ph_roster_locates_it_at_its_original_ea():
    ros = _read("sect1_harvestw3.dta", convert_categoricals=False)
    r = ros[ros["hhid"].astype(int).astype(str) == HH]
    assert len(r) == 2
    assert set(zip(r["state"], r["lga"], r["ea"])) == {(32, 3207, 1464)}
    pp_ros = _read("sect1_plantingw3.dta", convert_categoricals=False)
    assert HH not in _ids(pp_ros)


def test_d1_serves_it_in_2016Q1_with_na_weights_and_the_eas_coordinate(ng):
    s = ng.sample_for_wave("2015-16")
    assert s.groupby(level="t").size().to_dict() == {"2015Q3": 4696, "2016Q1": 4594}
    row = s.xs(HH, level="i")
    assert list(row.index) == ["2016Q1"]
    assert row.loc["2016Q1", "v"] == "32/3207/1464"
    assert pd.isna(row.loc["2016Q1", "weight"]) and pd.isna(row.loc["2016Q1", "panel_weight"])
    cf = ng.cluster_features_for_wave("2015-16")
    mine = cf[cf.index.get_level_values("i") == HH]
    assert mine.index.tolist() == [("2016Q1", HH, "32/3207/1464")]
    assert mine["Latitude"].notna().all()
    # The coordinate is the EA's: one pair across every household of the EA.
    ea = cf.xs(("2016Q1", "32/3207/1464"), level=("t", "v"))
    assert ea[["Latitude", "Longitude"]].round(6).drop_duplicates().shape[0] == 1


def test_roster_less_2015Q3_crosswalk_ids_are_hhtrack_non_interviews():
    """GH #1264 context: the 85 PP-cover ids with no PP roster rows are
    exactly the PP-cover households HHTrack records as not interviewed at
    W3 visit 1."""
    tr = _read("HHTrack.dta", convert_categoricals=True)
    tr["h"] = tr["hhid"].astype(int).astype(str)
    interviewed = {"1. Interviewed (in main survey phase)", "5. Tracked (and interviewed)"}
    pp = _ids(_read("secta_plantingw3.dta", convert_categoricals=False))
    pp_ros = _ids(_read("sect1_plantingw3.dta", convert_categoricals=False))
    not_interviewed = set(tr.loc[~tr["hhstatus_w3v1"].astype(str).isin(interviewed), "h"]) & pp
    assert len(pp - pp_ros) == 85
    assert pp - pp_ros == not_interviewed
