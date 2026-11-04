"""Tanzania #1441 follow-ups: two named 2010-11 clusters, and 2019-20 split-offs.

The oracle reads the source files directly and imports nothing from
`Tanzania/_/`, so a mistake in the country code cannot agree with itself.

Every test here needs data and carries `requires_s3`, so it skips without
credentials.  With them, a build failure fails rather than skips.  The rule for the
guard on B is tested without data in test_gh1441_tanzania_district_code.py.
"""
import json
import re
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.derivations import call_inputs, records_for
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

DIGIT_KEY = "Tanzania::cluster_features::upd-district-name-from-id-digit"
NAMED = {"4115002": "LUSHOTO", "10506001": "NAMTUMBO"}
CONFLICT = "19302076"

pytestmark = pytest.mark.requires_s3


def letters(x):
    return re.sub(r"[^A-Z0-9]", "", str(x).upper())


def _read(path):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return get_dataframe(countries_root() / "Tanzania" / path)


def _build(fn):
    """Lets a build failure fail: `requires_s3` already skips without data."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = fn()
    assert df is not None and not df.empty
    return df


@pytest.fixture(scope="module")
def tz():
    return ll.Country("Tanzania")


@pytest.fixture(scope="module")
def cf(tz):
    return _build(tz.cluster_features).reset_index().astype({"t": str, "v": str})


@pytest.fixture(scope="module")
def upd():
    df = _read("2008-15/Data/upd4_hh_a.dta").copy()
    df["round"] = df["round"].astype(int)
    df["cid"] = df["clusterid"].map(lambda x: str(int(x)))
    df["code"] = df["ha_02_1"].astype(int)
    df["name"] = df["ha_02_2"].astype("string").str.strip().str.upper()
    df["digit"] = df["cid"].map(lambda c: int(c.zfill(8)[2]))
    return df


def _label(cf, wave, v):
    return cf[(cf["t"] == wave) & (cf["v"] == v)]["District"].iloc[0]


@pytest.mark.parametrize("v, name", NAMED.items())
def test_named_cluster_has_no_confirming_household_in_2010_11(upd, v, name):
    r2 = upd[(upd["round"] == 2) & (upd["cid"] == v)]
    assert len(r2) and (r2["code"] != r2["digit"]).all()


@pytest.mark.parametrize("v, name", NAMED.items())
def test_named_cluster_neighbouring_rounds_agree_with_the_digit(upd, v, name):
    r1 = upd[(upd["round"] == 1) & (upd["cid"] == v)]
    assert (r1["code"] == r1["digit"]).all()
    r3 = upd[(upd["round"] == 3) & (upd["cid"] == v)].drop_duplicates("r_hhid")
    assert r3["name"].value_counts().index[0] == name


@pytest.mark.parametrize("v, name", NAMED.items())
def test_named_cluster_is_served_its_neighbours_label(cf, v, name):
    assert letters(_label(cf, "2008-09", v)) == letters(name)
    assert letters(_label(cf, "2010-11", v)) == letters(name)
    assert letters(_label(cf, "2012-13", v)) == letters(name)
    row = cf[(cf["t"] == "2010-11") & (cf["v"] == v)]
    assert row["Derivation"].iloc[0] == DIGIT_KEY


def test_conflicting_cluster_stays_na_without_a_key(cf, upd):
    """Its neighbours disagree, so this is why it is not named."""
    r3 = upd[(upd["round"] == 3) & (upd["cid"] == CONFLICT)].drop_duplicates("r_hhid")
    assert letters(r3["name"].value_counts().index[0]) != letters(_label(cf, "2008-09", CONFLICT))
    assert pd.isna(_label(cf, "2010-11", CONFLICT))
    row = cf[(cf["t"] == "2010-11") & (cf["v"] == CONFLICT)]
    assert pd.isna(row["Derivation"].iloc[0])


def test_digit_key_is_on_the_named_clusters_only(cf):
    hit = cf[cf["Derivation"].eq(DIGIT_KEY).fillna(False).astype(bool)]
    assert set(zip(hit["t"], hit["v"])) == {("2010-11", v) for v in NAMED}


def test_market_district_leaves_only_the_conflicting_cluster_na_in_2010_11(tz):
    sample = _build(tz.sample).reset_index().astype({"t": str, "v": "string"})
    market = _build(lambda: tz.sample(market="District")).reset_index().astype({"t": str})
    expected = set(sample[(sample["t"] == "2010-11") & (sample["v"] == CONFLICT)]["i"])
    got = set(market[(market["t"] == "2010-11") & market["m"].isna()]["i"])
    assert got == expected and got


# --------------------------------------------------------------------------
# B.  2019-20 split-offs take the parent's cluster.
# --------------------------------------------------------------------------

SPLIT_KEY = "Tanzania::sample::split-off-parent-cluster"


@pytest.fixture(scope="module")
def sdd():
    """HH_SEC_A.dta with the expected `v` worked out from the source alone."""
    raw = _read("2019-20/Data/HH_SEC_A.dta").copy()
    raw["own_v"] = raw["clusterid"].map(lambda x: str(int(x)))
    raw["in_frame"] = raw["sdd_cluster"].astype("string").str.strip().replace("", pd.NA).notna()
    frame = raw[raw["in_frame"]]
    frame_ids = set(frame["own_v"])
    sibling = frame.groupby("y4_hhid")["own_v"].agg(lambda s: sorted(set(s)))
    raw["stray"] = ~raw["in_frame"] & ~raw["own_v"].isin(frame_ids)
    raw["sibling"] = raw["y4_hhid"].map(sibling)
    raw["rekey"] = raw["stray"] & raw["sibling"].notna()
    raw["expected_v"] = raw["own_v"]
    raw.loc[raw["rekey"], "expected_v"] = raw.loc[raw["rekey"], "sibling"].map(lambda s: s[0])
    walk = json.loads((countries_root() / "Tanzania" / "_" / "updated_ids.json").read_text())["2019-20"]
    raw["i"] = raw["sdd_hhid"].map(lambda x: walk.get(x, x))
    return raw.set_index("i")


@pytest.fixture(scope="module")
def sample_2019(tz):
    return _build(tz.sample).xs("2019-20", level="t")


def test_each_rekeyed_household_has_one_parent_cluster(sdd):
    assert sdd["rekey"].any()
    assert (sdd.loc[sdd["rekey"], "sibling"].map(len) == 1).all()


def test_no_2019_20_household_is_gained_or_lost(sdd, sample_2019):
    assert set(sample_2019.index) == set(sdd.index)
    assert sample_2019.index.is_unique


def test_v_is_the_parent_cluster_exactly_where_the_source_says(sdd, sample_2019):
    served = sample_2019["v"].astype(str)
    assert (served.reindex(sdd.index) == sdd["expected_v"]).all()


def test_key_is_on_exactly_the_rekeyed_households(sdd, sample_2019):
    stamped = sample_2019["Derivation"].eq(SPLIT_KEY).fillna(False).astype(bool)
    assert set(stamped[stamped].index) == set(sdd[sdd["rekey"]].index)
    assert sample_2019["Derivation"].dropna().eq(SPLIT_KEY).all()


def test_design_columns_stay_the_households_own(sdd, sample_2019):
    s = sample_2019.reindex(sdd.index)
    assert (s["strata"].astype(str).str.upper().str.replace(r"\W", "", regex=True)
            == sdd["strataid"].astype(str).str.upper().str.replace(r"\W", "", regex=True)).all()
    ratio = (s["weight"].astype(float) / sdd["sdd_weights"].astype(float))
    assert ratio.max() - ratio.min() < 1e-9, "weight normalisation must be one constant"
    assert (s["weight"] == s["panel_weight"]).all()


def test_every_rekeyed_v_is_in_cluster_features(sdd, cf):
    known = set(cf.loc[cf["t"] == "2019-20", "v"])
    assert set(sdd.loc[sdd["rekey"], "expected_v"]) <= known


def test_cluster_features_gains_no_stray_cluster(sdd, cf):
    assert set(cf.loc[cf["t"] == "2019-20", "v"]) == set(sdd.loc[sdd["in_frame"], "own_v"])


def test_only_strays_without_a_parent_stay_unlabelled(tz, sdd):
    for col in ("Region", "District"):
        market = _build(lambda col=col: tz.sample(market=col)).xs("2019-20", level="t").reset_index()
        got = set(market.loc[market["m"].isna(), "i"])
        expected = set(sdd[sdd["stray"] & ~sdd["rekey"]].index)
        if col == "Region":
            assert got == expected and got
        else:
            assert expected <= got


def test_the_new_v_reaches_other_household_tables(tz, sdd):
    """`_join_v_from_sample` carries it, so no other table states a second `v`."""
    roster = _build(tz.household_roster)
    v = roster.index.get_level_values("v")[roster.index.get_level_values("t") == "2019-20"].astype(str)
    i = roster.index.get_level_values("i")[roster.index.get_level_values("t") == "2019-20"]
    got = pd.Series(v, index=i).groupby(level=0).first()
    rekeyed = sdd[sdd["rekey"]]
    both = got.index.intersection(rekeyed.index)
    assert len(both)
    assert (got.reindex(both) == rekeyed["expected_v"].reindex(both)).all()


def test_two_origin_households_of_2014_15_keep_no_v(tz, upd):
    r4 = upd[upd["round"] == 4]
    n = r4.groupby("r_hhid")["cid"].nunique()
    served = _build(tz.sample).xs("2014-15", level="t")
    assert int(served["v"].isna().sum()) == int((n > 1).sum())


def test_registered_inputs_join_every_rekeyed_household(tz, sdd):
    rec = records_for("Tanzania", "sample")[SPLIT_KEY]
    assert set(rec["raw_variables"]) <= set(call_inputs(rec).columns)
    inputs = tz.derivation_inputs(SPLIT_KEY)
    keys = set(inputs.xs("2019-20", level="t").index)
    assert set(sdd[sdd["rekey"]].index) <= keys
    summary = _build(tz.sample).attrs["derivations"]["Tanzania"][SPLIT_KEY]
    assert summary["rows"] == int(sdd["rekey"].sum())
