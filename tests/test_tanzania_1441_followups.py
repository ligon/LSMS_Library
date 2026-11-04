"""Tanzania #1441 follow-ups: two named 2010-11 clusters, and 2019-20 split-offs.

The oracle reads the source files directly and imports nothing from
`Tanzania/_/`, so a mistake in the country code cannot agree with itself.

Data-dependent tests skip cleanly when the Tanzania source files are unavailable.
"""
import re
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

DIGIT_KEY = "Tanzania::cluster_features::upd-district-name-from-id-digit"
NAMED = {"4115002": "LUSHOTO", "10506001": "NAMTUMBO"}
CONFLICT = "19302076"

pytestmark = pytest.mark.requires_s3


def letters(x):
    return re.sub(r"[^A-Z0-9]", "", str(x).upper())


def _read(path):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return get_dataframe(countries_root() / "Tanzania" / path)
    except Exception as exc:  # noqa: BLE001 - any data failure -> skip
        pytest.skip(f"{path} unavailable: {exc}")


def _build(fn):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = fn()
    except Exception as exc:  # noqa: BLE001 - any build/data failure -> skip
        pytest.skip(f"Tanzania table unavailable: {exc}")
    if df is None or df.empty:
        pytest.skip("Tanzania table empty")
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
