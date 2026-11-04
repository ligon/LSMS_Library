"""Tanzania `District` in NPS rounds 1-2, from the district code (GH #1441).

The UPD ships the within-region code `ha_02_1` in every round and leaves the name
`ha_02_2` blank in rounds 1-2, so `market='District'` had no label for any of
those households.  `2008-15/_/categorical_mapping.org` table `upd_district_code`
gives each code the name round 3's residents write for it.

Three tiers.  The config tier needs no data.  The source tier is the oracle: it
reads `upd4_hh_a.dta` directly and recomputes what the table claims, so neither a
re-release nor a hand edit can leave a row standing without its evidence.  The
served tier checks the rule through the Country API.

Data-dependent tests skip cleanly when the Tanzania source files are unavailable.
"""
import importlib.util
import re
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

EARLY = {1: "2008-09", 2: "2010-11"}
TABLE = "upd_district_code"


def _org_path():
    return countries_root() / "Tanzania" / "2008-15" / "_" / "categorical_mapping.org"


@pytest.fixture(scope="module")
def table():
    df = df_from_orgfile(_org_path(), name=TABLE)
    df["Code"] = df["Code"].astype(int)
    return df.set_index("Code")


@pytest.fixture(scope="module")
def tzmod():
    """The country module, loaded the way the config does."""
    path = countries_root() / "Tanzania" / "_" / "tanzania.py"
    spec = importlib.util.spec_from_file_location("_tz_gh1441_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# Config tier
# --------------------------------------------------------------------------

def test_table_cannot_be_reached_by_the_read_time_auto_apply(table):
    """The auto-apply matches a table NAME to a column name and needs a
    `Preferred Label`.  Either would key names against codes on every frame."""
    assert TABLE.lower() not in {"district", "region", "code"}
    assert "Preferred Label" not in table.columns


def test_every_code_decodes_to_its_named_region(table, tzmod):
    assert table.index.is_unique
    for code, row in table.iterrows():
        region_code, digit = f"{code // 10:02d}", code % 10
        assert 1 <= digit <= 9, code
        assert tzmod.TZ_REGION_BY_CODE[region_code] == row["Region"], code


def test_a_name_means_one_cell(table):
    """Two cells sharing a name would merge two places under `market='District'`."""
    assert table["District"].notna().all()
    assert table["District"].is_unique


def _frame(rows):
    df = pd.DataFrame(rows, columns=["i", "t", "v", "Region", "_district_code"])
    return df.set_index(["i", "t", "v"])


@pytest.mark.parametrize(
    "region, code, v, expected",
    [
        ("DODOMA", "1.0", "1114002", "KONDOA"),         # the '1.0' blank_to_na writes
        ("DODOMA", 1, "01114002", "KONDOA"),            # zero-padded id, plain int
        ("MJINI/MAGHARIBI UNGUJA", "2.0", "53203001", "URBAN"),  # source's spelling
        ("DODOMA", "2.0", "1114002", pd.NA),            # code is not the id's digit
        ("ARUSHA", "1.0", "1114002", pd.NA),            # region is not the id's
        ("DODOMA", pd.NA, "1114002", pd.NA),            # no code reported
        ("DODOMA", "9.0", "1914002", pd.NA),            # a cell the table lacks
    ],
)
def test_labels_only_a_report_its_own_cluster_id_confirms(tzmod, region, code, v, expected):
    df = _frame([("a", "2008-09", v, region, code)])
    got = tzmod.upd_district_labels(df, codes={11: "KONDOA", 532: "URBAN"}).iloc[0]
    assert (pd.isna(got) and pd.isna(expected)) or got == expected


# --------------------------------------------------------------------------
# Source tier: the oracle reads the .dta
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def upd(tzmod):
    """`upd4_hh_a.dta` at (household, round, cluster) grain, with the geocode."""
    path = countries_root() / "Tanzania" / "2008-15" / "Data" / "upd4_hh_a.dta"
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = get_dataframe(path)
    except Exception as exc:  # noqa: BLE001 - any data failure -> skip
        pytest.skip(f"upd4_hh_a.dta unavailable: {exc}")
    df = raw.drop_duplicates(["r_hhid", "round", "clusterid"]).copy()
    df["round"] = df["round"].astype(int)
    df["cid"] = df["clusterid"].map(lambda x: str(int(x)))
    df["region"] = df["ha_01_1"].astype("string")
    df["code"] = df["ha_02_1"].astype("Int64").astype("string")
    df["name"] = df["ha_02_2"].astype("string").str.strip().replace("", pd.NA)
    df["geo_rc"] = df["cid"].map(lambda x: tzmod.cluster_region_code(x, "nps"))
    df["geo_region"] = df["cid"].map(lambda x: tzmod.cluster_region(x, "nps")).astype("string")
    df["geo_digit"] = df["cid"].map(lambda x: tzmod.cluster_district_code(x, "nps")).astype("string")
    df["resident"] = (df["region"].map(tzmod.normalize_place)
                      == df["geo_region"].map(tzmod.normalize_place)) & (df["code"] == df["geo_digit"])
    df["cell"] = df["geo_rc"].astype(int) * 10 + df["geo_digit"].astype(int)
    return df


@pytest.mark.requires_s3
def test_premise_rounds_1_2_ship_the_code_and_no_name(upd):
    """If a re-release fills `ha_02_2`, the survey's answer replaces this table."""
    early = upd[upd["round"].isin(EARLY)]
    assert early["name"].isna().all()
    assert early["code"].notna().all()
    assert upd.loc[upd["round"] == 3, "name"].notna().all()


@pytest.mark.requires_s3
def test_premise_round_1_code_is_the_geocode_digit(upd):
    r1 = upd[upd["round"] == 1]
    assert r1["resident"].all()


@pytest.mark.requires_s3
def test_every_row_is_the_plurality_of_round_3_residents(table, upd):
    r3 = upd[(upd["round"] == 3) & upd["resident"]]
    for code, row in table.iterrows():
        counts = r3.loc[r3["cell"] == code, "name"].str.upper().value_counts()
        assert len(counts), f"{code}: no resident in round 3 supports this row"
        assert row["District"] == counts.index[0], code
        assert len(counts) < 2 or counts.iloc[0] > counts.iloc[1], f"{code}: tied"
        n, big_n = re.fullmatch(r"R3 ha_02_2 (\d+)/(\d+)", row["Source"]).groups()
        assert (int(n), int(big_n)) == (counts.iloc[0], counts.sum()), code


@pytest.mark.requires_s3
def test_every_early_cluster_has_a_row(table, upd):
    missing = set(upd.loc[upd["round"].isin(EARLY), "cell"]) - set(table.index)
    assert not missing


@pytest.mark.requires_s3
def test_round_4_is_a_different_vintage(table, upd, tzmod):
    """Why the table stops at round 3: the same number names another district."""
    r4 = upd[upd["round"] == 4].copy()
    own = r4["region"].map(lambda r: tzmod._REGION_CODE_BY_NAME[tzmod.normalize_place(r)])
    r4["own_cell"] = own.astype(int) * 10 + r4["code"].astype(int)
    plural = r4.groupby("own_cell")["name"].agg(lambda s: s.str.upper().value_counts().index[0])
    assert table.loc[113, "District"] == "MAKETE"
    assert plural[113] != "MAKETE"
    assert (plural.reindex(table.index).dropna() != table["District"].reindex(plural.index).dropna()).any()


@pytest.mark.requires_s3
def test_y5_column_is_the_stata_value_label(table, tzmod):
    path = countries_root() / "Tanzania" / "2020-21" / "Data" / "hh_sec_a.dta"
    try:
        cats = get_dataframe(path, convert_categoricals=True, categories_only=True)["hh_a02_1"]
    except Exception as exc:  # noqa: BLE001 - any data failure -> skip
        pytest.skip(f"hh_sec_a.dta unavailable: {exc}")
    labels = {int(k): str(v).strip().upper() for k, v in cats.items()}
    for code, row in table.iterrows():
        got = row["Y5 label"]
        assert (pd.isna(got) and code not in labels) or got == labels[code], code
        if row["Y5 check"] == "same":
            assert tzmod.normalize_place(row["District"]) == tzmod.normalize_place(got), code


# --------------------------------------------------------------------------
# Served tier
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tz():
    return ll.Country("Tanzania")


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
def cluster_features(tz):
    return _build(tz.cluster_features).reset_index().astype({"t": str, "v": str})


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", EARLY.values())
def test_early_label_is_the_table_label_of_the_cluster_id(cluster_features, table, tzmod, wave):
    sub = cluster_features[(cluster_features["t"] == wave) & cluster_features["District"].notna()]
    assert len(sub)
    for v, label in zip(sub["v"], sub["District"]):
        cell = int(tzmod.cluster_region_code(v, "nps")) * 10 + int(tzmod.cluster_district_code(v, "nps"))
        assert tzmod.normalize_place(label) == tzmod.normalize_place(table.loc[cell, "District"]), v


@pytest.mark.requires_s3
@pytest.mark.parametrize("rnd, wave", EARLY.items())
def test_early_cluster_is_unlabelled_only_without_a_confirming_resident(cluster_features, upd, rnd, wave):
    """A cluster with no household whose code its own id confirms stays NA; every
    other cluster is labelled.  The oracle comes from the source, not a count."""
    src = upd[upd["round"] == rnd]
    confirmed = set(src.loc[src["resident"], "cid"])
    served = cluster_features[cluster_features["t"] == wave].set_index("v")["District"]
    assert set(served.index) == set(src["cid"])
    assert set(served[served.isna()].index) == set(served.index) - confirmed


@pytest.mark.requires_s3
@pytest.mark.parametrize("rnd, wave", [(3, "2012-13"), (4, "2014-15")])
def test_later_rounds_keep_their_own_answers(cluster_features, upd, rnd, wave):
    """The fill touches rounds 1-2 only: a served later name is that cluster's
    own answer, whatever the table says about the same number."""
    # Letters only: the country `District` table respells some answers on read
    # (WANG'ING'OMBE -> Wanging'ombe).
    def letters(x):
        return re.sub(r"[^A-Z0-9]", "", str(x).upper())

    src = upd[upd["round"] == rnd]
    answers = src.groupby("cid")["name"].agg(lambda s: set(s.map(letters)))
    sub = cluster_features[cluster_features["t"] == wave]
    for v, label in zip(sub["v"], sub["District"]):
        assert letters(label) in answers[v], v


@pytest.mark.requires_s3
def test_market_district_labels_the_early_rounds(tz, cluster_features):
    """The households left with `m` NA in rounds 1-2 are those of unlabelled
    clusters, and no household is dropped.  `food_acquired` joins the same way."""
    sample = _build(tz.sample).reset_index().astype({"t": str, "v": "string"})
    market = _build(lambda: tz.sample(market="District")).reset_index().astype({"t": str})
    assert len(market) == len(sample)
    no_label = cluster_features.loc[cluster_features["District"].isna(), ["t", "v"]]
    for wave in EARLY.values():
        expected = sample[(sample["t"] == wave)
                          & sample["v"].isin(no_label.loc[no_label["t"] == wave, "v"])]
        got = market[(market["t"] == wave) & market["m"].isna()]
        assert set(got["i"]) == set(expected["i"]), wave
