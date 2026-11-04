"""Tanzania `District` in NPS rounds 1-2, from the district code (GH #1441).

The UPD ships the within-region code `ha_02_1` in every round and leaves the name
`ha_02_2` blank in rounds 1-2, so `market='District'` had no label for any of
those households.  `2008-15/_/categorical_mapping.org` table `upd_district_code`
gives each code the name round 3's residents write for it, and the rows filled
carry the registered key in `Derivation`.

Three tiers.  The config tier needs no data.  The source tier is the oracle: it
reads `upd4_hh_a.dta` directly and recomputes what the table claims, so neither a
re-release nor a hand edit can leave a row standing without its evidence.  The
served tier checks the rule through the Country API.

Data-dependent tests carry `requires_s3`, so they skip without credentials.  With
them, a build failure fails rather than skips.
"""
import importlib.util
import inspect
import re
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.derivations import call_inputs, records_for, resolve_callable
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

EARLY = {1: "2008-09", 2: "2010-11"}
TABLE = "upd_district_code"
KEY = "Tanzania::cluster_features::upd-district-name-from-round-3"
DIGIT_KEY = "Tanzania::cluster_features::upd-district-name-from-id-digit"
NAMED = {"4115002", "10506001"}


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


@pytest.fixture(scope="module")
def district():
    """`_/_upd_district.py`, loaded from its path as `derivations.yml` does."""
    path = countries_root() / "Tanzania" / "_" / "_upd_district.py"
    spec = importlib.util.spec_from_file_location("_tz_gh1441_district", path)
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
def test_labels_only_a_report_its_own_cluster_id_confirms(district, region, code, v, expected):
    df = _frame([("a", "2008-09", v, region, code)])
    got = district.upd_district_labels(df, codes={11: "KONDOA", 532: "URBAN"}).iloc[0]
    assert (pd.isna(got) and pd.isna(expected)) or got == expected


def test_derive_stamps_exactly_what_it_fills(district):
    rows = [
        ("filled", "2008-09", "1114002", "DODOMA", "1.0", pd.NA),
        ("unconfirmed", "2010-11", "1114002", "DODOMA", "2.0", pd.NA),
        ("recorded", "2008-09", "1114002", "DODOMA", "1.0", "OWN ANSWER"),
        ("later_answer", "2012-13", "1114002", "DODOMA", "1.0", "KONGWA"),
        ("later_blank", "2012-13", "1114002", "DODOMA", "1.0", pd.NA),
    ]
    df = pd.DataFrame(rows, columns=["i", "t", "v", "Region", "_district_code", "District"])
    out = district.derive_upd_district_name(df.set_index(["i", "t", "v"]),
                                            codes={11: "KONDOA"}).reset_index().set_index("i")
    assert out.loc["filled", "District"] == "KONDOA"
    assert out.loc["recorded", "District"] == "OWN ANSWER"
    assert out.loc["later_answer", "District"] == "KONGWA"
    assert pd.isna(out.loc["unconfirmed", "District"])
    assert pd.isna(out.loc["later_blank", "District"])
    assert out.loc["filled", "Derivation"] == KEY
    assert out["Derivation"].notna().sum() == 1


def test_digit_fill_touches_only_the_named_clusters_in_2010_11(district):
    rows = [
        ("named", "2010-11", "4115002", "TANGA", "2.0", pd.NA, pd.NA),
        ("first_round", "2008-09", "4115002", "TANGA", "1.0", "LUSHOTO", KEY),
        ("other_na", "2010-11", "19302076", "MWANZA", "8.0", pd.NA, pd.NA),
        ("named_answered", "2010-11", "10506001", "RUVUMA", "5.0", "OWN ANSWER", pd.NA),
    ]
    df = pd.DataFrame(rows, columns=["i", "t", "v", "Region", "_district_code", "District", "Derivation"])
    out = district.derive_upd_district_from_id_digit(
        df.set_index(["i", "t", "v"]),
        codes={41: "LUSHOTO", 105: "NAMTUMBO", 193: "NYAMAGANA"}).reset_index().set_index("i")
    assert out.loc["named", "District"] == "LUSHOTO"
    assert out.loc["named", "Derivation"] == DIGIT_KEY
    assert out.loc["first_round", "Derivation"] == KEY
    assert pd.isna(out.loc["other_na", "District"]) and pd.isna(out.loc["other_na", "Derivation"])
    assert out.loc["named_answered", "District"] == "OWN ANSWER"
    assert pd.isna(out.loc["named_answered", "Derivation"])


def test_derivation_is_registered_and_resolves():
    rec = records_for("Tanzania", "cluster_features")[KEY]
    assert list(rec["waves"]) == list(EARLY.values())
    assert resolve_callable(rec["function"]).__name__ == "derive_upd_district_name"
    params = inspect.signature(resolve_callable(rec["inputs"])).parameters
    assert "wave" not in params, "the names live in round 3, outside the entry's waves"


@pytest.fixture(scope="module")
def mapping_2019():
    """`2019-20/_/mapping.py`, loaded from its path as `derivations.yml` does."""
    path = countries_root() / "Tanzania" / "2019-20" / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location("_tz_gh1441_mapping", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _split_off_frame(rows):
    df = pd.DataFrame(rows, columns=["i", "v", "_frame", "_y4_hhid"])
    return df.assign(weight=1.0).set_index("i")


def test_split_off_takes_its_parents_cluster_only_when_its_id_is_stray(mapping_2019):
    out = mapping_2019.derive_split_off_parent_cluster(_split_off_frame([
        ("frame", "100", "0001", "A"),
        ("other_frame", "200", "0002", "B"),
        ("stray_with_parent", "900", "", "A"),
        ("stray_no_parent", "901", "", "Z"),
        ("id_is_a_frame_cluster", "200", "", "A"),
    ]))
    assert list(out.columns) == ["v", "weight", "Derivation"]
    assert out["v"].to_dict() == {"frame": "100", "other_frame": "200", "stray_with_parent": "100",
                                  "stray_no_parent": "901", "id_is_a_frame_cluster": "200"}
    stamped = out["Derivation"].dropna()
    assert stamped.index.tolist() == ["stray_with_parent"]
    assert stamped.iloc[0] == "Tanzania::sample::split-off-parent-cluster"


def test_guard_refuses_a_split_off_with_two_parent_clusters(mapping_2019):
    frame = _split_off_frame([
        ("frame_1", "100", "0001", "A"),
        ("frame_2", "200", "0002", "A"),
        ("stray", "900", "", "A"),
    ])
    with pytest.raises(mapping_2019.SplitOffClusterError):
        mapping_2019.derive_split_off_parent_cluster(frame)


def test_guard_does_not_fire_without_a_household_to_rekey(mapping_2019):
    out = mapping_2019.derive_split_off_parent_cluster(_split_off_frame([
        ("frame_1", "100", "0001", "A"),
        ("frame_2", "200", "0002", "A"),
        ("id_is_a_frame_cluster", "100", "", "A"),
    ]))
    assert out["Derivation"].isna().all()


def test_guard_error_is_none_of_the_types_the_build_paths_catch(mapping_2019):
    """Any of these would be swallowed, and every household table served no `v`."""
    caught = (FileNotFoundError, KeyError, ValueError, AttributeError, RuntimeError, OSError)
    assert not issubclass(mapping_2019.SplitOffClusterError, caught)


def test_the_v_join_does_not_swallow_the_guard(mapping_2019, monkeypatch):
    country = ll.Country("Tanzania", preload_panel_ids=False)

    def refuse(*args, **kwargs):
        raise mapping_2019.SplitOffClusterError("two parent clusters")

    monkeypatch.setattr(country, "sample", refuse, raising=False)
    frame = pd.DataFrame({"x": [1]}, index=pd.MultiIndex.from_tuples([("a", "2019-20")], names=["i", "t"]))
    with pytest.raises(mapping_2019.SplitOffClusterError):
        country._join_v_from_sample(frame)


# --------------------------------------------------------------------------
# Source tier: the oracle reads the .dta
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def upd_raw(tzmod):
    """`upd4_hh_a.dta` as shipped (panel lines), with the geocode."""
    path = countries_root() / "Tanzania" / "2008-15" / "Data" / "upd4_hh_a.dta"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        raw = get_dataframe(path)
    df = raw.copy()
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


@pytest.fixture(scope="module")
def upd(upd_raw):
    """One row per (household, round, cluster)."""
    return upd_raw.drop_duplicates(["r_hhid", "round", "clusterid"])


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


def _fold(name, tzmod):
    """Compare names across Swahili and English qualifiers."""
    same = {"MJINI": "URBAN", "MANISPAA": "URBAN", "JIJI": "URBAN", "MJI": "URBAN",
            "VIJIJINI": "RURAL", "V": "RURAL"}
    return " ".join(same.get(w, w) for w in tzmod.normalize_place(name).split())


@pytest.mark.requires_s3
def test_clusters_column_follows_panel_lines_into_round_3(table, upd_raw, tzmod):
    """`k/K`: of K clusters whose lines reach a round-3 resident, k have the
    row's name as their plurality.  Recomputed through UPHI, not through the
    (region, code) cell the name came from."""
    lines = upd_raw.drop_duplicates(["UPHI", "round"])
    early = lines.loc[lines["round"].isin(EARLY), ["UPHI", "cid", "cell"]].drop_duplicates()
    late = lines.loc[(lines["round"] == 3) & lines["resident"], ["UPHI", "r_hhid", "name"]]
    bridged = early.merge(late, on="UPHI").drop_duplicates(["cid", "r_hhid"])
    plural = (bridged.groupby(["cell", "cid"])["name"]
              .agg(lambda s: s.str.upper().value_counts().index[0]).reset_index())
    for code, row in table.iterrows():
        sub = plural[plural["cell"] == code]
        k = sum(_fold(n, tzmod) == _fold(row["District"], tzmod) for n in sub["name"])
        assert row["Clusters"] == f"{k}/{len(sub)}", code


@pytest.mark.requires_s3
def test_every_early_cluster_has_a_row(table, upd):
    missing = set(upd.loc[upd["round"].isin(EARLY), "cell"]) - set(table.index)
    assert not missing


@pytest.mark.requires_s3
def test_round_4_is_a_different_vintage(table, upd, tzmod, district):
    """Why the table stops at round 3: the same number names another district."""
    r4 = upd[upd["round"] == 4].copy()
    own = r4["region"].map(lambda r: district._REGION_CODE_BY_NAME[tzmod.normalize_place(r)])
    r4["own_cell"] = own.astype(int) * 10 + r4["code"].astype(int)
    plural = r4.groupby("own_cell")["name"].agg(lambda s: s.str.upper().value_counts().index[0])
    assert table.loc[113, "District"] == "MAKETE"
    assert plural[113] != "MAKETE"
    assert (plural.reindex(table.index).dropna() != table["District"].reindex(plural.index).dropna()).any()


@pytest.mark.requires_s3
def test_y5_column_is_the_stata_value_label(table, tzmod):
    path = countries_root() / "Tanzania" / "2020-21" / "Data" / "hh_sec_a.dta"
    cats = get_dataframe(path, convert_categoricals=True, categories_only=True)["hh_a02_1"]
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
    """Lets a build failure fail: `requires_s3` already skips without data."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = fn()
    assert df is not None and not df.empty
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
    """A cluster with no household whose code its own id confirms stays NA, unless
    it is one of the two named clusters; every other cluster is labelled.  The
    oracle comes from the source, not a count."""
    src = upd[upd["round"] == rnd]
    confirmed = set(src.loc[src["resident"], "cid"])
    served = cluster_features[cluster_features["t"] == wave].set_index("v")["District"]
    assert set(served.index) == set(src["cid"])
    named = NAMED if wave == "2010-11" else set()
    assert set(served[served.isna()].index) == set(served.index) - confirmed - named


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
def test_key_is_on_exactly_the_rows_the_fill_labels(tz, cluster_features):
    early = cluster_features["t"].isin(EARLY.values())
    named = cluster_features["t"].eq("2010-11") & cluster_features["v"].isin(NAMED)
    stamped = cluster_features["Derivation"].eq(KEY).fillna(False).astype(bool)
    by_digit = cluster_features["Derivation"].eq(DIGIT_KEY).fillna(False).astype(bool)
    assert (stamped == (early & ~named & cluster_features["District"].notna())).all()
    assert (by_digit == (named & cluster_features["District"].notna())).all()
    assert int(by_digit.sum()) == len(NAMED)
    assert cluster_features["Derivation"].dropna().isin([KEY, DIGIT_KEY]).all()
    summary = _build(tz.cluster_features).attrs["derivations"]["Tanzania"]
    assert summary[KEY]["rows"] == int(stamped.sum())
    assert summary[DIGIT_KEY]["rows"] == int(by_digit.sum())


@pytest.mark.requires_s3
def test_inputs_join_every_stamped_row(cluster_features):
    rec = records_for("Tanzania", "cluster_features")[KEY]
    inputs = call_inputs(rec)
    assert set(rec["raw_variables"]) <= set(inputs.columns) | set(inputs.index.names)
    keys = set(zip(inputs.index.get_level_values("t").astype(str),
                   inputs.index.get_level_values("v").astype(str)))
    stamped = cluster_features[cluster_features["Derivation"].eq(KEY).fillna(False).astype(bool)]
    assert len(stamped)
    assert set(zip(stamped["t"], stamped["v"])) <= keys
    # The names the key rests on are in the frame, in round 3.
    round_3 = inputs.xs("2012-13", level="t")
    assert round_3["ha_02_2"].astype("string").str.strip().ne("").any()


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
