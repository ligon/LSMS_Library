"""GH #689: Uganda's design PSU ``ea`` on ``sample()``, beside the parish ``v``.

From 2013-14 Uganda's ``v`` is a parish, not the unit the survey sampled.  The
ruling (@ligon, 2026-11-03) keeps ``v`` and ``cluster_features`` exactly as they
are and adds a column, ``ea``, the enumeration area a household was SAMPLED in,
carried with it when it moves:

* 2013-14, 2015-16: direct from ``GSEC1.ea``.
* 2018-19: the 2015-16 ``ea`` of the household ``GSEC1.t0_hhid`` names.
* 2019-20: the same, through ``gsec1.hhidold`` -> 2018-19 ``GSEC1.hhid``.
* No route: NA.  2005-06 to 2011-12: NA here (``v`` is the EA there).

The oracle reads the source files directly and imports nothing from
``Uganda/_/``, so it cannot agree with the implementation by sharing its code.
Config tests need no data; the ``requires_s3`` tests read covers and the build.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import _collect_file_paths_from_block, format_id, get_dataframe
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

UGANDA = countries_root() / "Uganda"
KEY = "Uganda::sample::ea-tracking-key"
DIRECT_WAVES = ("2013-14", "2015-16")
INHERITED_WAVES = ("2018-19", "2019-20")
EARLY_WAVES = ("2005-06", "2009-10", "2010-11", "2011-12")
#: Non-null ``ea``, distinct labels and rows, measured on the source files.
SERVED = {"2013-14": (3119, 317, 3119), "2015-16": (3305, 316, 3377),
          "2018-19": (2978, 315, 3242), "2019-20": (2823, 315, 3098)}


def _info(wave):
    return load_yaml(UGANDA / wave / "_" / "data_info.yml")


def _cover(wave, name):
    path = UGANDA / wave / "Data" / name
    return get_dataframe(str(path), convert_categoricals=False)


# ---------------------------------------------------------- the declarations --

class TestDeclarations:
    """No data: what the config says, including what it must NOT say."""

    def test_ea_is_declared_optional_for_uganda(self):
        scheme = load_yaml(UGANDA / "_" / "data_scheme.yml")["Data Scheme"]["sample"]
        assert scheme["ea"] == {"type": "str", "optional": True}

    def test_ea_and_derivation_are_canonical_optional_columns(self):
        canon = load_yaml(countries_root().parent / "data_info.yml")["Columns"]["sample"]
        for col in ("ea", "Derivation"):
            assert canon[col]["type"] == "str" and canon[col]["optional"] is True

    @pytest.mark.parametrize("wave, sources", [
        ("2018-19", {"GSEC1.dta", "../../2015-16/Data/gsec1.dta"}),
        ("2019-20", {"HH/gsec1.dta", "../../2015-16/Data/gsec1.dta",
                     "../../2018-19/Data/GSEC1.dta"}),
    ])
    def test_every_file_the_chain_reads_is_a_declared_source(self, wave, sources):
        """Or the wave's L2 parquet would not notice the source changing."""
        declared = set(_collect_file_paths_from_block(_info(wave)["sample"]))
        assert sources <= declared

    def test_the_declared_bridge_is_not_a_merged_sub_df(self):
        block = _info("2019-20")["sample"]
        assert "origin_ea_bridge" in block and "origin_ea_bridge" not in block["dfs"]

    @pytest.mark.parametrize("wave, v", [
        ("2013-14", "h1aq4a"), ("2015-16", "parish_code"),
        ("2018-19", ["distirct_name", "parish_name"]),
        ("2019-20", ["district", "s1aq04a"]),
    ])
    def test_v_is_still_the_parish_key(self, wave, v):
        assert _info(wave)["sample"]["locating_cover"]["myvars"]["v"] == v

    @pytest.mark.parametrize("wave", DIRECT_WAVES + INHERITED_WAVES)
    def test_cluster_features_does_not_read_ea(self, wave):
        text = repr(_info(wave)["cluster_features"])
        assert "'ea'" not in text

    @pytest.mark.parametrize("wave", EARLY_WAVES)
    def test_early_waves_declare_no_ea(self, wave):
        assert "'ea'" not in repr(_info(wave)["sample"])

    def test_the_derivation_is_registered_for_the_inherited_waves_only(self):
        from lsms_library.derivations import records_for
        rec = records_for("Uganda", "sample")[KEY]
        assert tuple(rec.waves) == INHERITED_WAVES
        assert tuple(rec.columns) == ("ea",)
        assert {"hhid", "t0_hhid", "hhidold", "HHID", "ea"} <= set(rec.raw_variables)


class TestAddDerivation:
    """No row carries two keys today, so the data cannot exercise this branch."""

    def test_a_row_already_carrying_a_key_carries_both(self):
        import lsms_library as ll
        add = ll.Country("Uganda", preload_panel_ids=False)["2018-19"] \
            .formatting_functions["_add_derivation"]
        flat = pd.DataFrame({"Derivation": pd.array(["x::y::z", pd.NA, "x::y::z"],
                                                    dtype="string")})
        got = add(flat, pd.Series([True, True, False]), KEY)["Derivation"]
        assert got[0] == f"{KEY}+x::y::z"
        assert got[1] == KEY
        assert got[2] == "x::y::z"


# ------------------------------------------------------------------ the oracle --

@pytest.fixture(scope="module")
def covers():
    return {"2013-14": _cover("2013-14", "GSEC1.dta"),
            "2015-16": _cover("2015-16", "gsec1.dta"),
            "2018-19": _cover("2018-19", "GSEC1.dta"),
            "2019-20": _cover("2019-20", "HH/gsec1.dta")}


def _label(x):
    """The served spelling of a raw int16 ``ea``."""
    return str(int(x))


@pytest.fixture(scope="module")
def oracle(covers):
    """``{wave: {raw household id: ea}}``: the rule, from the source files."""
    origin = dict(zip(covers["2015-16"]["HHID"], covers["2015-16"]["ea"]))
    t0 = dict(zip(covers["2018-19"]["hhid"], covers["2018-19"]["t0_hhid"]))
    out = {"2013-14": {h: _label(e) for h, e in zip(covers["2013-14"]["HHID"],
                                                    covers["2013-14"]["ea"])},
           "2015-16": {h: _label(e) for h, e in origin.items()},
           "2018-19": {}, "2019-20": {}}
    for hhid, parent in t0.items():
        if parent in origin:
            out["2018-19"][hhid] = _label(origin[parent])
    for hhid, old in zip(covers["2019-20"]["hhid"], covers["2019-20"]["hhidold"]):
        parent = t0.get(old)
        if parent in origin:
            out["2019-20"][hhid] = _label(origin[parent])
    return out


@pytest.fixture(scope="module")
def uga():
    import lsms_library as ll
    return ll.Country("Uganda")


@pytest.fixture(scope="module")
def sample(uga):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return uga.sample()


@pytest.fixture(scope="module")
def served(uga, sample):
    """``{wave: Series of served ea, keyed on the RAW household id}``."""
    out = {}
    for wave in DIRECT_WAVES + INHERITED_WAVES:
        walked = dict(uga.updated_ids.get(wave, {}))
        frame = sample.xs(wave, level="t")
        raw = {walked.get(h, h): h for h in _ids(wave)}
        # A household only the agriculture cover records has no walk to undo.
        out[wave] = frame.assign(raw=[raw.get(i, i) for i in frame.index]) \
            .reset_index(drop=True).set_index("raw")
    return out


def _ids(wave):
    name, col = {"2013-14": ("GSEC1.dta", "HHID"), "2015-16": ("gsec1.dta", "HHID"),
                 "2018-19": ("GSEC1.dta", "hhid"), "2019-20": ("HH/gsec1.dta", "hhid")}[wave]
    return _cover(wave, name)[col].map(format_id).tolist()


# --------------------------------------------------------- UBOS's id encoding --

@pytest.mark.requires_s3
class TestUbosWritesEaIntoItsIds:
    """A pin of the encoding, and NOT evidence for the inheritance.

    Characters 2-4 of the id are ``ea`` and the link is string identity between
    ids, so linked households share ``ea`` by construction: a wrong label or a
    wrong tracking key would pass.  ``TestEvidenceOutsideTheId`` is the evidence.
    """

    def test_every_linked_household_keeps_its_ea(self, covers):
        """2015-16 ``hh`` is the 2013-14 ``HHID`` with its wave token moved on."""
        new, old = covers["2015-16"], covers["2013-14"]
        linked = new.assign(link=new["hh"].str.replace("-05-", "-04-", regex=False)) \
            .merge(old[["HHID", "ea"]], left_on="link", right_on="HHID",
                   suffixes=("", "_2013"))
        assert len(linked) == 2872
        assert (linked["ea"] == linked["ea_2013"]).all()
        assert (linked.groupby("ea")["ea_2013"].nunique() == 1).all()
        assert (linked.groupby("ea_2013")["ea"].nunique() == 1).all()

    @pytest.mark.parametrize("wave, col", [("2013-14", "HHID"), ("2015-16", "HHID")])
    def test_the_household_id_itself_embeds_ea(self, covers, wave, col):
        frame = covers[wave]
        assert (frame[col].str[1:4].astype(int) == frame["ea"]).all()


@pytest.mark.requires_s3
class TestEvidenceOutsideTheId:
    """Checks that do not read the id's digits, so a wrong key could fail them."""

    def test_ea_pairs_one_to_one_with_the_2011_12_frame_ea(self, covers):
        """``comm`` is another wave's column in another numbering."""
        old = _cover("2011-12", "GSEC1.dta")
        new = covers["2013-14"]
        linked = new.assign(key=new["HHID_old"].map(format_id)).dropna(subset=["key"]) \
            .merge(old.assign(key=old["HHID"].map(format_id))[["key", "comm"]], on="key")
        assert len(linked) == 1717
        assert len(linked[["ea", "comm"]].drop_duplicates()) == 212
        assert (linked.groupby("ea")["comm"].nunique() == 1).all()
        assert (linked.groupby("comm")["ea"].nunique() == 1).all()

    def test_t0_hhid_names_a_household_in_the_same_district(self, covers):
        """The key the rule follows, against geography the 2018-19 id does not hold."""
        linked = covers["2018-19"].merge(covers["2015-16"][["HHID", "district_name"]],
                                         left_on="t0_hhid", right_on="HHID")
        child = linked["distirct_name"].astype(str).str.strip().str.upper()
        parent = linked["district_name"].astype(str).str.strip().str.upper()
        assert len(linked) == 2978
        assert (child == parent).sum() == 2729


# ------------------------------------------------------------------ the column --

@pytest.mark.requires_s3
class TestServedAgainstTheSourceFiles:

    def test_counts(self, sample):
        for wave, (filled, labels, rows) in SERVED.items():
            ea = sample.xs(wave, level="t")["ea"]
            assert (ea.notna().sum(), ea.nunique(), len(ea)) == (filled, labels, rows), wave

    @pytest.mark.parametrize("wave", EARLY_WAVES)
    def test_early_waves_stay_na(self, sample, wave):
        assert sample.xs(wave, level="t")["ea"].isna().all()

    @pytest.mark.parametrize("wave", DIRECT_WAVES + INHERITED_WAVES)
    def test_served_equals_the_rule(self, served, oracle, wave):
        """Both directions: a value where the rule gives one, NA everywhere else."""
        got = served[wave]["ea"].dropna().to_dict()
        assert got == oracle[wave]

    def test_a_t0_hhid_in_no_gsec1_is_na_even_where_the_id_would_parse(self, served, covers):
        """Nothing is guessed: not from the id's digits, nor from AGSEC1."""
        frame = covers["2018-19"]
        no_route = frame.loc[~frame["t0_hhid"].isin(covers["2015-16"]["HHID"]), "hhid"]
        assert len(no_route) == 198
        assert served["2018-19"].loc[no_route.map(format_id), "ea"].isna().all()

    def test_a_blank_hhidold_is_not_a_link(self, served, covers):
        frame = covers["2019-20"]
        blank = frame.loc[frame["hhidold"].str.strip() == "", "hhid"]
        assert len(blank) == 69
        assert served["2019-20"].loc[blank.map(format_id), "ea"].isna().all()

    def test_two_children_of_one_parent_both_inherit(self, served, covers):
        frame = covers["2018-19"]
        routed = frame.loc[frame["t0_hhid"].isin(covers["2015-16"]["HHID"])]
        twins = routed.loc[routed["t0_hhid"].duplicated(keep=False), "hhid"].map(format_id)
        assert len(twins) == 10
        assert served["2018-19"].loc[twins, "ea"].notna().all()

    def test_the_served_columns_are_exactly_the_declared_ones(self, sample):
        """``origin_ea``'s marker is build plumbing and must not be served."""
        # District: the household's own recorded label, added by GH #1441.
        assert set(sample.columns) == {"v", "weight", "panel_weight", "strata", "Rural",
                                       "Region", "District", "Derivation", "ea"}

    def test_no_float_spelling(self, sample):
        assert isinstance(sample["ea"].dtype, pd.StringDtype)
        assert sample["ea"].dropna().str.fullmatch(r"[1-9]\d*").all()

    def test_ea_is_not_v(self, sample):
        """The two keys are different things: a parish code is never an EA."""
        for wave in DIRECT_WAVES:
            frame = sample.xs(wave, level="t").dropna(subset=["ea"])
            assert (frame["ea"] != frame["v"]).all(), wave

    @pytest.mark.parametrize("wave, col", [("2013-14", "h1aq4a"), ("2015-16", "parish_code")])
    def test_v_is_untouched_where_it_is_a_code(self, served, covers, wave, col):
        frame = covers[wave]
        expected = dict(zip(frame["HHID"].map(format_id), frame[col].map(format_id)))
        got = served[wave]["v"]
        located = {h: v for h, v in expected.items() if pd.notna(v) and h in got.index}
        assert got.loc[list(located)].to_dict() == located

    def test_v_is_untouched_where_it_is_a_composite(self, served, covers):
        frame = covers["2018-19"]
        expected = (frame["distirct_name"].map(format_id).str.strip().str.upper() + "/"
                    + frame["parish_name"].map(format_id).str.strip().str.upper())
        expected.index = frame["hhid"].map(format_id)
        got = served["2018-19"]["v"].loc[expected.index]
        assert got.to_dict() == expected.to_dict()


@pytest.mark.requires_s3
class TestDerivation:

    def test_stamped_on_exactly_the_inherited_rows(self, served, oracle):
        for wave in INHERITED_WAVES:
            frame = served[wave]
            stamped = frame["Derivation"].astype("string").str.contains(KEY, regex=False)
            assert set(frame.index[stamped.fillna(False)]) == set(oracle[wave]), wave

    def test_never_stamped_on_a_direct_row(self, sample):
        for wave in DIRECT_WAVES:
            der = sample.xs(wave, level="t")["Derivation"].astype("string")
            assert not der.str.contains(KEY, regex=False).fillna(False).any(), wave

    def test_inputs_join_to_the_served_rows(self, uga, sample):
        for wave in INHERITED_WAVES:
            inputs = uga.derivation_inputs(KEY, wave=wave)
            rows = sample.xs(wave, level="t", drop_level=False)
            stamped = rows.index[rows["Derivation"].astype("string")
                                 .str.contains(KEY, regex=False).fillna(False)]
            joined = inputs.reset_index().merge(stamped.to_frame(index=False),
                                                on=["t", "i"], how="outer", indicator=True)
            assert (joined["_merge"] == "both").all(), wave
            assert len(joined) == len(stamped) == len(inputs), wave
            assert {"hhid", "t0_hhid", "HHID", "ea"} <= set(inputs.columns)

    def test_the_function_reproduces_the_served_value(self, uga, sample):
        rec = uga.derivations()[KEY]
        for wave in INHERITED_WAVES:
            inputs = uga.derivation_inputs(KEY, wave=wave)
            derived = rec.resolve()(inputs).astype("string")
            got = sample.xs(wave, level="t")["ea"]
            assert derived.droplevel("t").sort_index().to_dict() \
                == got.loc[derived.index.get_level_values("i")].sort_index().to_dict(), wave


@pytest.mark.requires_s3
class TestNothingElseMoves:

    def test_the_v_join_carries_v_alone(self, uga):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            roster = uga.household_roster(waves=["2018-19"])
        assert "ea" not in roster.columns
        if "Derivation" in roster.columns:
            assert not roster["Derivation"].astype("string").str.contains(
                KEY, regex=False).fillna(False).any()

    def test_cluster_features_has_no_ea(self, uga):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = uga.cluster_features()
        assert "ea" not in frame.columns and "ea" not in frame.index.names

    def test_weights_still_normalise_to_mean_one(self, sample):
        means = sample.groupby(level="t")["weight"].mean()
        assert ((means - 1).abs() < 1e-9).all(), means


@pytest.mark.requires_s3
class TestADropTheHandlerCatchesCostsEaAlone:
    """Why ``origin_ea`` is a sub-df: a ``FileNotFoundError`` does not cost ``v``.

    That holds only for what the GH #515 handler catches.  With the remote
    unreachable ``EndpointConnectionError`` escapes and ``sample`` raises, ``v``
    included (GH #1463), which this simulation does not exercise.
    """

    @pytest.mark.parametrize("wave, rows", [("2018-19", 3242), ("2019-20", 3098)])
    def test_the_wave_still_builds(self, tmp_path, monkeypatch, wave, rows):
        import lsms_library.country as C
        import lsms_library.local_tools as LT
        import lsms_library.paths as P

        blobs = P.data_root() / "dvc-cache"
        (tmp_path / "dvc-cache").symlink_to(blobs)
        monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
        P.data_root.cache_clear()
        real = LT.get_dataframe

        def unavailable(fn, *a, **k):
            if "2015-16/Data/gsec1.dta" in str(fn):
                raise FileNotFoundError(f"simulated fetch failure: {fn}")
            return real(fn, *a, **k)

        monkeypatch.setattr(LT, "get_dataframe", unavailable)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                frame = C.Country("Uganda", preload_panel_ids=False)[wave].grab_data("sample")
        finally:
            P.data_root.cache_clear()
            # The simulated drop is filed in process globals; under loadfile the
            # worker goes on to other files, which must not inherit it.
            for ledger in (C._DEGRADED_ATTEMPTED, C._SOURCE_PROBES,
                           C._BUILD_DROP_LEDGER, C._REBUILD_HISTORY):
                ledger.clear()
        assert any("origin_ea" in str(w.message) for w in caught)
        assert len(frame) == rows
        assert frame["v"].notna().all()
        assert "ea" not in frame.columns
        assert not frame["Derivation"].astype("string").str.contains(
            KEY, regex=False).fillna(False).any()
