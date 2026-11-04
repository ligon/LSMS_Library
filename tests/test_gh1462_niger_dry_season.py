"""Niger: the 2011-12 dry-season harvest and ``Condition_sold`` -- GH #1462.

Ruling @ligon 2026-11-04.  ``ecvmaas3e_p1`` (section 3E) is a crop line with no
field or parcel, so it is served on ``plot_id = 'Farm-level'``, with ``Quantity =
as03eq09a x as03eq06`` under a registered key, on the Mali tree precedent, and the
sale from ``as03eq11a/b`` and ``as03eq12``.  2014-15 has no dry-season harvest.

``Condition_sold`` is the state of what was sold (2018-19 ``s16cq16c``, 2021-22
``s16dq05d``), through the harvest level's fold, and NA rather than a sentinel.

Two tiers: config and synthetic (no microdata), and ``TestServed``, whose oracle
reads the sources through ``get_dataframe`` and imports nothing from ``Niger/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.derivations import MULTI_SEP, records_for, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "crop_production"
WAVE = "2011-12"
TOTAL = "Niger::crop_production::dry-season-cycle-total"
SALE = "Niger::crop_production::dry-season-sale-window"
DRY = "NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta"
#: The state fold, written out: the oracle must not read the table it checks.
EHCVM1 = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled",
          5: "shell_not_applicable"}
EHCVM2 = {1: "unshelled", 2: "shelled", 3: "shell_not_applicable"}
#: 3E's unit codes, from the questionnaire's own list, and the 99 'manquant'.
UNITS = {1: "Kg", 3: "Cope", 4: "Botte", 5: "Tiya", 6: "Tongolo", 7: "Sac de 50 kg",
         8: "Sac de 100 kg", 9: "Panier", 10: "Unité", 11: "Planche", 99: "Unknown"}


@pytest.fixture(scope="module")
def niger():
    """``Niger/_/niger.py``, imported as the wave scripts do; no data at import."""
    import sys
    sys.path.insert(0, str(countries_root() / COUNTRY / "_"))
    try:
        import niger as module
    finally:
        sys.path.pop(0)
    return module


class TestRule:
    @pytest.mark.parametrize("per_cycle, cycles, expected", [
        (10.0, 1, 10.0), (10.0, 3, 30.0), (0.0, 2, 0.0), (0.0, np.nan, 0.0),
        (np.nan, 2, np.nan), (10.0, np.nan, np.nan), (10.0, 0, np.nan),
    ])
    def test_cycle_total_edges(self, niger, per_cycle, cycles, expected):
        got = niger.derive_dry_season_cycle_total(pd.Series([per_cycle]), pd.Series([cycles]))
        assert got.dtype == "float64"
        if np.isnan(expected):
            assert np.isnan(got.iloc[0])
        else:
            assert got.iloc[0] == expected

    def test_the_sale_is_the_identity(self, niger):
        got = niger.derive_dry_season_sale_window(pd.Series([3.0, 0.0, np.nan]),
                                                  pd.Series([900.0, np.nan, 5.0]))
        assert got["Quantity_sold"].tolist()[:2] == [3.0, 0.0]
        assert got["Value_sold"].iloc[0] == 900.0 and got["Value_sold"].iloc[2] == 5.0

    def test_the_stamp(self, niger):
        idx = pd.MultiIndex.from_tuples(
            [("1_1", "a"), (niger.FARM_LEVEL, "b"), (niger.FARM_LEVEL, "c"),
             (niger.FARM_LEVEL, "d"), ("1_2", "e")], names=["plot_id", "x"])
        df = pd.DataFrame({
            "Quantity_sold": pd.array([5.0, 0.0, None, 2.0, 5.0], dtype="Float64"),
            "Value_sold": pd.array([5.0, 0.0, 9.0, None, None], dtype="Float64"),
        }, index=idx)
        got = niger.stamp_dry_season_derivations(df)
        both = MULTI_SEP.join(sorted((TOTAL, SALE)))
        assert got.isna().tolist() == [True, False, False, False, True]
        assert got.iloc[1:4].tolist() == [TOTAL, both, both]

    def test_a_second_module_has_its_own_line_tokens(self, niger):
        households = pd.Series(["1", "1"])
        lines = pd.Series([1, 2])
        assert niger._crop_reported_observations(households, lines).tolist() == \
            ["reported:1", "reported:2"]
        assert niger._crop_reported_observations(households, lines, module="3E").tolist() == \
            ["reported:3E:1", "reported:3E:2"]

    def test_a_blank_sold_state_stays_na(self, niger, monkeypatch):
        monkeypatch.chdir(countries_root() / COUNTRY / "_")
        got = niger._crop_sold_conditions(pd.Series([1, 2, 3, 4, 5, np.nan]),
                                          "harmonize_crop_condition_ehcvm1")
        assert got.iloc[:5].tolist() == [EHCVM1[c] for c in range(1, 6)]
        assert pd.isna(got.iloc[5])
        got = niger._crop_sold_conditions(pd.Series([1, 2, 3, np.nan]),
                                          "harmonize_crop_condition_ehcvm2")
        assert got.iloc[:3].tolist() == [EHCVM2[c] for c in range(1, 4)]
        assert pd.isna(got.iloc[3])

    def test_the_tail_keeps_the_column(self, niger):
        frame = pd.DataFrame({"i": ["1"], "plot_id": ["1_1"], "crop": ["Mil"], "u": ["Kg"],
                              "Quantity": [1.0], "Quantity_sold": [1.0], "Value_sold": [5.0],
                              "Condition_sold": ["shelled"]})
        assert niger._finish_crop_production(frame, WAVE)["Condition_sold"].tolist() == ["shelled"]
        frame = frame.drop(columns="Condition_sold")
        got = niger._finish_crop_production(frame, WAVE)["Condition_sold"]
        assert got.dtype == "string" and got.isna().all()


class TestDeclared:
    def test_condition_sold_is_optional_and_a_column(self):
        with open(countries_root() / COUNTRY / "_" / "data_scheme.yml", encoding="utf-8") as f:
            entry = yaml.safe_load(f.read().replace("!make", "make"))["Data Scheme"][TABLE]
        assert entry["Condition_sold"] == {"type": "str", "optional": True}
        assert "Condition_sold" not in entry["index"]

    @pytest.mark.parametrize("key, columns", [(TOTAL, ["Quantity"]),
                                              (SALE, ["Quantity_sold", "Value_sold"])])
    def test_the_keys_are_registered_for_the_one_wave(self, key, columns):
        record = records_for(COUNTRY, TABLE)[key]
        assert list(record["waves"]) == [WAVE]
        assert list(record["columns"]) == columns
        assert callable(resolve_callable(record["function"]))
        assert callable(resolve_callable(record["inputs"]))

    def test_the_multi_cycle_wording_is_a_choice_not_a_reading(self):
        # The Stata label says an average, the English instrument the first cycle.
        record = records_for(COUNTRY, TABLE)[TOTAL]
        first = record["assumptions"][0]
        assert first["basis"] == "modelling-choice"
        assert "first cycle" in first["text"] and "en moyenne" in first["text"]


def _read(wave, name, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(countries_root() / COUNTRY / wave / "Data" / name), **kw)


@pytest.fixture(scope="module")
def built():
    import lsms_library as ll
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            country = ll.Country(COUNTRY)
            df = country.crop_production()
    except Exception as e:  # pragma: no cover - data not available
        pytest.skip(f"{COUNTRY} {TABLE} not buildable here: {e}")
    return country, df


@pytest.fixture(scope="module")
def dry_source():
    """The raw 3E rows, with exact repeats gone and the one key they would break."""
    raw = _read(WAVE, DRY, convert_categoricals=False)
    raw = raw[~raw.duplicated()].copy()
    raw["i"] = raw["hid"].map(lambda x: str(int(x)))
    raw["production_obs"] = "reported:3E:" + raw["as03eq01"].astype(int).astype(str)
    return raw.set_index(["i", "production_obs"])


@pytest.mark.requires_s3
class TestServed:
    def _dry(self, built):
        _, df = built
        got = df.xs(WAVE, level="t").reset_index()
        return got[got["plot_id"] == "Farm-level"].set_index(["i", "production_obs"])

    def test_every_source_line_is_one_served_row(self, built, dry_source):
        got = self._dry(built)
        assert got.index.is_unique
        assert set(got.index) == set(dry_source.index)
        assert got.index.get_level_values("i").nunique() == 271

    def test_quantity_is_the_answer_times_the_cycles(self, built, dry_source):
        got = self._dry(built).astype({"Quantity": "float64"})["Quantity"]
        q = dry_source["as03eq09a"].astype("float64").where(lambda s: s != 99999)
        want = (q * dry_source["as03eq06"].astype("float64")).reindex(got.index)
        pd.testing.assert_series_equal(got, want, check_names=False, check_index=False)

    def test_the_sale_is_as_recorded(self, built, dry_source):
        got = self._dry(built)
        src = dry_source.reindex(got.index)
        pd.testing.assert_series_equal(got["Quantity_sold"].astype("float64"),
                                       src["as03eq11a"].astype("float64"),
                                       check_names=False, check_index=False)
        value = src["as03eq12"].astype("float64").where(lambda s: s != 9999999)
        pd.testing.assert_series_equal(got["Value_sold"].astype("float64"), value,
                                       check_names=False, check_index=False)

    def test_the_units_are_the_questionnaires(self, built, dry_source):
        got = self._dry(built)
        src = dry_source.reindex(got.index)
        harvest = src["as03eq09b"].map(UNITS).fillna("Unknown")
        assert (got["u"].astype(object) == harvest.astype(object)).all()
        sold = src["as03eq11b"].map(UNITS)
        assert (got["Unit_sold"].astype(object).fillna("-") == sold.astype(object).fillna("-")).all()

    def test_no_crop_passes_through_unlabelled(self, built, dry_source):
        # A label the table misses would serve raw and in lower case.
        crops = self._dry(built)["crop"]
        assert crops.notna().all() and (crops.str[0].str.isupper()).all()
        carrots = dry_source["as03eq03"].eq(30)
        assert int((crops == "Carotte").sum()) == int(carrots.sum()) > 0

    def test_the_key_is_on_every_dry_row_and_no_rainy_one(self, built):
        _, df = built
        got = df.reset_index()
        dry = got["plot_id"] == "Farm-level"
        assert set(got.loc[dry, "t"]) == {WAVE}
        assert got.loc[dry, "Derivation"].str.contains(TOTAL, regex=False).all()
        assert got.loc[~dry, "Derivation"].isna().all()
        sold = (got["Quantity_sold"].fillna(0) > 0) | (got["Value_sold"].fillna(0) > 0)
        carries = got["Derivation"].fillna("").str.contains(SALE, regex=False)
        assert (carries == (dry & sold)).all()

    @pytest.mark.parametrize("key", [TOTAL, SALE])
    def test_the_registered_inputs_join_to_the_served_rows(self, built, key):
        country, df = built
        raw = country.derivation_inputs(key).reset_index()
        got = df.xs(WAVE, level="t").reset_index()
        got = got[got["plot_id"] == "Farm-level"]
        on = ["i", "plot_id", "production_obs"]
        assert sorted(map(tuple, raw[on].to_numpy())) == sorted(map(tuple, got[on].to_numpy()))

    def test_the_rainy_module_is_still_its_own_lines(self, built):
        _, df = built
        got = df.xs(WAVE, level="t").reset_index()
        rainy = got[got["plot_id"] != "Farm-level"]
        assert rainy["production_obs"].str.fullmatch(r"reported:\d+").all()

    def test_2014_15_has_no_dry_season_row(self, built):
        # Not missing: ECVMA2_AS03P1 is agricultural equipment, not a harvest.
        _, df = built
        got = df.xs("2014-15", level="t").index.get_level_values("plot_id")
        assert "Farm-level" not in set(got)
        raw = _read("2014-15", "NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS03P1.dta",
                    convert_categoricals=False)
        assert not {"AS03EQ09A", "as03eq09a"} & set(raw.columns)

    def test_condition_sold_2018_19_is_the_source_code(self, built):
        _, df = built
        got = df.xs("2018-19", level="t").reset_index()
        raw = _read("2018-19", "s16c_me_ner2018.dta", convert_categoricals=False)
        want = pd.to_numeric(raw["s16cq16c"], errors="coerce").map(EHCVM1)
        assert got["Condition_sold"].value_counts().to_dict() == want.value_counts().to_dict()
        sold = got["Quantity_sold"].fillna(0) > 0
        assert (got["Condition_sold"].notna() == sold).all()

    def test_condition_sold_2021_22_is_the_sale_blocks_code(self, built):
        country, df = built
        got = df.xs("2021-22", level="t").reset_index()
        got = got[got["Condition_sold"].notna()]
        raw = _read("2021-22", "s16d_me_ner2021.dta", convert_categoricals=False)
        raw = raw[~raw.duplicated()]
        raw = raw[pd.to_numeric(raw["s16dq05a"], errors="coerce") > 0].copy()
        walk = country.updated_ids.get("2021-22", {}) if country.updated_ids else {}
        raw["i"] = [walk.get(x, x) for x in
                    ("E_" + str(int(g)) + "0" + f"{int(m):02d}"
                     for g, m in zip(raw["grappe"], raw["menage"]))]
        states = pd.to_numeric(raw["s16dq05d"]).map(EHCVM2)
        wanted = {}
        for key, state in zip(zip(raw["i"], raw["s16dq05a"].astype(float)), states):
            wanted.setdefault(key, set()).add(state)
        keys = zip(got["i"], got["Quantity_sold"].astype(float))
        assert all(state in wanted[key] for key, state in zip(keys, got["Condition_sold"]))

    def test_condition_sold_is_na_where_no_state_is_asked(self, built):
        _, df = built
        for wave in ("2011-12", "2014-15"):
            assert df.xs(wave, level="t")["Condition_sold"].isna().all()

    def test_declared_spellings_check_passes(self, built):
        from lsms_library.diagnostics import _check_declared_spellings
        _, df = built
        assert _check_declared_spellings(df, TABLE).status == "pass"
