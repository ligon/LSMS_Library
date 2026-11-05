"""GH #1436, option (A): Niger's own harvest kilograms on ``crop_production``.

``Quantity_kg`` is a total for the line, in three waves of four: 2011-12 ``as02eq07c``
and, for section 3E, ``as03eq09c`` x ``as03eq06``; 2014-15 ``AS02EQ07C``; 2021-22
``s16cq16c``, an "Estimation" that carries a registered key.  2018-19's ``s16cq16c`` is the
sold state.  ``TestRules`` and ``TestDeclared`` need no microdata.  ``TestServed`` does
(``requires_s3``); its oracle reads the ``.dta`` files and imports nothing from ``Niger/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import records_for, resolve_callable
from lsms_library.paths import countries_root
from lsms_library.transformations import KG_FACTOR_LAYERS, harvest_kg, harvest_kg_factors
from lsms_library.yaml_utils import load_yaml

COUNTRY = "Niger"
TABLE = "crop_production"
ESTIMATE = "Niger::crop_production::survey-kg-estimate"
TOTAL = "Niger::crop_production::dry-season-cycle-total"
RAINY_2011 = "NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta"
DRY_2011 = "NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta"
FILE_2014 = "NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E1P2.dta"
FILE_2021 = "s16c_me_ner2021.dta"
ROOT = countries_root() / COUNTRY


@pytest.fixture(scope="module")
def niger():
    record = records_for(COUNTRY, TABLE)[ESTIMATE]
    resolve_callable(record["function"])
    import sys
    return sys.modules[resolve_callable(record["function"]).__module__]


class TestRules:
    def test_the_estimate_is_the_identity(self, niger):
        got = niger.derive_survey_kg_estimate(pd.Series([2.5, None, 0.0]))
        assert got.dtype == "float64"
        assert got.iloc[0] == 2.5 and np.isnan(got.iloc[1]) and got.iloc[2] == 0.0

    def test_the_stamp_is_where_a_value_is(self, niger):
        frame = pd.DataFrame({"Quantity_kg": pd.array([1.0, None, 0.0], dtype="Float64")})
        got = niger.stamp_survey_kg_estimate(frame)
        assert got.dtype == "string"
        assert got.tolist()[0] == ESTIMATE and pd.isna(got.tolist()[1]) and got.tolist()[2] == ESTIMATE

    def test_the_tail_keeps_the_column_and_supplies_it(self, niger):
        frame = pd.DataFrame({"i": ["1"], "plot_id": ["1_1"], "crop": ["Mil"], "u": ["Tiya"],
                              "Quantity": [4.0], "Quantity_sold": [1.0], "Value_sold": [5.0],
                              "Quantity_kg": [10.0]})
        got = niger._finish_crop_production(frame, "2011-12")["Quantity_kg"]
        assert got.dtype == "Float64" and got.tolist() == [10.0]
        got = niger._finish_crop_production(frame.drop(columns="Quantity_kg"), "2018-19")["Quantity_kg"]
        assert got.dtype == "Float64" and got.isna().all()

    def test_a_zero_beside_a_positive_harvest_is_no_answer(self, niger):
        frame = pd.DataFrame({"i": ["1", "2", "3"], "plot_id": ["1_1"] * 3, "crop": ["Mil"] * 3,
                              "u": ["Tiya"] * 3, "Quantity": [4.0, 0.0, None],
                              "Quantity_sold": [None] * 3, "Value_sold": [None] * 3,
                              "Quantity_kg": [0.0, 0.0, 0.0]})
        got = niger._finish_crop_production(frame, "2014-15").reset_index().set_index("i")["Quantity_kg"]
        assert pd.isna(got["1"])
        assert got["2"] == 0.0 and got["3"] == 0.0

    def test_a_kilogram_total_takes_the_cycle_product_and_keeps_its_zero(self, niger):
        got = niger.derive_dry_season_cycle_total(pd.Series([50.0, 0.0, None, 7.0]),
                                                  pd.Series([2, 3, 2, 0]))
        assert got.iloc[0] == 100.0 and got.iloc[1] == 0.0
        assert np.isnan(got.iloc[2]) and np.isnan(got.iloc[3])


class TestDeclared:
    def test_the_column_is_optional_and_never_an_index_level(self):
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert entry["Quantity_kg"] == {"type": "float", "optional": True}
        assert "Quantity_kg" not in entry["index"]

    def test_the_estimate_is_registered_for_its_wave(self):
        record = records_for(COUNTRY, TABLE)[ESTIMATE]
        assert list(record["waves"]) == ["2021-22"]
        assert list(record["columns"]) == ["Quantity_kg"]
        assert callable(resolve_callable(record["inputs"]))
        assert "s16cq16c" in record["raw_variables"]
        assert "Estimation" in str(record["assumptions"])

    def test_the_cycle_product_names_the_kilograms(self):
        record = records_for(COUNTRY, TABLE)[TOTAL]
        assert "Quantity_kg" in record["columns"]
        assert "as03eq09c" in record["raw_variables"]

    def test_no_wave_turns_the_total_into_a_factor(self):
        """The ruling: line totals are served as totals, never as ``KgFactor``."""
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert "KgFactor" not in entry
        for wave in ("2011-12", "2014-15", "2018-19", "2021-22"):
            assert "KgFactor" not in (ROOT / wave / "_" / "crop_production.py").read_text(encoding="utf-8")


def _read(wave, name, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / name), **kw)


def _numeric(frame, name):
    return pd.to_numeric(frame[name], errors="coerce").astype("float64")


def _id(series):
    return series.map(lambda x: str(int(x)) if pd.notna(x) else "")


def _plot(frame, field, parcel):
    return _id(frame[field]) + "_" + _id(frame[parcel])


@pytest.fixture(scope="module")
def built():
    # No skip on a failed build: TestServed is `requires_s3`.
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country(COUNTRY)
        df = country.crop_production()
    return country, df


def _wave(built, wave):
    return built[1].xs(wave, level="t").reset_index()


def _totals(served):
    return served.astype({"Quantity_kg": "float64"})


@pytest.mark.requires_s3
class TestServed:
    def test_2011_12_rainy_is_as02eq07c_with_its_code_masked(self, built):
        raw = _read("2011-12", RAINY_2011, convert_categoricals=False)
        want = _numeric(raw, "as02eq07c").where(lambda s: s != 999999)
        assert int((_numeric(raw, "as02eq07c") == 999999).sum()) == 11
        key = pd.MultiIndex.from_arrays([_id(raw["hid"]),
                                         "reported:" + _id(raw["as02eq0"])], names=["i", "production_obs"])
        want = pd.Series(want.to_numpy(), index=key)
        got = _totals(_wave(built, "2011-12"))
        got = got[got["plot_id"] != "Farm-level"].set_index(["i", "production_obs"])["Quantity_kg"]
        pd.testing.assert_series_equal(got.sort_index(), want.reindex(got.index).sort_index(),
                                       check_names=False, check_index=False)
        assert np.isclose(float(got.sum()), float(want.sum()))

    def test_2011_12_dry_is_as03eq09c_times_the_cycles(self, built):
        raw = _read("2011-12", DRY_2011, convert_categoricals=False)
        raw = raw[~raw.duplicated()]
        per_cycle = _numeric(raw, "as03eq09c")
        harvest = _numeric(raw, "as03eq09a")
        assert int((per_cycle == 99999).sum()) == 42
        no_answer = (per_cycle == 0) & (harvest > 0)
        assert int(no_answer.sum()) == 74
        want = per_cycle.where((per_cycle != 99999) & ~no_answer) * _numeric(raw, "as03eq06")
        key = pd.MultiIndex.from_arrays([_id(raw["hid"]), "reported:3E:" + _id(raw["as03eq01"])],
                                        names=["i", "production_obs"])
        want = pd.Series(want.to_numpy(), index=key)
        got = _totals(_wave(built, "2011-12"))
        got = got[got["plot_id"] == "Farm-level"].set_index(["i", "production_obs"])["Quantity_kg"]
        pd.testing.assert_series_equal(got.sort_index(), want.reindex(got.index).sort_index(),
                                       check_names=False, check_index=False)
        # Neither a code nor a 0 kg for a positive harvest is served.
        assert float(got.max()) < 99999
        assert int((got == 0).sum()) == 0

    def test_2014_15_is_as02eq07c_with_both_its_codes_masked(self, built):
        raw = _read("2014-15", FILE_2014, convert_categoricals=False)
        recorded = _numeric(raw, "AS02EQ07C")
        assert int((recorded == 999999).sum()) == 23 and int((recorded == 999998).sum()) == 1
        no_answer = (recorded == 0) & (_numeric(raw, "AS02EQ07A") > 0)
        assert int(no_answer.sum()) == 101
        want = recorded.where(~recorded.isin([999999, 999998]) & ~no_answer)
        got = _totals(_wave(built, "2014-15"))["Quantity_kg"]
        assert int(got.notna().sum()) == int(want.notna().sum())
        assert np.isclose(float(got.sum()), float(want.sum()))
        assert float(got.max()) < 999998

    def test_2018_19_has_no_kilograms(self, built):
        """Not unwired: the file asks none.  Its s16cq16c is the sold state."""
        from lsms_library.data_access import get_data_file
        path = get_data_file(f"{COUNTRY}/2018-19/Data/s16c_me_ner2018.dta")
        with pd.io.stata.StataReader(str(path)) as reader:
            labels = reader.variable_labels()
        assert not [v for v, text in labels.items() if "kg" in text.lower().split()
                    or "kgs" in text.lower().split() or "UML en kg" in text]
        assert "Etat du produit" in labels["s16cq16c"]
        assert _totals(_wave(built, "2018-19"))["Quantity_kg"].isna().all()

    def test_no_positive_harvest_is_served_a_zero(self, built):
        _, df = built
        kg = df["Quantity_kg"].astype("float64").to_numpy()
        q = df["Quantity"].astype("float64").to_numpy()
        assert not ((kg == 0) & (q > 0)).any()
        assert harvest_kg_factors(df).attrs["kg_factor_sources"]["survey_kg_zero"] == 0

    def test_the_low_sacks_are_served_as_recorded(self, built):
        """38 rows at about 7 kg per 100 kg sack look wrong and are not trimmed."""
        got = _totals(_wave(built, "2011-12"))
        dry = got[got["plot_id"] == "Farm-level"]
        sacks = dry[dry["u"] == "Sac de 100 kg"].astype({"Quantity": "float64"})
        low = sacks[(sacks["Quantity_kg"] > 0) & (sacks["Quantity_kg"] / sacks["Quantity"] < 20)]
        assert len(low) == 38
        assert np.isclose(float(low["Quantity_kg"].sum()), 11169.0)

    def test_2021_22_is_s16cq16c_row_by_row(self, built):
        raw = _read("2021-22", FILE_2021, convert_categoricals=False)
        got = _totals(_wave(built, "2021-22"))
        position = got["production_obs"].str.rsplit(":", n=1).str[-1].astype(int).to_numpy()
        want = _numeric(raw, "s16cq16c").to_numpy()[position]
        np.testing.assert_array_equal(got["Quantity_kg"].to_numpy(), want)

    def test_the_estimate_key_is_exactly_where_2021_22_has_a_total(self, built):
        got = _wave(built, "2021-22")
        keyed = got["Derivation"].astype("string").eq(ESTIMATE).fillna(False)
        assert (keyed == got["Quantity_kg"].notna()).all()
        for wave in ("2011-12", "2014-15", "2018-19"):
            other = _wave(built, wave)["Derivation"].astype("string").fillna("")
            assert not other.str.contains(ESTIMATE, regex=False).any()

    def test_the_registered_inputs_join_to_the_served_rows(self, built):
        country, _ = built
        raw = country.derivation_inputs(ESTIMATE).reset_index()
        got = _wave(built, "2021-22")
        got = got[got["Quantity_kg"].notna()]
        on = ["i", "plot_id", "production_obs"]
        assert sorted(map(tuple, raw[on].to_numpy())) == sorted(map(tuple, got[on].to_numpy()))

    def test_a_tiya_is_exactly_2_5_on_most_2021_22_rows(self, built):
        """Why the key exists.  Moves with a re-release, and then the key's text does too."""
        got = _totals(_wave(built, "2021-22"))
        tiya = got[(got["u"] == "Tiya") & (got["Quantity_kg"] > 0)]
        at = ((tiya["Quantity_kg"] / tiya["Quantity"].astype("float64")).round(4) == 2.5).sum()
        assert len(tiya) == 4402 and at == 2881

    def test_every_positive_total_is_served_by_the_rung(self, built):
        _, df = built
        factors = harvest_kg_factors(df)
        positive = df["Quantity_kg"].astype("float64").gt(0).to_numpy()
        assert (factors["KgFactorSource"].to_numpy()[positive] == "survey_kg").all()
        assert not (factors["KgFactorSource"].to_numpy()[~positive] == "survey_kg").any()
        counts = factors.attrs["kg_factor_sources"]
        assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)

    @pytest.mark.parametrize("wave", ["2011-12", "2014-15", "2021-22"])
    def test_harvest_kg_is_the_survey_where_it_speaks(self, built, wave):
        _, df = built
        got = df.xs(wave, level="t", drop_level=False)
        total = float(harvest_kg(got)["Harvest_kg"].sum())
        survey = got["Quantity_kg"].astype("float64")
        survey = float(survey[survey > 0].sum())
        rest = harvest_kg_factors(got)
        rest = rest[rest["KgFactorSource"] != "survey_kg"]
        qty = got["Quantity"].astype("float64").reindex(rest.index)
        assert np.isclose(total, survey + float((qty * rest["kg_per_unit"]).sum()))
