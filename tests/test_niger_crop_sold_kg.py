"""GH #1560 / #1571 (ruling @ligon 2026-11-12, "Treat 2021 as totals"): Niger's sold kilograms.

``Quantity_sold_kg`` on ``crop_production`` is the survey's own kilograms for the SALE, a
total in the window of ``Quantity_sold``: 2011-12 ``as02eq12c`` (rainy) and ``as03eq11c``
(section 3E, per cycle, NOT multiplied by the cycle count); 2014-15 ``AS02EQ12C``; 2021-22
``s16dq05c``.  2018-19 asks none.  Each module's own code is NA (9999 rainy, 99999 dry,
9999 in 2014-15), and so is a 0 beside a positive sold quantity.  It is never a factor:
Niger serves no ``KgFactor_sold``.  ``TestRules`` and ``TestDeclared`` need no microdata.
``TestServed`` does (``requires_s3``); its oracle reads the ``.dta`` files and imports nothing
from ``Niger/_/``.
"""
from __future__ import annotations

import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import records_for, resolve_callable
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

COUNTRY = "Niger"
TABLE = "crop_production"
SALE_KEY = "Niger::crop_production::hh-crop-sale-single-row"
DRY_SALE = "Niger::crop_production::dry-season-sale-window"
NO_SALE = "Niger::crop_production::dry-season-sold-kg-no-sale"
RAINY_2011 = "NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta"
DRY_2011 = "NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta"
SALE_2014 = "NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E2P2.dta"
SALE_2021 = "s16d_me_ner2021.dta"
ROOT = countries_root() / COUNTRY


@pytest.fixture(scope="module")
def niger():
    fn = resolve_callable(records_for(COUNTRY, TABLE)[DRY_SALE]["function"])
    return sys.modules[fn.__module__]


def _frame(**cols):
    n = len(next(iter(cols.values())))
    base = {"i": [str(k) for k in range(n)], "plot_id": ["1_1"] * n, "crop": ["Mil"] * n,
            "u": ["Tiya"] * n, "Quantity": [4.0] * n, "Value_sold": [None] * n}
    base.update(cols)
    return pd.DataFrame(base)


class TestRules:
    def test_the_tail_supplies_and_types_the_column(self, niger):
        frame = _frame(Quantity_sold=[2.0], Quantity_sold_kg=[5.0])
        got = niger._finish_crop_production(frame, "2011-12")["Quantity_sold_kg"]
        assert got.dtype == "Float64" and got.tolist() == [5.0]
        got = niger._finish_crop_production(frame.drop(columns="Quantity_sold_kg"),
                                            "2018-19")["Quantity_sold_kg"]
        assert got.dtype == "Float64" and got.isna().all()

    def test_a_zero_beside_a_positive_sale_is_no_answer(self, niger):
        frame = _frame(Quantity_sold=[3.0, 0.0, None, 3.0],
                       Quantity_sold_kg=[0.0, 0.0, 0.0, 7.5])
        got = niger._finish_crop_production(frame, "2014-15").reset_index().set_index("i")
        got = got["Quantity_sold_kg"]
        assert pd.isna(got["0"])
        assert got["1"] == 0.0 and got["2"] == 0.0 and got["3"] == 7.5

    def test_the_harvest_kilograms_are_not_touched(self, niger):
        frame = _frame(Quantity_sold=[3.0], Quantity_sold_kg=[0.0], Quantity_kg=[12.0])
        got = niger._finish_crop_production(frame, "2014-15")
        assert got["Quantity_kg"].tolist() == [12.0]

    def test_the_dry_sale_window_carries_the_kilograms_unmultiplied(self, niger):
        got = niger.derive_dry_season_sale_window(pd.Series([3.0, 0.0]), pd.Series([900.0, 5.0]),
                                                  pd.Series([150.0, np.nan]))
        assert got["Quantity_sold_kg"].dtype == "float64"
        assert got["Quantity_sold_kg"].iloc[0] == 150.0 and np.isnan(got["Quantity_sold_kg"].iloc[1])
        # Without the kilograms the identity is what it was.
        got = niger.derive_dry_season_sale_window(pd.Series([3.0]), pd.Series([900.0]))
        assert list(got.columns) == ["Quantity_sold", "Value_sold"]

    def test_sold_kilograms_without_a_sale_are_masked(self, niger):
        got = niger.derive_dry_season_sold_kg_no_sale(
            pd.Series([150.0, 80.0, 40.0, 30.0, np.nan]),
            pd.Series([np.nan, 0.0, 2.0, np.nan, 3.0]),
            pd.Series([np.nan, 0.0, np.nan, 500.0, 900.0]))
        assert got.dtype == "float64"
        assert np.isnan(got.iloc[0]) and np.isnan(got.iloc[1])     # no quantity, no value
        assert got.iloc[2] == 40.0 and got.iloc[3] == 30.0          # either one is a sale
        assert np.isnan(got.iloc[4])

    def test_the_mask_key_joins_the_row_keys(self, niger):
        idx = pd.MultiIndex.from_tuples(
            [("1", niger.FARM_LEVEL, "reported:3E:1"), ("1", niger.FARM_LEVEL, "reported:3E:2"),
             ("1", "1_1", "reported:1")], names=["i", "plot_id", "production_obs"])
        df = pd.DataFrame({"Derivation": pd.array(["Niger::crop_production::dry-season-cycle-total"] * 2
                                                  + [pd.NA], dtype="string")}, index=idx)
        got = niger.stamp_dry_season_sold_kg_no_sale(df, {("1", "reported:3E:2")}).tolist()
        assert got[0] == "Niger::crop_production::dry-season-cycle-total"
        assert got[1] == "+".join(sorted(("Niger::crop_production::dry-season-cycle-total", NO_SALE)))
        assert pd.isna(got[2])

    def test_a_farm_level_sale_carries_its_kilograms(self, niger):
        harvest = pd.DataFrame({"i": ["1", "1"], "crop": ["Mil", "Mil"], "plot_id": ["1_1", "1_2"],
                                "Quantity": [4.0, 5.0], "Quantity_sold": [np.nan, np.nan],
                                "Value_sold": [np.nan, np.nan]})
        records = pd.DataFrame({"i": ["1"], "crop": ["Mil"], "Unit_sold": ["Tiya"],
                                "Quantity_sold": [2.0], "Quantity_sold_kg": [5.0],
                                "Value_sold": [700.0]})
        got = niger.derive_hh_crop_sale_attachment(harvest, records)
        farm = got[got["plot_id"] == niger.FARM_LEVEL]
        assert len(farm) == 1 and farm["Quantity_sold_kg"].tolist() == [5.0]


class TestDeclared:
    def test_the_column_is_optional_and_never_an_index_level(self):
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert entry["Quantity_sold_kg"] == {"type": "float", "optional": True}
        assert "Quantity_sold_kg" not in entry["index"]

    def test_the_canonical_column_is_optional_and_not_a_factor(self):
        path = countries_root().parent / "data_info.yml"
        canonical = load_yaml(path)["Columns"][TABLE]
        assert canonical["Quantity_sold_kg"]["optional"] is True
        assert canonical["Quantity_sold_kg"]["type"] == "float"
        assert "KgFactor_sold" in canonical["Quantity_sold_kg"]["note"]

    def test_no_total_becomes_a_sold_factor(self):
        """The ruling: a total is never put in KgFactor_sold, kg per ONE unit."""
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert "KgFactor_sold" not in entry
        for wave in ("2011-12", "2014-15", "2018-19", "2021-22"):
            text = (ROOT / wave / "_" / "crop_production.py").read_text(encoding="utf-8")
            assert "KgFactor_sold'" not in text and "KgFactor_sold\"" not in text

    def test_the_sale_records_name_the_kilograms(self):
        sale = records_for(COUNTRY, TABLE)[SALE_KEY]
        assert "Quantity_sold_kg" in sale["columns"]
        assert {"AS02EQ12C", "s16dq05c"} <= set(sale["raw_variables"])
        dry = records_for(COUNTRY, TABLE)[DRY_SALE]
        assert "Quantity_sold_kg" in dry["columns"] and "as03eq11c" in dry["raw_variables"]
        mask = records_for(COUNTRY, TABLE)[NO_SALE]
        assert list(mask["waves"]) == ["2011-12"] and list(mask["columns"]) == ["Quantity_sold_kg"]
        assert {"as03eq10", "as03eq11a", "as03eq11c", "as03eq12"} <= set(mask["raw_variables"])
        assert callable(resolve_callable(mask["function"]))
        assert callable(resolve_callable(mask["inputs"]))


def _read(wave, name, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / name), **kw)


def _numeric(frame, name):
    return pd.to_numeric(frame[name], errors="coerce").astype("float64")


def _id(series):
    return series.map(lambda x: str(int(x)) if pd.notna(x) else "")


@pytest.fixture(scope="module")
def built():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country(COUNTRY)
        df = country.crop_production()
    return country, df


def _wave(built, wave):
    return built[1].xs(wave, level="t").reset_index().astype({"Quantity_sold_kg": "float64"})


@pytest.mark.requires_s3
class TestServed:
    def test_2011_12_rainy_is_as02eq12c_on_its_line_with_9999_masked(self, built):
        raw = _read("2011-12", RAINY_2011, convert_categoricals=False)
        kg = _numeric(raw, "as02eq12c")
        assert int((kg == 9999).sum()) == 10 and int((kg == 0).sum()) == 0
        want = pd.Series(kg.where(kg != 9999).to_numpy(),
                         index=pd.MultiIndex.from_arrays(
                             [_id(raw["hid"]), "reported:" + _id(raw["as02eq0"])],
                             names=["i", "production_obs"]))
        got = _wave(built, "2011-12")
        got = got[got["plot_id"] != "Farm-level"].set_index(["i", "production_obs"])["Quantity_sold_kg"]
        np.testing.assert_array_equal(got.sort_index().to_numpy(),
                                      want.reindex(got.index).sort_index().to_numpy())
        assert int(got.gt(0).sum()) == 1286
        assert np.isclose(float(got.sum()), 333567.8, atol=1e-3)

    def test_2011_12_dry_is_as03eq11c_per_cycle_with_its_codes_masked(self, built):
        raw = _read("2011-12", DRY_2011, convert_categoricals=False)
        raw = raw[~raw.duplicated()]
        kg, sold = _numeric(raw, "as03eq11c"), _numeric(raw, "as03eq11a")
        value = _numeric(raw, "as03eq12")
        assert int((kg == 99999).sum()) == 39
        no_answer = (kg == 0) & (sold > 0) & (sold != 99999)
        assert int(no_answer.sum()) == 36
        sale = ((sold > 0) & (sold != 99999)) | ((value > 0) & (value != 9999999))
        no_sale = kg.notna() & (kg != 99999) & ~sale
        # Ruling 2026-11-12 (a): seven records state sold kilograms and no sale.
        assert int(no_sale.sum()) == 7 and float(kg[no_sale].sum()) == 8740.0
        want = kg.where((kg != 99999) & ~no_answer & sale)    # NOT x as03eq06
        want = pd.Series(want.to_numpy(), index=pd.MultiIndex.from_arrays(
            [_id(raw["hid"]), "reported:3E:" + _id(raw["as03eq01"])], names=["i", "production_obs"]))
        got = _wave(built, "2011-12")
        got = got[got["plot_id"] == "Farm-level"].set_index(["i", "production_obs"])["Quantity_sold_kg"]
        np.testing.assert_array_equal(got.sort_index().to_numpy(),
                                      want.reindex(got.index).sort_index().to_numpy())
        assert int(got.gt(0).sum()) == 356 and float(got.sum()) == 364768.0

    def test_the_no_sale_mask_is_keyed_on_exactly_its_seven_rows(self, built):
        country, _ = built
        got = _wave(built, "2011-12")
        keyed = got["Derivation"].astype("string").str.contains(NO_SALE, regex=False).fillna(False)
        assert int(keyed.sum()) == 7
        rows = got[keyed]
        assert (rows["plot_id"] == "Farm-level").all() and rows["Quantity_sold_kg"].isna().all()
        assert not ((rows["Quantity_sold"].astype("float64").fillna(0) > 0)
                    | (rows["Value_sold"].astype("float64").fillna(0) > 0)).any()
        # The row is a harvest and keeps its dry-season key.
        assert rows["Derivation"].str.contains(
            "Niger::crop_production::dry-season-cycle-total", regex=False).all()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = country.derivation_inputs(NO_SALE).reset_index()
        on = ["i", "plot_id", "production_obs"]
        assert sorted(map(tuple, raw[on].to_numpy())) == sorted(map(tuple, rows[on].to_numpy()))
        assert float(pd.to_numeric(raw["as03eq11c"]).sum()) == 8740.0

    def test_2014_15_is_AS02EQ12C_with_9999_masked(self, built):
        raw = _read("2014-15", SALE_2014, convert_categoricals=False)
        kg, sold = _numeric(raw, "AS02EQ12C"), _numeric(raw, "AS02EQ12A")
        assert int((kg == 9999).sum()) == 11 and float(kg.max()) == 9999
        want = kg.where((kg != 9999) & ~((kg == 0) & (sold > 0) & (sold != 9999)))
        got = _wave(built, "2014-15")["Quantity_sold_kg"]
        assert int(got.gt(0).sum()) == int(want.gt(0).sum()) == 1051
        assert np.isclose(float(got.sum()), float(want.sum())) and float(got.sum()) == 467963.0
        assert float(got.max()) < 9999

    def test_2021_22_is_s16dq05c_once_per_sale(self, built):
        raw = _read("2021-22", SALE_2021, convert_categoricals=False)
        raw = raw[~raw.duplicated(keep="first")]
        want = _numeric(raw, "s16dq05c")
        got = _wave(built, "2021-22")["Quantity_sold_kg"]
        assert int(got.gt(0).sum()) == int(want.gt(0).sum()) == 1818
        assert np.isclose(float(got.sum()), float(want.sum()))

    def test_2018_19_has_no_sold_kilograms(self, built):
        assert _wave(built, "2018-19")["Quantity_sold_kg"].isna().all()

    def test_no_code_and_no_zero_beside_a_sale_is_served(self, built):
        _, df = built
        kg = df["Quantity_sold_kg"].astype("float64")
        q = df["Quantity_sold"].astype("float64")
        assert not kg.isin([9999, 99999, 999999]).any()
        assert not ((kg == 0) & (q > 0)).any()
        assert "KgFactor_sold" not in df.columns

    def test_every_sale_kilogram_rides_on_a_sale_row(self, built):
        """No sold kilogram sits on a row with no positive sale, in any wave (3E's seven
        such records are masked by the 2026-11-12 rule)."""
        for wave in ("2011-12", "2014-15", "2021-22"):
            got = _wave(built, wave)
            sale = (got["Quantity_sold"].astype("float64").fillna(0) > 0) | (
                got["Value_sold"].astype("float64").fillna(0) > 0)
            assert not (got["Quantity_sold_kg"].fillna(0).gt(0) & ~sale).any()

    def test_in_kilograms_the_total_is_the_quantity(self, built):
        """The total reading's control: on a sale in Kg, kg == the sold quantity."""
        _, df = built
        kg = df["Unit_sold"].astype("string").eq("Kg").fillna(False)
        sub = df[kg & df["Quantity_sold_kg"].notna()]
        share = float((sub["Quantity_sold_kg"].astype("float64")
                       == sub["Quantity_sold"].astype("float64")).mean())
        assert len(sub) > 200 and share > 0.9

    def test_the_registered_inputs_carry_the_raw_kilograms(self, built):
        country, _ = built
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert "as03eq11c" in country.derivation_inputs(DRY_SALE).columns
            for wave, name in (("2014-15", "AS02EQ12C"), ("2021-22", "s16dq05c")):
                assert name in country.derivation_inputs(SALE_KEY, wave=wave).columns
