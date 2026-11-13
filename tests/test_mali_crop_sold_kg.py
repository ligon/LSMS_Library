"""GH #1560 (ruling @ligon 2026-11-12, "Treat 2021 as totals"): Mali 2021-22's sold kilograms.

``Quantity_sold_kg`` is ``s16dq05c``, "Estimation Quantite totale UML en kg", served as the
TOTAL kilograms of the sale it rides with, on the single harvest row or the Farm-level row.
A 0 beside a positive sold quantity (4 rows) is NA.  ``KgFactor_sold`` stays NA in this
wave: the total is never divided into a factor.  The other waves carry NA.  ``TestRules``
and ``TestDeclared`` need no microdata; ``TestServed`` does (``requires_s3``), and its oracle
reads the ``.dta`` and imports nothing from ``Mali/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import records_for, resolve_callable
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

COUNTRY = "Mali"
TABLE = "crop_production"
KEY = "Mali::crop_production::hh-crop-sale-single-row"
SOLD_KG = "Mali::crop_production::sold-kg-estimate"
ROOT = countries_root() / COUNTRY
WAVE = "2021-22"
FILE = "s16d_me_mli2021.dta"


def _mali(name):
    return resolve_callable(f"lsms_library.countries.Mali._.mali:{name}")


class TestRules:
    def test_a_zero_beside_a_positive_sale_is_no_answer(self):
        got = _mali("ehcvm_sold_kilograms")(pd.Series([0.0, 0.0, 700.0, np.nan]),
                                             pd.Series([4.0, np.nan, 7.0, 2.0]))
        assert got.dtype == "float64"
        assert np.isnan(got.iloc[0]) and got.iloc[1] == 0.0 and got.iloc[2] == 700.0
        assert np.isnan(got.iloc[3])

    def test_the_sale_carries_its_kilograms_wherever_it_goes(self):
        attach = _mali("derive_hh_crop_sale_attachment")
        harvest = pd.DataFrame({"i": ["1", "2", "2"], "crop": ["Mil"] * 3,
                                "plot_id": ["1_1", "1_1", "1_2"], "Quantity": [5.0, 1.0, 2.0]})
        sales = pd.DataFrame({"i": ["1", "2"], "crop": ["Mil", "Mil"],
                              "Quantity_sold": [2.0, 3.0], "Quantity_sold_kg": [200.0, 75.0],
                              "Unit_sold": ["Sac (100 Kg)", "Kg"], "Value_sold": [9.0, 8.0]})
        got = attach(harvest, sales, columns=["Quantity_sold", "Quantity_sold_kg",
                                              "Unit_sold", "Value_sold"])
        assert got.loc[got["plot_id"] == "1_1", "Quantity_sold_kg"].tolist()[0] == 200.0
        farm = got[got["plot_id"] == "Farm-level"]
        assert farm["Quantity_sold_kg"].tolist() == [75.0]
        assert float(got["Quantity_sold_kg"].sum()) == 275.0

    def test_the_finished_frame_types_the_column(self):
        finish = _mali("finish_ehcvm_crop_production")
        frame = pd.DataFrame({"i": ["1"], "plot_id": ["1_1"], "crop": ["Mil"], "u": ["Kg"],
                              "Quantity": [4.0], "Quantity_sold": [2.0], "Quantity_sold_kg": [2.0],
                              "Value_sold": [100.0], "intercropped": [False]})
        got = finish(frame, WAVE)
        assert got["Quantity_sold_kg"].dtype == "float64" and got["Quantity_sold_kg"].tolist() == [2.0]
        got = finish(frame.drop(columns="Quantity_sold_kg"), "2018-19")
        assert got["Quantity_sold_kg"].isna().all()


class TestDeclared:
    def test_the_column_is_optional_and_not_a_level(self):
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert entry["Quantity_sold_kg"] == {"type": "float", "optional": True}
        assert "Quantity_sold_kg" not in entry["index"]

    def test_the_sale_record_names_the_kilograms(self):
        record = records_for(COUNTRY, TABLE)[KEY]
        assert "Quantity_sold_kg" in record["columns"]
        assert "s16dq05c" in record["raw_variables"]

    def test_site_q_screens_the_column(self):
        from lsms_library import quantity_audit
        assert "Quantity_sold_kg" in quantity_audit._SCREENED_COLUMNS["crop_production"]
        assert "Quantity_sold_kg" in quantity_audit._NEAR_ZERO_COLUMNS["crop_production"]

    def test_the_estimate_key_is_registered(self):
        record = records_for(COUNTRY, TABLE)[SOLD_KG]
        assert list(record["waves"]) == [WAVE] and list(record["columns"]) == ["Quantity_sold_kg"]
        assert "s16dq05c" in record["raw_variables"] and "Estimation" in str(record["rule"])
        assert callable(resolve_callable(record["function"]))
        assert callable(resolve_callable(record["inputs"]))


def _read(name, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / WAVE / "Data" / name), **kw)


@pytest.fixture(scope="module")
def built():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country(COUNTRY)
        df = country.crop_production()
    return country, df


@pytest.mark.requires_s3
class TestServed:
    def test_2021_22_is_s16dq05c_with_its_zeros_masked(self, built):
        raw = _read(FILE, convert_categoricals=False)
        kg = pd.to_numeric(raw["s16dq05c"], errors="coerce")
        sold = pd.to_numeric(raw["s16dq05a"], errors="coerce")
        assert int(((kg == 0) & (sold > 0)).sum()) == 4
        _, df = built
        got = df.xs(WAVE, level="t")["Quantity_sold_kg"].astype("float64")
        assert int(got.notna().sum()) == int(kg.gt(0).sum()) == 1136
        assert float(got.sum()) == float(kg.sum()) == 4873574.0
        assert not (got == 0).any()

    def test_every_sold_kilogram_is_on_a_sale_row(self, built):
        _, df = built
        got = df.xs(WAVE, level="t")
        sale = (got["Quantity_sold"].fillna(0) > 0) | (got["Value_sold"].fillna(0) > 0)
        assert not (got["Quantity_sold_kg"].fillna(0).gt(0) & ~sale).any()
        farm = got.index.get_level_values("plot_id") == "Farm-level"
        assert got.loc[farm & sale.to_numpy(), "Quantity_sold_kg"].notna().all()

    def test_no_total_becomes_a_factor(self, built):
        _, df = built
        assert df.xs(WAVE, level="t")["KgFactor_sold"].isna().all()

    def test_in_kilograms_the_total_is_the_quantity(self, built):
        _, df = built
        got = df.xs(WAVE, level="t")
        kg = got["Unit_sold"].astype("string").eq("Kg").fillna(False) & got["Quantity_sold_kg"].notna()
        assert int(kg.sum()) == 495
        assert (got.loc[kg, "Quantity_sold_kg"] == got.loc[kg, "Quantity_sold"]).all()

    def test_the_other_waves_carry_no_sold_kilograms(self, built):
        _, df = built
        other = df[df.index.get_level_values("t") != WAVE]
        assert other["Quantity_sold_kg"].isna().all()

    def test_the_registered_inputs_carry_the_raw_kilograms(self, built):
        country, _ = built
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert "s16dq05c" in country.derivation_inputs(KEY, wave=WAVE).columns

    def test_site_q_names_without_changing_anything(self, built):
        """Report-only: every named value is served as recorded; the largest is still there."""
        from lsms_library.quantity_audit import quantity_reports
        _, df = built
        served = df["Quantity_sold_kg"].astype("float64")
        assert float(served.max()) == 1150000.0
        for report in quantity_reports(country=COUNTRY, table=TABLE):
            if report["column"] != "Quantity_sold_kg":
                continue
            assert report["wave"] == WAVE
            for offender in report["offenders"]:
                assert (served == offender["value"]).any()

    def test_site_q_names_exactly_the_four_2021_22_rows(self, built):
        """The rows named, pinned on the built frame rather than the process-wide
        ledger: 3 high and 1 low, all 2021-22 (GH #1560 (d)).  The warning prints the
        harvest unit as ``harvest_u`` and the value as kilograms sold (red team F2)."""
        from lsms_library.quantity_audit import _format_quantity_report, audit_quantities
        _, df = built
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            reports = audit_quantities(df, "Quantity_sold_kg", country=COUNTRY,
                                       table=TABLE, tails=("high", "low"))
        named = sorted((r["wave"], r["direction"], o["value"])
                       for r in reports for o in r["offenders"])
        assert named == sorted([(WAVE, "high", 1150000.0), (WAVE, "high", 437500.0),
                                (WAVE, "high", 400000.0), (WAVE, "low", 5.0)])
        high = next(r for r in reports if r["direction"] == "high")
        top = high["offenders"][0]
        assert (top["j"], top["u"], top["value"]) == ("Arachide", "Sac (100 Kg)", 1150000.0)
        message = _format_quantity_report(high)
        assert "harvest_u=Sac (100 Kg) 1,150,000 kg sold (x" in message
        assert "TOTAL kilograms sold" in message and " u=Sac" not in message

    def test_the_sold_kilograms_carry_the_estimate_key(self, built):
        """Ruling 2026-11-12 (c): exactly the rows with a Quantity_sold_kg, Farm-level too."""
        country, df = built
        got = df.reset_index()
        keyed = got["Derivation"].astype("string").str.contains(SOLD_KG, regex=False).fillna(False)
        assert (keyed == got["Quantity_sold_kg"].notna()).all()
        assert int(keyed.sum()) == 1136 and set(got.loc[keyed, "t"]) == {WAVE}
        assert int((keyed & (got["plot_id"] == "Farm-level")).sum()) == 70
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = country.derivation_inputs(SOLD_KG, wave=WAVE).reset_index()
        rows = got[keyed]
        assert sorted(zip(raw["i"].astype(str), raw["crop"].astype(str))) == sorted(
            zip(rows["i"].astype(str), rows["crop"].astype(str)))
