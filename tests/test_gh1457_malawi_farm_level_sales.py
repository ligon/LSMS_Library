"""GH #1457: Malawi sales no harvest row holds go on a Farm-level row.

Modules I and Q record a sale per household and crop code, in its own unit and state.
The ladder attaches one to a single-plot crop's harvest row in the same unit.  Every other
sale now has ONE row of its own: ``plot_id='Farm-level'``, no ``Quantity``, ``u='Unknown'``,
``condition='unknown_condition'``, its own ``Unit_sold`` / ``Condition_sold``.

``TestRules`` needs no data.  ``TestOracle`` reads the module files directly and imports
nothing from ``Malawi/_/``.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

MALAWI = countries_root() / "Malawi"
FARM = "Farm-level"
KG, BAG = "Kilogramme", "50 kg Bag"

SALES = {
    "2010-11": [("Data/Full_Sample/Agriculture/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Full_Sample/Agriculture/ag_mod_q.dta", "ag_q02a", "ag_q03")],
    "2013-14": [("Data/AG_MOD_I_13.dta", "ag_i02a", "ag_i03"),
                ("Data/AG_MOD_Q_13.dta", "ag_q02a", "ag_q03")],
    "2016-17": [("Data/Cross_Sectional/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Cross_Sectional/ag_mod_q.dta", "ag_q02a", "ag_q03"),
                ("Data/Panel/ag_mod_i_16.dta", "ag_i02a", "ag_i03"),
                ("Data/Panel/ag_mod_q_16.dta", "ag_q02a", "ag_q03")],
    "2019-20": [("Data/Cross_Sectional/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Cross_Sectional/ag_mod_q.dta", "ag_q02a", "ag_q03"),
                ("Data/Panel/ag_mod_i_19.dta", "ag_i02a", "ag_i03"),
                ("Data/Panel/ag_mod_q_19.dta", "ag_q02a", "ag_q03")],
}


@pytest.fixture(scope="module")
def malawi():
    sys.path.insert(0, str(MALAWI / "_"))
    spec = importlib.util.spec_from_file_location("malawi_1457", MALAWI / "_" / "malawi.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _harvest(rows):
    df = pd.DataFrame(rows, columns=["i", "plot_id", "crop", "_crop_code", "u", "condition", "Quantity"])
    return df.assign(
        t="2010-11", production_obs=lambda d: "reported:G:" + d["_crop_code"].astype(str),
        crop_variety="x", planting_month=pd.array([pd.NA] * len(df), dtype="Int64"),
        harvest_month=pd.array([pd.NA] * len(df), dtype="Int64"),
        intercropped=pd.array([pd.NA] * len(df), dtype="boolean"),
        perennial=pd.array([False] * len(df), dtype="boolean"),
        _crop_code=df["_crop_code"].astype("Int64"), u=df["u"].astype("string"),
        condition=df["condition"].astype("string"), Quantity=df["Quantity"].astype("Float64"))


def _sale(rows):
    df = pd.DataFrame(rows, columns=["i", "_crop_code", "u", "condition_sold", "Quantity_sold", "Value_sold"])
    return df.astype({"_crop_code": "Int64", "u": "string", "condition_sold": "string",
                      "Quantity_sold": "Float64", "Value_sold": "Float64"})


def _assemble(malawi, harvest, sale):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return malawi.assemble_crop_production("2010-11", [_harvest(harvest)], [_sale(sale)])


class TestRules:

    def test_a_multi_plot_sale_is_one_farm_level_row(self, malawi):
        out = _assemble(malawi,
                        [("h", "R1", "Maize", 1, KG, "shelled", 10.0), ("h", "R2", "Maize", 1, KG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        farm = out.xs(FARM, level="plot_id")
        assert len(farm) == 1
        row = farm.iloc[0]
        assert (row.Quantity_sold, row.Value_sold, row.Unit_sold, row.Condition_sold) == (3.0, 300.0, KG, "shelled")
        assert pd.isna(row.Quantity) and row.u == "Unknown" and row.condition == "unknown_condition"
        assert row.production_obs == "reported:I:1" and pd.isna(row.Derivation)
        assert out[out.index.get_level_values("plot_id") != FARM]["Value_sold"].isna().all()
        assert out.attrs["sale_farm_level"]["by_cause"]["no_candidate_row"]["sales"] == 1

    def test_the_ladder_keeps_what_it_attached_and_states_the_sale(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Maize", 1, KG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        assert len(out) == 1
        row = out.iloc[0]
        assert (row.Value_sold, row.Unit_sold, row.Condition_sold) == (300.0, KG, "shelled")
        assert pd.isna(row.Derivation)

    def test_one_row_in_another_unit_goes_to_farm_level_unmoved(self, malawi):
        """GH #824 stands: a sale in another unit does not go on the harvest row."""
        out = _assemble(malawi, [("h", "R1", "Maize", 1, BAG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        assert out.loc[out.index.get_level_values("plot_id") == "R1", "Value_sold"].isna().all()
        assert out.xs(FARM, level="plot_id")["Unit_sold"].tolist() == [KG]

    def test_a_refused_basis_goes_to_farm_level_in_its_own_state(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Groundnut", 11, KG, "shelled", 10.0)],
                        [("h", 11, KG, "unshelled", 3.0, 300.0)])
        assert out.loc[out.index.get_level_values("plot_id") == "R1", "Value_sold"].isna().all()
        assert out.xs(FARM, level="plot_id")["Condition_sold"].tolist() == ["unshelled"]
        assert out.attrs["sale_basis_mismatch"]["sales"] == 1

    def test_two_records_of_one_code_keep_two_rows_and_no_sum(self, malawi):
        out = _assemble(malawi,
                        [("h", "R1", "Maize", 1, BAG, "shelled", 10.0), ("h", "R2", "Maize", 1, BAG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0), ("h", 1, "Bale", "shelled", 1.0, 50.0)])
        farm = out.xs(FARM, level="plot_id")
        assert sorted(farm["Value_sold"]) == [50.0, 300.0]
        assert sorted(farm["production_obs"]) == ["reported:I:1/row:0", "reported:I:1/row:1"]

    def test_a_did_not_sell_line_serves_nothing_new(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Maize", 1, KG, "shelled", 10.0), ("h", "R2", "Maize", 1, KG, "shelled", 5.0)],
                        [("h", 1, pd.NA, "unknown_condition", 0.0, 0.0)])
        assert FARM not in out.index.get_level_values("plot_id")

    def test_a_perennial_sale_is_named_for_its_module(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Mango", 1004, KG, "unknown_condition", 10.0),
                                 ("h", "R2", "Mango", 1004, KG, "unknown_condition", 5.0)],
                        [("h", 1004, KG, "unknown_condition", 1.0, 10.0)])
        row = out.xs(FARM, level="plot_id").iloc[0]
        assert row.production_obs == "reported:Q:4" and row.perennial == True  # noqa: E712

    def test_a_sale_on_a_sentinel_plot_is_not_on_it(self, malawi):
        out = _assemble(malawi, [("h", "unknown_plot", "Mango", 1004, KG, "unknown_condition", 10.0)],
                        [("h", 1004, KG, "unknown_condition", 1.0, 3600.0)])
        assert out.loc[out.index.get_level_values("plot_id") == "unknown_plot", "Value_sold"].isna().all()
        assert out.xs(FARM, level="plot_id")["Value_sold"].tolist() == [3600.0]


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Malawi").crop_production().reset_index()


def _source(wave):
    value = quantity = 0.0
    n = 0
    for name, qty, val in SALES[wave]:
        raw = get_dataframe(str(MALAWI / wave / name), convert_categoricals=False)
        raw = raw[~raw.duplicated(keep="first")]       # exact repeats (IHS3 2010-11)
        q = pd.to_numeric(raw[qty], errors="coerce").astype("float64")
        v = pd.to_numeric(raw[val], errors="coerce").astype("float64")
        value += float(v.sum())
        quantity += float(q.sum())
        n += int(((q > 0) | (v > 0)).sum())
    return value, quantity, n


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", list(SALES))
class TestOracle:

    def test_value_and_quantity_equal_source(self, served, wave):
        value, quantity, _ = _source(wave)
        g = served[served.t == wave]
        assert float(g.Value_sold.sum()) == pytest.approx(value, abs=1e-6)
        assert float(g.Quantity_sold.sum()) == pytest.approx(quantity, abs=1e-6)

    def test_a_farm_level_row_states_only_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == FARM)]
        assert len(g)
        assert g.Quantity.isna().all() and g.u.eq("Unknown").all() and g.condition.eq("unknown_condition").all()
        assert ((g.Quantity_sold > 0) | (g.Value_sold > 0)).all()
        assert g.Derivation.isna().all()
        assert g.production_obs.str.fullmatch(r"reported:[IQ]:\d+(/row:\d+)?").all()

    def test_a_sale_row_says_its_unit_and_a_harvest_rows_is_its_u(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id != FARM)]
        sold = (g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)
        assert g.loc[~sold, "Unit_sold"].isna().all() and g.loc[~sold, "Condition_sold"].isna().all()
        known = g.loc[sold, "u"].notna()
        assert (g.loc[sold, "Unit_sold"][known] == g.loc[sold, "u"][known]).all()

    def test_no_sentinel_row_holds_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == "unknown_plot")]
        assert not (g.Value_sold.astype(float) > 0).any()


class TestReview:
    """F3 and F4 of the #1457 review."""

    def test_a_sale_takes_its_households_label_for_the_code(self, malawi):
        """Code 48 beside lines whose text says cowpea is not a second crop (GH #1228)."""
        cowpea = "Cowpea (Khobwe)"
        out = _assemble(malawi, [("h", "R1", cowpea, 48, KG, "unknown_condition", 5.0),
                                 ("h", "R2", cowpea, 48, KG, "unknown_condition", 5.0)],
                        [("h", 48, KG, "unknown_condition", 1.0, 10.0)])
        assert out.xs(FARM, level="plot_id").index.get_level_values("crop").tolist() == [cowpea]

    def test_two_labels_on_a_households_code_fall_back_to_the_table(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Cowpea (Khobwe)", 48, KG, "unknown_condition", 5.0),
                                 ("h", "R2", "Other", 48, KG, "unknown_condition", 5.0)],
                        [("h", 48, KG, "unknown_condition", 1.0, 10.0)])
        assert out.xs(FARM, level="plot_id").index.get_level_values("crop").tolist() == ["Other"]
