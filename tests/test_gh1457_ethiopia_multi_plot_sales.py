"""GH #1457: Ethiopia §11/§12 household-crop sales, option (d) of the #843 ruling.

Section 11 and 12 record a sale per holder and crop, with no plot.  A sale on exactly one
§9 line goes on it (the SALES GRAIN CAVEAT).  Any other has ONE row with
``plot_id='Farm-level'``, no harvest ``Quantity``, ``u='Unknown'``, its own ``Unit_sold``.

``TestRules`` needs no data.  ``TestOracle`` builds the table and reads the source files
directly, importing nothing from ``Ethiopia/_/``.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

ETHIOPIA = countries_root() / "Ethiopia"
FARM = "Farm-level"
SINGLE = "Ethiopia::crop_production::hh-crop-sale-single-row"
SUMMED = "Ethiopia::crop_production::hh-crop-sale-records-summed"
TWO = "Ethiopia::crop_production::hh-crop-sale-two-units"
S12 = "Ethiopia::crop_production::sect12-sale-period"
PERMANENT = "Ethiopia::crop_production::permanent-crop-12-month-window"

SOURCE = {
    "2011-12": dict(s11="sect11_ph_w1.dta", code="crop_code", q="ph_s11q03_a", v="ph_s11q04_a", s12="sect12_ph_w1.dta"),
    "2013-14": dict(s11="sect11_ph_w2.dta", code="crop_code", q="ph_s11q03_a", v="ph_s11q04", s12="sect12_ph_w2.dta"),
    "2015-16": dict(s11="sect11_ph_w3.dta", code="crop_code", q="ph_s11q03_a", v="ph_s11q04", s12="sect12_ph_w3.dta"),
    "2018-19": dict(s11="sect11_ph_w4.dta", code="s11q01", q="s11q11a", v="s11q12", s12=None),
    "2021-22": dict(s11="sect11_ph_w5.dta", code="s11q01b", q="s11q11a", v="s11q12", s12=None),
}


@pytest.fixture(scope="module")
def ethiopia():
    sys.path.insert(0, str(ETHIOPIA / "_"))
    spec = importlib.util.spec_from_file_location("ethiopia_1457", ETHIOPIA / "_" / "ethiopia.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _lines(rows):
    df = pd.DataFrame(rows, columns=["_hh", "_holder", "_code", "_sunit", "Quantity_sold", "Value_sold"])
    return df.astype({"_hh": "string", "_holder": "string", "_code": "Int64", "_sunit": "string",
                      "Quantity_sold": "float64", "Value_sold": "float64"})


def _nlines(rows):
    return pd.DataFrame(rows, columns=["_holder", "_code", "_nlines"]).astype(
        {"_holder": "string", "_code": "Int64"})


class TestRules:

    def test_one_line_is_attached_and_keyed(self, ethiopia):
        got = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0)]), _nlines([("a", 1, 1)])).iloc[0]
        assert got.attached and got.Derivation == SINGLE
        assert (got.Quantity_sold, got.Unit_sold, got.Value_sold) == (2.0, "Kg", 10.0)

    @pytest.mark.parametrize("lines", [[("a", 1, 2)], []])
    def test_two_lines_or_none_are_not(self, ethiopia, lines):
        got = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0)]), _nlines(lines)).iloc[0]
        assert not got.attached and pd.isna(got.Derivation)

    def test_two_lines_in_one_unit_sum_and_say_so(self, ethiopia):
        got = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0), ("h", "a", 1, "Kg", 3.0, 20.0)]), _nlines([("a", 1, 2)])).iloc[0]
        assert (got.Quantity_sold, got.Unit_sold, got.Value_sold, got.Derivation) == (5.0, "Kg", 30.0, SUMMED)

    def test_two_units_sum_the_value_and_serve_no_quantity(self, ethiopia):
        got = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0), ("h", "a", 1, "Box", 3.0, 20.0)]), _nlines([("a", 1, 2)])).iloc[0]
        assert got.Value_sold == 30.0 and pd.isna(got.Quantity_sold) and pd.isna(got.Unit_sold)
        assert got.Derivation == TWO

    def test_two_holders_of_one_household_keep_two_records(self, ethiopia):
        got = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0), ("h", "b", 1, "Kg", 3.0, 20.0)]), _nlines([]))
        assert len(got) == 2

    def test_a_farm_level_row_carries_holder_and_module_and_no_harvest(self, ethiopia):
        placed = ethiopia.derive_hh_crop_sale_attachment(
            _lines([("h", "a", 1, "Kg", 2.0, 10.0)]), _nlines([("a", 1, 2)]))
        row = ethiopia._farm_level_rows("2018-19", placed, "s11", {1: "Maize"}, set(), "Unknown",
                                        permanent=pd.MultiIndex.from_tuples([("a", 1)])).iloc[0]
        assert (row.plot_id, row.j, row.u, row.production_obs) == (FARM, "Maize", "Unknown", "reported:s11:a:1")
        assert pd.isna(row.Quantity) and row.Unit_sold == "Kg"
        assert row.Derivation == PERMANENT

    def test_a_section_12_row_keeps_its_window_key(self, ethiopia):
        placed = ethiopia.derive_hh_crop_sale_attachment(_lines([("h", "a", 7, "Kg", 2.0, 10.0)]), _nlines([]))
        row = ethiopia._farm_level_rows("2013-14", placed, "s12", {7: "Mango"}, {7}, "Unknown",
                                        derivation=S12).iloc[0]
        assert row.Derivation == S12 and row.production_obs == "reported:s12:a:7" and row.perennial


def _sold(served, wave):
    g = served[served.t == wave]
    return g[(g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)]


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Ethiopia").crop_production().reset_index()


def _num(x):
    return pd.to_numeric(x, errors="coerce").astype("float64")


def _source(wave):
    spec = SOURCE[wave]
    d = get_dataframe(str(ETHIOPIA / wave / "Data" / spec["s11"]), convert_categoricals=False)
    x = pd.DataFrame({"h": d["holder_id"].map(format_id), "c": _num(d[spec["code"]]),
                      "q": _num(d[spec["q"]]), "v": _num(d[spec["v"]])})
    x = x.dropna(subset=["c"])    # W2: one line with no crop code stays unserved
    value = float(x.v.sum())
    if spec["s12"]:
        e = get_dataframe(str(ETHIOPIA / wave / "Data" / spec["s12"]), convert_categoricals=False)
        y = pd.DataFrame({"h": e["holder_id"].map(format_id), "c": _num(e["crop_code"]),
                          "q": _num(e["ph_s12q07"]), "v": _num(e["ph_s12q08"])})
        y["h"] = y["h"].where(y["h"].astype(str).str.strip().str.len() > 0)
        y = y.dropna(subset=["h", "c"])
        # the two classes #820 leaves out: a repeated pair, and a pair §11 also holds
        y = y[~y.duplicated(["h", "c"], keep=False)]
        in11 = pd.MultiIndex.from_frame(y[["h", "c"]]).isin(pd.MultiIndex.from_frame(x[["h", "c"]]))
        y = y[~in11]
        value += float(y.v.sum())
    return value


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", list(SOURCE))
class TestOracle:

    def test_value_equals_source(self, served, wave):
        g = served[served.t == wave]
        assert float(g.Value_sold.astype(float).sum()) == pytest.approx(_source(wave), abs=1e-3)

    def test_a_farm_level_row_states_only_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == FARM)]
        assert len(g) and g.Quantity.isna().all() and g.u.eq("Unknown").all()
        assert g.production_obs.str.fullmatch(r"reported:s1[12]:[^:]+:\d+").all()
        assert not g.duplicated(["i", "production_obs"]).any()
        assert ((g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)).all()

    def test_one_holder_crop_is_on_one_row(self, served, wave):
        """No sale on two rows: per (holder, crop, module) one row has a sold value."""
        g = _sold(served, wave)
        holder = g.plot_id.str.split("_").str[0].where(g.plot_id != FARM, g.production_obs.str.split(":").str[2])
        code = g.production_obs.str.split(":").str[-1]
        farm = g[g.plot_id == FARM].assign(h=holder, c=code)
        assert not farm.duplicated(["h", "c"]).any()

    def test_a_section_12_farm_level_row_carries_the_window_key(self, served, wave):
        g = served[(served.t == wave) & served.production_obs.str.startswith("reported:s12:")]
        assert g.Derivation.astype("string").str.contains(S12, regex=False).all()

    def test_every_keyed_row_holds_a_sale(self, served, wave):
        g = served[served.t == wave]
        keyed = g.Derivation.astype("string").str.contains("hh-crop-sale", regex=False).fillna(False)
        sold = (g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)
        assert sold[keyed].all()

    def test_a_row_with_a_sold_quantity_says_its_unit(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id != FARM)]
        q = g.Quantity_sold.astype(float).gt(0)
        assert g.loc[q, "Unit_sold"].notna().all() and (g.loc[q, "Unit_sold"] == g.loc[q, "u"]).all()


@pytest.mark.requires_s3
@pytest.mark.parametrize("key,wave", [(SINGLE, "2021-22"), (SUMMED, "2021-22"), (TWO, "2021-22"),
                                      (SINGLE, "2013-14"), (SINGLE, "2011-12"), (SINGLE, "2018-19")])
def test_inputs_return_the_lines_of_the_keyed_rows(served, key, wave):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        x = ll.Country("Ethiopia").derivation_inputs(key, wave=wave).reset_index()
    assert list(x.columns[:3]) == ["t", "i", "j"] and "holder_id" in x.columns
    g = served[served.t == wave]
    keyed = g[g.Derivation.astype("string").str.contains(key, regex=False).fillna(False)]
    assert len(keyed)
    assert set(zip(keyed.i.astype(str), keyed.j.astype(str))) <= set(zip(x.i.astype(str), x.j.astype(str)))


class TestServedLines:
    """F1 of the #1457 review: the caveat counts lines that will be SERVED."""

    def _frame(self, rows):
        return pd.DataFrame(rows, columns=["_holder", "_code", "u", "Quantity"]).astype(
            {"_holder": "string", "_code": "Int64", "u": "string", "Quantity": "Float64"})

    def test_a_placeholder_does_not_count_beside_a_served_line(self, ethiopia):
        out = self._frame([("a", 1, "Kg", 5.0), ("a", 1, pd.NA, pd.NA)])
        assert ethiopia._s9_line_is_served(out).tolist() == [True, False]
        assert ethiopia._s9_lines_per_holder_crop(out)["_nlines"].tolist() == [1]

    def test_a_lone_placeholder_counts_and_two_count_as_two(self, ethiopia):
        out = self._frame([("a", 1, pd.NA, pd.NA), ("b", 1, pd.NA, pd.NA), ("b", 1, pd.NA, pd.NA)])
        got = ethiopia._s9_lines_per_holder_crop(out).set_index("_holder")["_nlines"]
        assert got.to_dict() == {"a": 1, "b": 2}

    def test_a_row_with_a_unit_is_served_though_it_states_no_harvest(self, ethiopia):
        out = self._frame([("a", 1, "Kg", pd.NA), ("a", 1, "Kg", 5.0)])
        assert ethiopia._s9_lines_per_holder_crop(out)["_nlines"].tolist() == [2]

    def test_a_frame_of_keys_counts_every_line(self, ethiopia):
        out = pd.DataFrame({"_holder": ["a", "a"], "_code": pd.array([1, 1], dtype="Int64")})
        assert ethiopia._s9_lines_per_holder_crop(out)["_nlines"].tolist() == [2]

    def test_the_sale_goes_on_the_served_line_not_the_placeholder(self, ethiopia):
        harvest = pd.DataFrame(
            [("HH1", "H1", 1, 1, 7, 50.0, "1. Kilogram"), ("HH1", "H1", 1, 2, 7, np.nan, None)],
            columns=["household_id", "holder_id", "parcel_id", "field_id", "crop_code", "qty", "unit"])
        sale = pd.DataFrame([("HH1", "H1", 7, 1, 2.0, 30.0)],
                            columns=["household_id", "holder_id", "crop_code", "sold", "sqty", "sval"])
        colmap = dict(hhid="household_id", holder_id="holder_id", parcel_id="parcel_id", field_id="field_id",
                      crop_code="crop_code", quantity="qty", s_crop_code="crop_code", s_sold_flag="sold",
                      s_qty="sqty", s_value="sval")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            got = ethiopia.crop_production_for_wave("2015-16", harvest, None, sale, colmap,
                                                    unit_labels=harvest["unit"]).reset_index()
        assert len(got) == 1 and got.plot_id.tolist() == ["H1_1_1"]
        assert got.Value_sold.tolist() == [30.0] and got.Quantity_sold.tolist() == [2.0]
        assert got.Derivation.tolist() == [SINGLE]
