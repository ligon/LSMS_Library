"""GH #1457: Niger 2014-15 / 2021-22 household-crop sales, option (d) of the #843 ruling.

``ECVMA2_AS2E2P2`` and ``s16d`` record a sale per household and crop, with no plot.  A
sale goes on the crop's single harvest LINE, keyed ``hh-crop-sale-single-row``, and
otherwise on ONE ``plot_id='Farm-level'`` row with no harvest ``Quantity``.

``TestRules`` needs no data.  ``TestOracle`` builds the table and reads the ``.dta``
files directly, importing nothing from ``Niger/_/``, spelling the household id itself;
it maps crops through the org table the build reads.
"""
from __future__ import annotations

import importlib.util
import json
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, format_id, get_dataframe
from lsms_library.paths import countries_root

NIGER = countries_root() / "Niger"
FARM = "Farm-level"
KEY = "Niger::crop_production::hh-crop-sale-single-row"

SOURCE = {
    "2014-15": dict(base="NER_2014_ECVMA-II_v02_M_STATA8/", harvest="ECVMA2_AS2E1P2.dta", sale="ECVMA2_AS2E2P2.dta",
                    hcrop="CULTURE", scrop="AS02EQ110B", qty="AS02EQ12A", value="AS02EQ13",
                    qcode=9999, vcode=9999999, unit="AS02EQ12B", hqty="AS02EQ07A", hmonth="AS02EQ06A", hstand=None),
    "2021-22": dict(base="", harvest="s16c_me_ner2021.dta", sale="s16d_me_ner2021.dta",
                    hcrop="s16cq04", scrop="s16dq01", qty="s16dq05a", value="s16dq06",
                    qcode=None, vcode=None, unit="s16dq05b", hqty="s16cq16a", hmonth=None, hstand="s16cq07"),
}


@pytest.fixture(scope="module")
def niger():
    spec = importlib.util.spec_from_file_location("niger_1457", NIGER / "_" / "niger.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _records(rows):
    return pd.DataFrame(rows, columns=["i", "crop", "Quantity_sold", "Value_sold", "Unit_sold"])


class TestRules:

    def _harvest(self, rows):
        return pd.DataFrame(rows, columns=["i", "plot_id", "crop", "u", "Quantity",
                                           "Quantity_sold", "Value_sold", "Unit_sold"])

    def test_a_sale_on_its_one_line_adds_no_row(self, niger):
        h = self._harvest([("a", "1_1", "Mil", "Kg", 5.0, 2.0, 300.0, "Kg")])
        got = niger.derive_hh_crop_sale_attachment(h, _records([("a", "Mil", 2.0, 300.0, "Kg")]))
        assert len(got) == 1 and got.plot_id.tolist() == ["1_1"]

    def test_a_sale_on_two_lines_is_one_farm_level_row(self, niger):
        h = self._harvest([("a", "1_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA),
                           ("a", "1_1", "Mil", "Tiya", 5.0, np.nan, np.nan, pd.NA),
                           ("a", "2_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA)])
        got = niger.derive_hh_crop_sale_attachment(h, _records([("a", "Mil", 2.0, 300.0, "Sac")]))
        farm = got[got.plot_id == FARM]
        assert len(got) == 4 and len(farm) == 1
        row = farm.iloc[0]
        assert (row.Quantity_sold, row.Value_sold, row.Unit_sold) == (2.0, 300.0, "Sac")
        assert pd.isna(row.Quantity) and pd.isna(row.u)
        assert got[got.plot_id != FARM].Value_sold.isna().all()

    def test_a_sale_with_no_harvest_row_is_kept(self, niger):
        h = self._harvest([("a", "1_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA)])
        got = niger.derive_hh_crop_sale_attachment(h, _records([("a", "Sorgho", 2.0, 300.0, "Sac")]))
        assert got[got.plot_id == FARM].crop.tolist() == ["Sorgho"]

    def test_a_record_that_is_not_a_sale_serves_nothing(self, niger):
        h = self._harvest([("a", "1_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA),
                           ("a", "2_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA)])
        records = _records([("a", "Mil", 0.0, 0.0, pd.NA), ("b", "Mil", np.nan, np.nan, pd.NA)])
        got = niger.derive_hh_crop_sale_attachment(h, records)
        assert len(got) == 2

    def test_production_obs_is_the_sentinel_where_the_column_exists(self, niger):
        h = self._harvest([("a", "1_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA),
                           ("a", "2_1", "Mil", "Kg", 5.0, np.nan, np.nan, pd.NA)])
        h["production_obs"] = ["row:x:1", "row:x:2"]
        got = niger.derive_hh_crop_sale_attachment(h, _records([("a", "Mil", 2.0, 300.0, "Sac")]))
        assert got[got.plot_id == FARM].production_obs.tolist() == [niger.CROP_OBS_NA]

    def test_a_line_the_api_removes_does_not_count_unless_it_is_alone(self, niger):
        df = pd.DataFrame({"i": ["a", "a", "b", "c", "c"], "crop": ["Mil"] * 5,
                           "Quantity": [5.0, np.nan, np.nan, np.nan, np.nan],
                           "harvest_month": [np.nan] * 5})
        assert niger.served_harvest_lines(df).tolist() == [True, False, True, True, True]

    def test_the_key_needs_a_plot_and_a_positive_sale(self, niger):
        idx = pd.MultiIndex.from_tuples(
            [("a", "1_1"), ("b", "1_1"), ("c", FARM), ("d", "1_1")], names=["i", "plot_id"])
        df = pd.DataFrame({"Quantity_sold": [1.0, 0.0, 1.0, np.nan],
                           "Value_sold": [np.nan, 0.0, 1.0, np.nan]}, index=idx)
        got = niger.stamp_hh_crop_sale(df)
        assert got.isna().tolist() == [False, True, True, True] and got.iloc[0] == KEY

    def test_the_key_joins_a_key_already_there_and_leaves_the_rest(self, niger):
        idx = pd.MultiIndex.from_tuples([("a", "1_1"), ("b", "1_1"), ("c", FARM)], names=["i", "plot_id"])
        df = pd.DataFrame({"Quantity_sold": [1.0, np.nan, 1.0], "Value_sold": [1.0, np.nan, 1.0],
                           "Derivation": ["Niger::crop_production::other", "Niger::crop_production::other", pd.NA]},
                          index=idx)
        got = niger.stamp_hh_crop_sale(df)
        assert got.iloc[0] == "+".join(sorted([KEY, "Niger::crop_production::other"]))
        assert got.iloc[1] == "Niger::crop_production::other" and pd.isna(got.iloc[2])


def _household(df, wave):
    """The served raw id, spelled here (``niger.i``): 2014-15 grappe + '0' + menage
    (2 digits) + extension; 2021-22 'E_' + grappe + '0' + menage (2 digits)."""
    if wave == "2014-15":
        return (df["GRAPPE"].map(format_id) + "0" + df["MENAGE"].map(lambda x: format_id(x, zeropadding=2))
                + df["EXTENSION"].map(format_id)).astype(str)
    return ("E_" + df["grappe"].map(format_id) + "0"
            + df["menage"].map(lambda x: format_id(x, zeropadding=2))).astype(str)


@pytest.fixture(scope="module")
def crops():
    t = df_from_orgfile(str(NIGER / "_" / "categorical_mapping.org"), name="harmonize_food")
    return dict(zip(t["Original Label"].astype(str).str.strip(), t["Preferred Label"]))


@pytest.fixture(scope="module")
def walks():
    return json.loads((NIGER / "_" / "updated_ids.json").read_text())


def _raw(wave, name):
    spec = SOURCE[wave]
    path = NIGER / wave / "Data" / (spec["base"] + spec[name])
    return (get_dataframe(str(path), convert_categoricals=False),
            get_dataframe(str(path), convert_categoricals=True))


def _sales(wave, crops, walks):
    spec = SOURCE[wave]
    code, lab = _raw(wave, "sale")
    if wave == "2021-22":        # s16d's exact repeats: one record, not a second sale
        keep = ~code.duplicated(keep="first").to_numpy()
        code, lab = code[keep], lab[keep]
    q = pd.to_numeric(code[spec["qty"]], errors="coerce").astype("float64")
    v = pd.to_numeric(code[spec["value"]], errors="coerce").astype("float64")
    if spec["qcode"]:
        q = q.mask(q == spec["qcode"])
        v = v.mask(v == spec["vcode"])
    m = walks.get(wave, {})
    out = pd.DataFrame({"i": _household(code, wave).map(lambda x: m.get(x, x)).values,
                        "crop": lab[spec["scrop"]].astype("string").str.strip().map(crops).values,
                        "rq": q.values, "rv": v.values})
    return out[(out.rq > 0) | (out.rv > 0)]


def _lines(wave, crops, walks):
    """The SERVED lines per (i, crop): one holding a quantity, month or stand, since the API
    removes a line holding none.  A crop with no such line counts all its lines."""
    spec = SOURCE[wave]
    code, lab = _raw(wave, "harvest")
    m = walks.get(wave, {})
    q = pd.to_numeric(code[spec["hqty"]], errors="coerce")
    if spec["qcode"]:
        q = q.mask(q == spec["qcode"])
    content = q.notna().to_numpy()
    if spec["hmonth"]:
        mo = pd.to_numeric(code[spec["hmonth"]], errors="coerce")
        content = content | mo.between(1, 12).to_numpy()
    if spec["hstand"]:
        content = content | lab[spec["hstand"]].astype("string").isin(["Association de cultures", "Pure"]).to_numpy()
    out = pd.DataFrame({"i": _household(code, wave).map(lambda x: m.get(x, x)).values,
                        "crop": lab[spec["hcrop"]].astype("string").str.strip().map(crops).values,
                        "content": content}).dropna(subset=["crop"])
    per = out.groupby(["i", "crop"])["content"].agg(["size", "sum"])
    return per["sum"].where(per["sum"] > 0, per["size"]).rename("n").reset_index()


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Niger").crop_production().reset_index()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", list(SOURCE))
class TestOracle:

    def test_value_and_quantity_equal_source(self, served, wave, crops, walks):
        s = _sales(wave, crops, walks)
        g = served[served.t == wave]
        assert float(g.Value_sold.sum()) == pytest.approx(float(s.rv.sum()), abs=1e-6)
        assert float(g.Quantity_sold.sum()) == pytest.approx(float(s.rq.sum()), abs=1e-6)

    def test_every_sale_is_on_one_row_and_the_row_the_rule_names(self, served, wave, crops, walks):
        s = _sales(wave, crops, walks)
        g = served[served.t == wave]
        sold = g[(g.Quantity_sold > 0) | (g.Value_sold > 0)]
        assert len(sold) == len(s) and not sold.duplicated(["i", "crop"]).any()
        x = sold.merge(s, on=["i", "crop"], how="outer", indicator=True)
        assert x._merge.eq("both").all()
        x = x.merge(_lines(wave, crops, walks), on=["i", "crop"], how="left")
        one = x.n.fillna(0).eq(1)
        assert x.loc[~one, "plot_id"].eq(FARM).all()
        assert x.loc[one, "plot_id"].ne(FARM).all()
        # Tolerates a second key on the row (`Derivation` is MULTI_SEP-joined).
        carries = x.Derivation.astype("string").str.contains(KEY, regex=False).fillna(False)
        assert carries[one].all()
        assert not carries[~one].any()
        assert x.rv.fillna(-1).astype(float).eq(x.Value_sold.fillna(-1).astype(float)).all()

    def test_a_farm_level_row_states_only_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == FARM)]
        assert len(g)
        assert g.Quantity.isna().all() and g.u.eq("Unknown").all()
        assert g.condition.eq("unknown_condition").all()
        assert g.production_obs.eq("unresolved_observation").all()
        assert g.Unit_sold.notna().mean() > 0.95


@pytest.mark.requires_s3
def test_inputs_join_the_keyed_rows(served):
    c = ll.Country("Niger")
    for wave in SOURCE:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            x = c.derivation_inputs(KEY, wave=wave).reset_index()
        keyed = served[(served.t == wave) & served.Derivation.astype("string").str.contains(
            KEY, regex=False).fillna(False)]
        assert len(x) == len(keyed)
        assert set(zip(x.i, x.crop)) == set(zip(keyed.i, keyed.crop))
