"""GH #1457: Tanzania household-crop sales, option (d) of the #843 ruling.

``AG_SEC_5A`` / ``7A`` / ``7B`` record a sale per household and crop code, each asking
about one harvest module.  A sale goes on the code's single plot of that module, keyed
``hh-crop-sale-single-row``, and otherwise on ONE ``plot_id='Farm-level'`` row.

``TestRules`` needs no data: ``_hh_crop_sales.py`` loaded by path with invented frames.
``TestOracle`` builds the table and reads the sale modules directly, importing nothing
from ``Tanzania/_/`` (ids through ``updated_ids.json``).
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

TANZANIA = countries_root() / "Tanzania"
FARM = "Farm-level"
KEY = "Tanzania::crop_production::hh-crop-sale-single-row"
HH = {"2019-20": "sdd_hhid", "2020-21": "y5_hhid"}
SPELL = {"2019-20": str.upper, "2020-21": str.lower}
#: sale file, quantity, value, its harvest file, the harvest's section id, the sale's.
MODULES = [("5a", "ag5a_02", "ag5a_03", "4a", "4A", "5A"),
           ("7a", "ag7a_03", "ag7a_04", "6a", "6A", "7A"),
           ("7b", "ag7b_03", "ag7b_04", "6b", "6B", "7B")]


@pytest.fixture(scope="module")
def sales_module():
    sys.path.insert(0, str(TANZANIA / "_"))
    spec = importlib.util.spec_from_file_location("hh_crop_sales_1457", TANZANIA / "_" / "_hh_crop_sales.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(rows):
    df = pd.DataFrame(rows, columns=["i", "plot_id", "j", "production_obs", "Quantity", "Quantity_sold", "Value_sold"])
    df["t"] = "2019-20"
    df["u"] = pd.Series("kg", index=df.index, dtype="string")
    df["harvest_month"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    df["intercropped"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    df["perennial"] = pd.Series(False, index=df.index, dtype="boolean")
    for col in ("Quantity", "Quantity_sold", "Value_sold"):
        df[col] = df[col].astype("Float64")
    return df.set_index(["t", "i", "plot_id", "j", "production_obs"])


COLMAPS = {f"sales_{k}": dict(hhid="hh", crop="cropid", qty="q", value="v")
           for k in ("seasonal", "fruit", "perennial")}


def _sale(rows):
    return pd.DataFrame(rows, columns=["hh", "cropid", "q", "v"])


class TestRules:

    def _derive(self, m, rows, seasonal, perennial=None):
        sales = {"seasonal": _sale(seasonal), "fruit": None, "perennial": _sale(perennial or [])}
        return m.derive_hh_crop_sale_attachment(_rows(rows), "2019-20", sales, COLMAPS,
                                                crop_labels={34: "Pulses", 35: "Pulses", 9: "Maize"})

    def test_a_code_on_one_plot_keeps_its_row_and_takes_the_key(self, sales_module):
        got = self._derive(sales_module,
                           [("1", "p1", "Maize", "reported:4A:9", 10.0, 2.0, 300.0)],
                           [("1", 9, 2.0, 300.0)]).reset_index()
        assert len(got) == 1
        assert got.Derivation.tolist() == [KEY] and got.Unit_sold.tolist() == ["kg"]

    def test_two_plots_are_one_farm_level_row_named_for_the_sale_module(self, sales_module):
        got = self._derive(sales_module,
                           [("1", "p1", "Maize", "reported:4A:9", 10.0, pd.NA, pd.NA),
                            ("1", "p2", "Maize", "reported:4A:9", 0.0, pd.NA, pd.NA)],
                           [("1", 9, 2.0, 300.0)]).reset_index()
        farm = got[got.plot_id == FARM]
        assert len(got) == 3 and len(farm) == 1
        row = farm.iloc[0]
        assert row.production_obs == "reported:5A:9" and row.j == "Maize"
        assert pd.isna(row.Quantity) and pd.isna(row.u) and row.Unit_sold == "kg"
        assert pd.isna(row.Derivation) and not row.perennial
        assert got[got.plot_id != FARM].Value_sold.isna().all()

    def test_lumped_codes_and_two_modules_stay_apart(self, sales_module):
        rows = [("1", "p1", "Pulses", "reported:4A:34", 1.0, pd.NA, pd.NA),
                ("1", "p2", "Pulses", "reported:4A:34", 1.0, pd.NA, pd.NA),
                ("1", "p1", "Pulses", "reported:4A:35", 1.0, pd.NA, pd.NA),
                ("1", "p2", "Pulses", "reported:4A:35", 1.0, pd.NA, pd.NA),
                ("1", "p1", "Pulses", "reported:6B:34", 1.0, pd.NA, pd.NA),
                ("1", "p2", "Pulses", "reported:6B:34", 1.0, pd.NA, pd.NA)]
        got = self._derive(sales_module, rows, [("1", 34, 1.0, 10.0), ("1", 35, 2.0, 20.0)],
                           [("1", 34, 3.0, 30.0)]).reset_index()
        farm = got[got.plot_id == FARM].set_index("production_obs")
        assert farm.Value_sold.to_dict() == {"reported:5A:34": 10.0, "reported:5A:35": 20.0,
                                             "reported:7B:34": 30.0}
        assert farm.perennial.to_dict() == {"reported:5A:34": False, "reported:5A:35": False,
                                            "reported:7B:34": True}

    def test_a_sale_with_no_row_is_kept_and_an_unmapped_code_keeps_its_own_label(self, sales_module):
        got = self._derive(sales_module, [("1", "p1", "Maize", "reported:4A:9", 10.0, pd.NA, pd.NA)],
                           [("2", 77, 1.0, 10.0)]).reset_index()
        assert got[got.plot_id == FARM].j.tolist() == ["crop_77"]

    def test_a_record_that_is_not_a_sale_serves_nothing_new(self, sales_module):
        got = self._derive(sales_module,
                           [("1", "p1", "Maize", "reported:4A:9", 10.0, pd.NA, pd.NA),
                            ("1", "p2", "Maize", "reported:4A:9", 10.0, pd.NA, pd.NA)],
                           [("1", 9, 0.0, 0.0), ("2", 9, np.nan, np.nan)]).reset_index()
        assert len(got) == 2 and got.Derivation.isna().all()


@pytest.mark.parametrize("wave", list(HH))
def test_each_wave_script_calls_it(wave):
    """``tanzania.py``'s digest is pinned (tests/test_gh905_tanzania_signed_sums.py), so
    the wave scripts are what reach this module."""
    assert "_hh_crop_sales.derive_hh_crop_sale_attachment" in (
        TANZANIA / wave / "_" / "crop_production.py").read_text()


def _sales(wave):
    data = TANZANIA / wave / "Data"
    walk = json.loads((TANZANIA / "_" / "updated_ids.json").read_text()).get(wave, {})
    out = []
    for sale, qty, val, harvest, hsec, ssec in MODULES:
        s = get_dataframe(str(data / f"{SPELL[wave]('ag_sec_')}{SPELL[wave](sale)}.dta"), convert_categoricals=False)
        h = get_dataframe(str(data / f"{SPELL[wave]('ag_sec_')}{SPELL[wave](harvest)}.dta"), convert_categoricals=False)
        q = pd.to_numeric(s[qty], errors="coerce").astype("float64")
        v = pd.to_numeric(s[val], errors="coerce").astype("float64")
        code = pd.to_numeric(s["cropid"], errors="coerce").astype("Int64")
        x = pd.DataFrame({"raw": s[HH[wave]].map(format_id).values, "code": code.values, "rq": q.values, "rv": v.values,
                          "hsec": hsec, "ssec": ssec}).dropna(subset=["code"])
        n = (pd.DataFrame({"raw": h[HH[wave]].map(format_id).values,
                           "code": pd.to_numeric(h["cropid"], errors="coerce").astype("Int64").values})
             .dropna(subset=["code"]).groupby(["raw", "code"]).size().rename("n").reset_index())
        out.append(x[(x.rq > 0) | (x.rv > 0)].merge(n, on=["raw", "code"], how="left"))
    out = pd.concat(out, ignore_index=True)
    out["i"] = out.raw.map(lambda r: walk.get(r, r))
    return out


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Tanzania").crop_production().reset_index()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", list(HH))
class TestOracle:

    def test_totals_equal_source(self, served, wave):
        s = _sales(wave)
        g = served[served.t == wave]
        assert float(g.Value_sold.sum()) == pytest.approx(float(s.rv.sum()), abs=1e-6)
        assert float(g.Quantity_sold.sum()) == pytest.approx(float(s.rq.sum()), abs=1e-6)

    def test_each_sale_on_one_row_inside_its_own_wave_and_module(self, served, wave):
        s = _sales(wave)
        g = served[served.t == wave]
        sold = g[(g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)]
        assert len(sold) == len(s)
        s["harvest_obs"] = "reported:" + s.hsec + ":" + s.code.astype("string")
        s["farm_obs"] = "reported:" + s.ssec + ":" + s.code.astype("string")
        one = s.n.fillna(0).eq(1)
        on_row = s[one].merge(sold[sold.plot_id != FARM], left_on=["i", "harvest_obs"],
                              right_on=["i", "production_obs"], how="left", indicator=True)
        assert on_row._merge.eq("both").all() and len(on_row) == one.sum()
        assert on_row.Derivation.eq(KEY).all()
        assert on_row.rv.fillna(-1).eq(on_row.Value_sold.astype(float).fillna(-1)).all()
        at_farm = s[~one].merge(sold[sold.plot_id == FARM], left_on=["i", "farm_obs"],
                                right_on=["i", "production_obs"], how="left", indicator=True)
        assert at_farm._merge.eq("both").all() and len(at_farm) == (~one).sum()
        assert at_farm.Derivation.isna().all()
        assert at_farm.rv.fillna(-1).eq(at_farm.Value_sold.astype(float).fillna(-1)).all()
        assert at_farm.rq.fillna(-1).eq(at_farm.Quantity_sold.astype(float).fillna(-1)).all()

    def test_a_farm_level_row_states_only_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == FARM)]
        assert len(g) and g.Quantity.isna().all() and g.u.isna().all()
        assert g.production_obs.str.fullmatch(r"reported:(5A|7A|7B):\d+").all()
        assert g.loc[g.Quantity_sold.astype(float) > 0, "Unit_sold"].eq("kg").all()

    def test_the_sold_unit_is_the_instruments_on_every_sold_quantity(self, served, wave):
        g = served[served.t == wave]
        sold = g.Quantity_sold.astype(float).gt(0)
        assert g.loc[sold, "Unit_sold"].eq("kg").all() and g.loc[~sold, "Unit_sold"].isna().all()

    def test_inputs_join_the_keyed_rows(self, served, wave):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            x = ll.Country("Tanzania").derivation_inputs(KEY, wave=wave).reset_index()
        keyed = served[(served.t == wave) & served.Derivation.eq(KEY).fillna(False)]
        assert len(x) == len(keyed)
        assert set(zip(x.i, x.production_obs)) == set(zip(keyed.i, keyed.production_obs))
