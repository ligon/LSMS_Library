"""GH #1457: Mali 2017-18 household-crop sales, option (d) of the #843 ruling.

``eaci17_s7gp2`` records a sale per household and crop, with no plot.  A sale (a
positive ``s7gq21a`` or ``s7gq22``) goes on the crop's single ``s7f`` harvest row,
keyed ``hh-crop-sale-single-row``, and otherwise on ONE ``plot_id='Farm-level'`` row
with no harvest ``Quantity``.  Nothing is split, summed or dropped.

``TestRules`` needs no data.  ``TestOracle`` builds the wave and reads the ``.dta``
files directly, importing nothing from ``Mali/_/``; it maps crops through the org
table the build reads, so a wrong row in that table is not caught here.
"""
from __future__ import annotations

import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

MALI = countries_root() / "Mali"
WAVE = "2017-18"
FARM = "Farm-level"
KEY = "Mali::crop_production::hh-crop-sale-single-row"
MASKED = (99, 9999)


@pytest.fixture(scope="module")
def mali():
    spec = importlib.util.spec_from_file_location("mali_1457", MALI / "_" / "mali.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRules:

    def test_extra_sale_columns_ride_both_paths(self, mali):
        harvest = pd.DataFrame({"i": ["a", "b", "b"], "crop": ["Mil"] * 3,
                                "plot_id": ["1_1", "1_1", "2_1"]})
        sales = pd.DataFrame({"i": ["a", "b"], "crop": ["Mil"] * 2, "Quantity_sold": [1.0, 2.0],
                              "Unit_sold": ["Sac", "Kg"], "KgFactor_sold": [100.0, 1.0],
                              "Value_sold": [10.0, 20.0]})
        got = mali.derive_hh_crop_sale_attachment(
            harvest, sales, columns=["Quantity_sold", "Unit_sold", "KgFactor_sold", "Value_sold"])
        on_row = got[(got.i == "a")].iloc[0]
        assert (on_row.plot_id, on_row.KgFactor_sold, on_row.Derivation) == ("1_1", 100.0, KEY)
        farm = got[got.plot_id == FARM]
        assert len(farm) == 1 and farm.iloc[0].KgFactor_sold == 1.0 and pd.isna(farm.iloc[0].Derivation)
        # the two plot rows of (b, Mil) hold nothing
        assert got[(got.i == "b") & (got.plot_id != FARM)].Value_sold.isna().all()

    def test_stamp_needs_a_plot_and_a_positive_sale(self, mali):
        idx = pd.MultiIndex.from_tuples(
            [("t", "a", "1_1", "Mil"), ("t", "b", "1_1", "Mil"), ("t", "c", FARM, "Mil"),
             ("t", "d", "1_1", "Mil")], names=["t", "i", "plot_id", "crop"])
        df = pd.DataFrame({"Quantity_sold": [1.0, 0.0, 1.0, np.nan],
                           "Value_sold": [np.nan, 0.0, 1.0, np.nan]}, index=idx)
        got = mali.stamp_hh_crop_sale(pd.Series(pd.NA, index=idx, dtype="string"), df)
        assert got.isna().tolist() == [False, True, True, True]
        assert got.iloc[0] == KEY


def _hh(df):
    """``EACI17_`` + grappe + exploitation, spelled here and not imported."""
    g = df["grappe"].astype("Int64").astype("string")
    e = df["exploitation"].astype("Int64").astype("string").str.zfill(3)
    return "EACI17_" + g + e


@pytest.fixture(scope="module")
def harmonize():
    t = df_from_orgfile(str(MALI / "_" / "categorical_mapping.org"), name="harmonize_food")
    return dict(zip(t["Code"].astype(str).str.strip(), t["Preferred Label"]))


@pytest.fixture(scope="module")
def sales(harmonize):
    raw = get_dataframe(str(MALI / WAVE / "Data" / "eaci17_s7gp2.dta"))
    q = pd.to_numeric(raw["s7gq21a"], errors="coerce").astype("float64")
    q = q.mask(q.isin(MASKED) | np.isclose(q, 9999.9))
    v = pd.to_numeric(raw["s7gq22"], errors="coerce").astype("float64")
    out = pd.DataFrame({"i": _hh(raw).values,
                        "crop": raw["s7gq01"].astype("string").str.strip().map(harmonize).values,
                        "rq": q.values, "rv": v.values})
    return out[(out.rq > 0) | (out.rv > 0)]


@pytest.fixture(scope="module")
def plots(harmonize):
    raw = get_dataframe(str(MALI / WAVE / "Data" / "eaci17_s7fp2.dta"))
    out = pd.DataFrame({"i": _hh(raw).values,
                        "crop": raw["s7fq03"].astype("string").str.strip().map(harmonize).values,
                        "plot_id": (raw["s7fq01"].astype("Int64").astype("string") + "_"
                                    + raw["s7fq02"].astype("Int64").astype("string")).values})
    return out.dropna(subset=["crop"])


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("Mali").crop_production(waves=[WAVE])
    return df.reset_index()


@pytest.mark.requires_s3
class TestOracle:

    def test_value_equals_source(self, served, sales):
        field = served[served.perennial.ne(True).fillna(True)]
        assert field.Value_sold.sum() == pytest.approx(sales.rv.sum(), abs=1e-6)
        assert sales.rv.sum() > 180_000_000     # all of s7gq22, not a part of it

    def test_quantity_equals_source_by_row(self, served, sales):
        field = served[served.perennial.ne(True).fillna(True)]
        assert field.Quantity_sold.sum() == pytest.approx(sales.rq.sum(), abs=1e-6)

    def test_every_sale_is_on_one_row_and_the_row_the_rule_names(self, served, sales, plots):
        field = served[served.perennial.ne(True).fillna(True)]
        sold = field[(field.Quantity_sold > 0) | (field.Value_sold > 0)]
        assert len(sold) == len(sales)
        assert not sold.duplicated(["i", "crop"]).any()
        n = plots.groupby(["i", "crop"]).size().rename("n").reset_index()
        x = sold.merge(sales, on=["i", "crop"], how="outer", indicator=True)
        assert x._merge.eq("both").all()
        x = x.merge(n, on=["i", "crop"], how="left")
        one = x.n.fillna(0).eq(1)
        assert x.loc[~one, "plot_id"].eq(FARM).all()
        assert x.loc[one, "plot_id"].ne(FARM).all()
        # Tolerates a second key on the row (`Derivation` is MULTI_SEP-joined).
        carries = x.Derivation.astype("string").str.contains(KEY, regex=False).fillna(False)
        assert carries[one].all()
        assert not carries[~one].any()
        assert x.rv.fillna(-1).eq(x.Value_sold.fillna(-1)).all()
        assert x.rq.fillna(-1).eq(x.Quantity_sold.fillna(-1)).all()

    def test_a_farm_level_sale_states_no_harvest(self, served):
        rows = served[served.plot_id.eq(FARM) & served.perennial.isna()]
        assert len(rows)
        assert rows.Quantity.isna().all() and rows.u.isna().all()
        assert ((rows.Quantity_sold > 0) | (rows.Value_sold > 0)).all()

    def test_no_zero_written_on_a_farm_level_row(self, served):
        rows = served[served.plot_id.eq(FARM)]
        assert not (rows.Value_sold == 0).any() or rows[rows.Value_sold == 0].perennial.eq(True).all()

    def test_trees_do_not_meet_a_sale(self, served):
        """A tree row and a sale's row share plot_id; the tree's harvest stays its own."""
        rows = served[served.plot_id.eq(FARM)]
        assert not rows.duplicated(["i", "crop"]).any()
        assert rows[rows.perennial.eq(True)].shape[0] == 270

    def test_inputs_join_the_keyed_rows(self, served):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            x = ll.Country("Mali").derivation_inputs(KEY, wave=WAVE).reset_index()
        keyed = served[served.Derivation.astype("string").str.contains(KEY, regex=False).fillna(False)]
        assert len(x) == len(keyed)
        assert set(zip(x.i, x.crop)) == set(zip(keyed.i, keyed.crop))
