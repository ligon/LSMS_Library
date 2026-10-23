"""Ethiopia W4/W5 permanent crops: a 12-month window, registered (GH #1256).

ESS4 (2018-19) folds the old §12 tree/fruit/root module into §9 and §11 and
puts a PERMANENT crop on a fixed 12 months:

* §9 Q4 "Have you harvested any of the [CROP] from this [FIELD]? (for
  permanent crop harvested between March 10, 2018 to March 9, 2019)" -- Q5 is
  that harvest's quantity, served as ``Quantity``;
* §11 Q1 "LIST ALL CROPS HARVESTED DURING THIS AGRICULTURAL SEASON. FOR
  PERMANENT CROPS PLEASE INCLUDE CROP HARVESTED BETWEEN MARCH 10, 2018 TO
  MARCH 9, 2019" -- Q7 / Q11 / Q12 are its sales (``Quantity_sold`` /
  ``Value_sold``).

The instrument lists no permanent crops; the post-planting crop roster flags
each crop row itself (§4 Q15, ``s4q15``: W4 "IS THIS CROP A FRUIT TREE
CROP?", W5 variable label "IS THIS CROP A Permanent/TREE CROP?" -- no W5
questionnaire is held).  Rows whose crop that flag marks permanent (every §4
line of the key says Yes) and that serve one of the three numbers are stamped
``Derivation = 'Ethiopia::crop_production::permanent-crop-12-month-window'``;
the values are untouched (the registered function is the identity).

Tiers: the synthetic and static tests need no data; ``requires_s3`` tests
read the raw ``.dta`` blobs or build the table.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
KEY = "Ethiopia::crop_production::permanent-crop-12-month-window"
W45 = {"2018-19": 4, "2021-22": 5}


@pytest.fixture(scope="module")
def eth():
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1256_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic: one plot per case
# ---------------------------------------------------------------------------

COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
    pl_crop_code="crop_code", pl_month="month", pl_permanent="perm",
    s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval",
)
nan = np.nan


def _harvest():
    rows = [  # household, holder, parcel, field, crop code, qty
        ("HH1", "H1", 1, 1, 72, 10.0),   # coffee, flagged permanent  -> stamped
        ("HH1", "H1", 1, 2, 7, 50.0),    # teff, flagged No           -> not
        ("HH2", "H2", 1, 1, 74, 3.0),    # enset, §4 lines DISAGREE   -> not
        ("HH3", "H3", 1, 1, 71, 2.0),    # chat, no §4 line           -> not
        ("HH4", "H4", 1, 1, 42, nan),    # banana, flagged, sold only -> stamped
        ("HH5", "H5", 1, 1, 46, nan),    # mango, flagged, nothing    -> not
    ]
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "qty"])


def _planting():
    rows = [("H1", 1, 1, 72, 1, 1), ("H1", 1, 2, 7, 1, 2),
            ("H2", 1, 1, 74, 1, 1), ("H2", 1, 1, 74, 1, 2),
            ("H4", 1, 1, 42, 1, 1), ("H5", 1, 1, 46, 1, 1)]
    return pd.DataFrame(rows, columns=["holder_id", "parcel_id", "field_id",
                                       "crop_code", "month", "perm"])


def _sale():
    return pd.DataFrame([("H4", 42, 1, 4.0, 40.0), ("H1", 72, 2, nan, nan)],
                        columns=["holder_id", "crop_code", "sold", "sqty", "sval"])


def _build(eth, colmap=COLMAP, wave="2018-19"):
    h = _harvest()
    units = pd.Series(["1. Kilogram"] * len(h), dtype=object)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return eth.crop_production_for_wave(wave, h, _planting(), _sale(), colmap,
                                            unit_labels=units)


def _d(df, holder):
    sub = df[df.index.get_level_values("plot_id").str.startswith(holder + "_")]
    return sub


def test_the_stamp_is_on_exactly_the_flagged_rows_that_serve_a_number(eth):
    df = _build(eth)
    d = df["Derivation"]
    got = {h: list(_d(df, h)["Derivation"]) for h in ("H1", "H2", "H3", "H4", "H5")}
    coffee = _d(df, "H1")[_d(df, "H1").index.get_level_values("j") == "Coffee"]
    assert list(coffee["Derivation"]) == [KEY]
    teff = _d(df, "H1")[_d(df, "H1").index.get_level_values("j") == "Teff"]
    assert teff["Derivation"].isna().all()
    assert _d(df, "H2")["Derivation"].isna().all(), got      # disagreeing §4 lines
    assert _d(df, "H3")["Derivation"].isna().all(), got      # no §4 line
    assert list(_d(df, "H4")["Derivation"]) == [KEY], got    # a sale is a window value
    assert _d(df, "H5")["Derivation"].isna().all(), got      # serves nothing
    assert set(d.dropna()) == {KEY}


def test_the_stamp_moves_no_value_and_no_row(eth):
    """Built with and without the flag, every row and every value is equal."""
    with_flag = _build(eth)
    plain = _build(eth, colmap={k: v for k, v in COLMAP.items() if k != "pl_permanent"})
    assert "Derivation" not in plain.columns
    pd.testing.assert_frame_equal(with_flag.drop(columns="Derivation"), plain)


def test_derive_permanent_crop_window_is_the_identity(eth):
    q, qs, vs = pd.Series([1.0, nan]), pd.Series([nan, 2.0]), pd.Series([0.0, 5.0])
    out = eth.derive_permanent_crop_window(q, qs, vs)
    pd.testing.assert_series_equal(out["Quantity"], q, check_names=False)
    pd.testing.assert_series_equal(out["Quantity_sold"], qs, check_names=False)
    pd.testing.assert_series_equal(out["Value_sold"], vs, check_names=False)


def test_the_derivation_is_registered_and_resolves():
    from lsms_library.derivations import derivation_records
    rec = derivation_records("Ethiopia")[KEY]
    # 2011-12 since the W1 tree-crop ruling (PR #1257): W1's minted §12 rows
    # serve §12 Q3, a harvest "in the past 12 months".
    assert rec.waves == ["2011-12", "2018-19", "2021-22"]
    assert rec.columns == ["Quantity", "Quantity_sold", "Value_sold"]
    assert rec.resolve().__name__ == "derive_permanent_crop_window"
    assert rec.resolve("inputs").__name__ == "inputs_permanent_crop_window"


@pytest.mark.parametrize("wave", sorted(W45))
def test_w4_w5_scripts_read_the_permanent_flag(wave):
    src = (ROOT / wave / "_" / "crop_production.py").read_text()
    assert "pl_permanent='s4q15'" in src


@pytest.mark.parametrize("wave", ["2011-12", "2013-14", "2015-16"])
def test_w1_w3_scripts_do_not(wave):
    """W1-W3 put permanent crops in §12 (``sect12-sale-period``, GH #820);
    W1's minted §12 rows get the window key from §12 Q3, not from a §4 flag."""
    assert "pl_permanent" not in (ROOT / wave / "_" / "crop_production.py").read_text()


# ---------------------------------------------------------------------------
# Delivered
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


@pytest.mark.requires_s3
def test_the_stamp_rides_to_the_served_table(cp):
    """Measured cold 2026-10-23: 3,796 W4 and 3,875 W5 rows (607 / 554 with a
    positive Value_sold); none in W1-W3.  (3,713 / 3,723 before the same
    day's ruling that a row carrying a sale value is never dropped.)  The
    attrs summary counts them."""
    t = np.asarray(cp.index.get_level_values("t"), dtype=object)
    d = (cp["Derivation"].astype("string").str.contains(KEY, regex=False)
         .fillna(False).to_numpy(dtype=bool))
    got = {w: int((d & (t == w)).sum()) for w in sorted(set(t))}
    # W1 3,270 since the W1 tree-crop ruling (PR #1257): the minted §12 rows
    # that serve a Q3 harvest (3,249 of them also carry sect12-sale-period).
    assert got == {"2011-12": 3270, "2013-14": 0, "2015-16": 0,
                   "2018-19": 3796, "2021-22": 3875}, got
    assert cp.loc[d, ["Quantity", "Quantity_sold", "Value_sold"]].notna().any(axis=1).all()
    summ = cp.attrs["derivations"]["Ethiopia"][KEY]
    assert summ["rows"] == int(d.sum())
    assert summ["columns"] == ["Quantity", "Quantity_sold", "Value_sold"]


def _raw(wave, fn):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / fn), convert_categoricals=False)


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(W45))
def test_raw_oracle_stamped_rows_are_the_flagged_ones(cp, eth, wave):
    """Recomputed from the raw §4 / §9 files, independently of the builder:
    the served rows whose (plot, crop) has every §4 Q15 line = 1 and that
    serve a number are exactly the stamped rows (measured: 3,796 / 3,875, 0
    mismatches)."""
    from lsms_library.local_tools import format_id
    n = W45[wave]
    s4, s9 = _raw(wave, f"sect4_pp_w{n}.dta"), _raw(wave, f"sect9_ph_w{n}.dta")

    def pid(d):
        return (d["holder_id"].apply(format_id).astype(str) + "_"
                + d["parcel_id"].apply(format_id).astype(str) + "_"
                + d["field_id"].apply(format_id).astype(str))
    f4 = pd.DataFrame({"plot_id": pid(s4).values,
                       "code": pd.to_numeric(s4["s4q01b"], errors="coerce").values,
                       "yes": (pd.to_numeric(s4["s4q15"], errors="coerce") == 1).values}
                      ).dropna(subset=["code"])
    perm = f4.groupby(["plot_id", "code"])["yes"].all()
    perm = set(perm[perm].index)
    cmap = eth._eth_crop_label_map_for_wave(wave)
    h = pd.DataFrame({"plot_id": pid(s9).values,
                      "code": pd.to_numeric(s9["s9q00b"], errors="coerce").values}
                     ).dropna(subset=["code"])
    h["j"] = [cmap.get(int(c)) for c in h["code"]]
    h["perm"] = [(p, c) in perm for p, c in zip(h["plot_id"], h["code"])]
    pj = h.groupby(["plot_id", "j"])["perm"].agg(["all", "any"])
    expected = set(pj[pj["all"]].index)
    ambiguous = set(pj[pj["any"] & ~pj["all"]].index)
    sv = cp[np.asarray(cp.index.get_level_values("t"), dtype=object) == wave].reset_index()
    keys = list(zip(sv["plot_id"], sv["j"]))
    amb = np.array([k in ambiguous for k in keys])
    exp = (np.array([k in expected for k in keys])
           & sv[["Quantity", "Quantity_sold", "Value_sold"]].notna().any(axis=1).to_numpy())
    stamped = (sv["Derivation"] == KEY).fillna(False).to_numpy(dtype=bool)
    assert (stamped[~amb] == exp[~amb]).all(), int((stamped[~amb] != exp[~amb]).sum())
    assert int(stamped.sum()) > 3000


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(W45))
def test_derivation_inputs_are_the_raw_lines_and_join_to_the_served_rows(cp, wave):
    """Every §9 row (with its §4 flag) and every §11 line, original names,
    indexed (t, i, j); every stamped row's (t, i, j) is a §9 row flagged 1."""
    import lsms_library as ll
    raw = ll.Country("Ethiopia").derivation_inputs(KEY, wave=wave)
    n = W45[wave]
    assert list(raw.index.names) == ["t", "i", "j"]
    m = raw["module"]
    assert int((m == "sect9_ph").sum()) == len(_raw(wave, f"sect9_ph_w{n}.dta"))
    assert int((m == "sect11_ph").sum()) == len(_raw(wave, f"sect11_ph_w{n}.dta"))
    assert {"s9q04", "s9q05a", "s9q05b", "s4q15", "s11q02a1", "s11q07",
            "s11q11a", "s11q12"} <= set(raw.columns)
    s9 = raw[(m == "sect9_ph").to_numpy() & (raw["s4q15"] == 1).fillna(False).to_numpy()]
    served = cp[(cp["Derivation"] == KEY).fillna(False).to_numpy(dtype=bool)
                & (np.asarray(cp.index.get_level_values("t"), dtype=object) == wave)]
    sk = set(served.reset_index().set_index(["t", "i", "j"]).index)
    missing = sk - set(s9.index)
    assert not missing, (len(missing), len(sk))
