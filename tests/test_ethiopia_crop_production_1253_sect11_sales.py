"""Ethiopia ``crop_production`` §11 sale path: no fabricated zeros (GH #1253).

The ESS post-harvest §11 ("CROP DISPOSITION") is keyed (household, holder,
crop) and is attached to a crop's single §9 plot (the SALES GRAIN CAVEAT,
``ethiopia.crop_production_for_wave``).  Three behaviours of that path served
a sale of 0 the survey never recorded:

(a) the (holder, crop) aggregate was a bare ``.sum()``, so a group whose sale
    cells are ALL blank was served ``Value_sold = 0`` -- 46 / 9 / 0 / 9 / 8
    single-plot groups in W1-W5 (46 / 6 / 0 / 6 / 8 served rows: three W2 and
    three W4 groups' only §9 row has neither unit nor quantity and is removed
    by the build's empty-row filter, before and after);
(b) a "No" sold flag overwrote a value recorded beside it with 0 (one raw W1
    line, a positive quantity; it lands on no served single-plot row);
(c) a blank coded sale unit (W3-W5) was read as Kg, so a "No"'s zero
    quantity was attached to ``Quantity_sold`` only where the harvest happened
    to be recorded in Kg (542 / 816 / 819 served rows in W3 / W4 / W5).

Now: ``sum(min_count=1)`` (vectorised, never a per-group callable), a "No"
fills only a blank cell (``ethiopia._no_sale_reading``, the reading §12 already
used), and a line with no recorded sale unit attaches no quantity -- the rule
the §12 path in the same function follows (GH #820).

Tiers: the synthetic tests need no data; ``requires_s3`` tests read the raw
``.dta`` blobs or build the table.
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


@pytest.fixture(scope="module")
def eth():
    """Import ``Ethiopia/_/ethiopia.py`` as a module, wherever the tree is."""
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1253_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic: one holder per behaviour, every crop on ONE plot (so it attaches)
# ---------------------------------------------------------------------------

COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
    s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval",
)
nan = np.nan


def _harvest():
    rows = [  # household, holder, parcel, field, crop code, harvest qty
        ("HH1", "H1", 1, 1, 7, 50.0),    # teff: §11 Yes, both cells blank
        ("HH2", "H2", 1, 1, 2, 40.0),    # maize: §11 flag missing, blank
        ("HH3", "H3", 1, 1, 8, 30.0),    # wheat: §11 No, quantity 5 recorded
        ("HH4", "H4", 1, 1, 6, 20.0),    # sorghum: §11 No, both blank
        ("HH5", "H5", 1, 1, 1, 10.0),    # barley: §11 Yes, 4 kg for 80 Birr
        ("HH6", "H6", 1, 1, 3, 9.0),     # millet: two lines, one blank
    ]
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "qty"])


def _sale11():
    return pd.DataFrame(
        [("H1", 7, 1, nan, nan),
         ("H2", 2, nan, nan, nan),
         ("H3", 8, 2, 5.0, nan),
         ("H4", 6, 2, nan, nan),
         ("H5", 1, 1, 4.0, 80.0),
         ("H6", 3, 1, 2.0, 30.0),
         ("H6", 3, 1, nan, nan)],
        columns=["holder_id", "crop_code", "sold", "sqty", "sval"])


def _units(n, label="1. Kilogram"):
    return pd.Series([label] * n, dtype=object)


def _build(eth, sale_unit_labels=None, sale=None):
    h = _harvest()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return eth.crop_production_for_wave(
            "2015-16", h, None, _sale11() if sale is None else sale, COLMAP,
            unit_labels=_units(len(h)), sale_unit_labels=sale_unit_labels)


def _row(df, holder):
    sub = df[df.index.get_level_values("plot_id").str.startswith(holder + "_")]
    assert len(sub) == 1, holder
    return sub.iloc[0]


def test_an_all_blank_group_is_not_a_sale_of_zero(eth):
    """(a) A Yes with both cells blank, and a missing flag with both cells
    blank, are not "sold nothing": both stay NaN (a bare ``.sum()`` served 0)."""
    df = _build(eth)
    for holder in ("H1", "H2"):
        r = _row(df, holder)
        assert pd.isna(r["Value_sold"]), holder
        assert pd.isna(r["Quantity_sold"]), holder


def test_a_partly_blank_group_still_sums_what_was_recorded(eth):
    """``min_count=1`` changes only all-blank groups: H6's two lines (one
    blank) still serve the recorded 2 kg / 30 Birr."""
    r = _row(_build(eth), "H6")
    assert r["Value_sold"] == 30.0 and r["Quantity_sold"] == 2.0


def test_a_no_never_overwrites_a_recorded_value(eth):
    """(b) A positive quantity beside "No" is kept as recorded; the value the
    household did not report stays blank (it used to become 0 / 0)."""
    r = _row(_build(eth), "H3")
    assert r["Quantity_sold"] == 5.0
    assert pd.isna(r["Value_sold"])


def test_a_no_with_blank_cells_is_still_a_sale_of_zero(eth):
    """Control, true before and after: Q1 = No skips the sale questions, so its
    blank cells are 0.  Where the instrument fixes the sale unit at KG (W1/W2;
    no ``sale_unit_labels``) the zero quantity lands on the Kg harvest row."""
    r = _row(_build(eth), "H4")
    assert r["Value_sold"] == 0.0 and r["Quantity_sold"] == 0.0


def test_a_blank_coded_sale_unit_is_not_kg(eth):
    """(c) W3-W5 record the sale unit; behind a "No" it is blank (the skip).
    The "No" still serves Value_sold 0, but no Quantity_sold is attached to the
    Kg harvest row -- a blank unit is not Kg.  A recorded Kg sale still
    attaches (H5)."""
    labels = pd.Series(["1. Kilogram" if f == 1 else None
                        for f in _sale11()["sold"]], dtype=object)
    df = _build(eth, sale_unit_labels=labels)
    r4 = _row(df, "H4")
    assert r4["Value_sold"] == 0.0
    assert pd.isna(r4["Quantity_sold"])
    r5 = _row(df, "H5")
    assert r5["Quantity_sold"] == 4.0 and r5["Value_sold"] == 80.0


def test_the_sale_aggregation_calls_no_per_group_python(eth, monkeypatch):
    """The (holder, crop) aggregate is vectorised: a per-group callable makes
    pandas deepcopy ``attrs`` once per group (CLAUDE.md, GH #871).  Building
    with ``GroupBy.apply`` disabled must still work."""
    from pandas.core.groupby import generic

    def _no_apply(*a, **k):
        raise AssertionError("crop_production_for_wave used GroupBy.apply")

    monkeypatch.setattr(generic.DataFrameGroupBy, "apply", _no_apply)
    monkeypatch.setattr(generic.SeriesGroupBy, "apply", _no_apply)
    df = _build(eth)
    assert len(df) == 6


def test_no_sale_reading_is_shared_with_sect12(eth):
    """§11 and §12 read a "No" through one function, so they cannot drift."""
    q, v = eth._no_sale_reading([2, 2, 2, 1, None], [nan, 5.0, 0.0, nan, nan],
                                [nan, nan, nan, nan, nan])
    assert q.tolist()[:3] == [0.0, 5.0, 0.0]
    assert pd.isna(v.iloc[1])          # a positive quantity: the gap stays
    assert v.iloc[0] == 0.0 and v.iloc[2] == 0.0
    assert q.iloc[3:].isna().all() and v.iloc[3:].isna().all()
    d = eth.derive_sect12_sale([2, 2], [nan, 5.0], [nan, nan])
    assert d["Quantity_sold"].tolist() == [0.0, 5.0]


# ---------------------------------------------------------------------------
# Delivered: raw oracle and the served table
# ---------------------------------------------------------------------------

#: wave -> (§11 file, crop-code col, sold-flag col, value col, §9 file, §9 crop-code col)
RAW = {
    "2011-12": ("sect11_ph_w1.dta", "crop_code", "ph_s11q01", "ph_s11q04_a", "sect9_ph_w1.dta", "crop_code"),
    "2013-14": ("sect11_ph_w2.dta", "crop_code", "ph_s11q01", "ph_s11q04", "sect9_ph_w2.dta", "crop_code"),
    "2015-16": ("sect11_ph_w3.dta", "crop_code", "ph_s11q01", "ph_s11q04", "sect9_ph_w3.dta", "crop_code"),
    "2018-19": ("sect11_ph_w4.dta", "s11q01", "s11q07", "s11q12", "sect9_ph_w4.dta", "s9q00b"),
    "2021-22": ("sect11_ph_w5.dta", "s11q01b", "s11q07", "s11q12", "sect9_ph_w5.dta", "s9q00b"),
}


def _raw(wave, fn):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / fn), convert_categoricals=False)


def _all_blank_single_plot_groups(wave):
    """(holder, crop code) groups whose §11 Value_sold is blank on every line
    and that no "No" makes a zero, grown on exactly one §9 plot -- computed
    from the raw files, independently of ``ethiopia.py``'s §11 code."""
    from lsms_library.local_tools import format_id
    f11, cc, fl, v, f9, cc9 = RAW[wave]
    s, h = _raw(wave, f11), _raw(wave, f9)
    L = pd.DataFrame({
        "holder": s["holder_id"].apply(format_id).values,
        "code": pd.to_numeric(s[cc], errors="coerce").astype("Int64").values,
        "blank": (pd.to_numeric(s[v], errors="coerce").isna()
                  & ~(pd.to_numeric(s[fl], errors="coerce") == 2)).values,
    }).dropna(subset=["holder", "code"])
    allb = L.groupby(["holder", "code"])["blank"].all()
    plot = (h["holder_id"].apply(format_id).astype(str) + "_"
            + h["parcel_id"].apply(format_id).astype(str) + "_"
            + h["field_id"].apply(format_id).astype(str))
    H = pd.DataFrame({"holder": h["holder_id"].apply(format_id).values,
                      "code": pd.to_numeric(h[cc9], errors="coerce").astype("Int64").values,
                      "plot": plot.values}).dropna(subset=["code"])
    npl = H.groupby(["holder", "code"])["plot"].nunique()
    return {k for k in allb[allb].index if npl.get(k, 0) == 1}


@pytest.mark.requires_s3
def test_the_issue_counts_all_blank_single_plot_groups_from_raw():
    """The issue's 46 / 9 / 0 / 9 / 8, recomputed from the raw §9 / §11 files."""
    got = {w: len(_all_blank_single_plot_groups(w)) for w in RAW}
    assert got == {"2011-12": 46, "2013-14": 9, "2015-16": 0,
                   "2018-19": 9, "2021-22": 8}, got


@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(RAW))
def test_raw_oracle_no_all_blank_group_is_served_as_zero(cp, wave, eth):
    """Every served row of an all-blank single-plot group has Value_sold NaN.
    The served row's holder is the prefix of its ``plot_id``; its crop is
    matched through ``harmonize_crop`` (labels several codes fold onto are
    skipped -- the parquet does not say which code the row was)."""
    groups = _all_blank_single_plot_groups(wave)
    # the wave's own crop decode where the module has one (GH #1252)
    for_wave = getattr(eth, "_eth_crop_label_map_for_wave", None)
    cmap = for_wave(wave) if for_wave else eth._eth_crop_label_map()
    lab = pd.Series([cmap.get(int(c)) for _, c in groups], dtype=object)
    folded = {lb for lb, n in pd.Series(list(cmap.values())).value_counts().items() if n > 1}
    keys = {(hld, lb) for (hld, _), lb in zip(groups, lab) if lb not in folded}
    sub = cp[cp.index.get_level_values("t") == wave].reset_index()
    sub["holder"] = sub["plot_id"].str.split("_").str[0]
    hit = sub[[k in keys for k in zip(sub["holder"], sub["j"])]]
    assert hit["Value_sold"].isna().all(), hit["Value_sold"].value_counts(dropna=False)


@pytest.mark.requires_s3
def test_served_zero_sale_counts_per_wave(cp):
    """Measured 2026-10-23, cold, ``627d3cfdf`` -> GH #1253.  ``Value_sold == 0``
    rows: 3,187 / 7,402 / 6,975 / 4,378 / 2,727 -> 3,141 / 7,396 / 6,975 /
    4,372 / 2,719 (sub-fix (a)).  ``Quantity_sold == 0`` rows: 3,187 / 2,585 /
    542 / 816 / 819 -> 3,142 / 2,581 / 0 / 0 / 0 ((a) in W1/W2; (c) in W3-W5,
    where every remaining "No" has no sale unit).  No row added or dropped by
    GH #1253 itself; the later empty-row-filter ruling (2026-10-23) keeps the
    rows that carry a sale value, among them 338 / 70 / 141 / 245 W2-W5 rows
    whose "No" serves Value_sold 0 -- hence the Value_sold counts below and
    86,403 rows."""
    t = cp.index.get_level_values("t")
    waves = sorted(set(t))
    v0 = {w: int(((cp["Value_sold"] == 0).fillna(False) & (t == w)).sum()) for w in waves}
    q0 = {w: int(((cp["Quantity_sold"] == 0).fillna(False) & (t == w)).sum()) for w in waves}
    # W1 3,141 / 3,142 -> 5,728 / 5,728 and 86,403 -> 89,840 rows on the W1
    # tree-crop ruling (PR #1257): the 3,437 rows minted from §12 include
    # 2,587 whose §12 "No" serves Value_sold 0 (2,586 with Quantity_sold 0:
    # W1's Q7 is KG, the minted row's u).  No §11 cell moved.
    assert v0 == {"2011-12": 5728, "2013-14": 7734, "2015-16": 7045,
                  "2018-19": 4513, "2021-22": 2964}, v0
    assert q0 == {"2011-12": 5728, "2013-14": 2581, "2015-16": 0,
                  "2018-19": 0, "2021-22": 0}, q0
    assert len(cp) == 89840
