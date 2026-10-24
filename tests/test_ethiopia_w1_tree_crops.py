"""Ethiopia W1 tree / fruit / root crops served as rows (GH #820 ruling).

Rulings (@ligon, 2026-10-23, on PR #1257):

1. "W1 tree crops: serve them as new rows.  Harvest comes from §12 Q3 and the
   sale from Q6-Q8.  Rows are keyed to the crop's §4 field where it is unique
   and stamped with the window derivation."
2. "Attach rules: keep both.  A several-plot crop gets no §12 sale, and §11
   wins on a collision."
3. The empty-row filter keeps a row carrying a sale value -- landed in PR #1274
   (``0663ac4fe``); pinned in ``test_ethiopia_crop_production_empty_row_filter.py``.

W1's §9 is headed "EXCLUDE TREE/ROOT CROPS", so these crops had no row: 6,134
§12 lines (1,679 with a positive sale, 1.83M Birr) were counted, not served.
The minted row's ``Quantity`` is §12 Q3, "harvested in the past 12 months" (KG),
so it carries ``12-month-harvest-window`` (waves widened to 2011-12);
its sale is attached by the reviewed §12 attach and carries
``sect12-sale-period``; a row with both carries both, joined by ``'+'``.

Privacy: assertion messages carry counts only.

Tiers: synthetic tests need no data; ``requires_s3`` tests read the raw files
or build the table.
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
SALE = "Ethiopia::crop_production::sect12-sale-period"
WINDOW = "Ethiopia::crop_production::12-month-harvest-window"
BOTH = WINDOW + "+" + SALE
nan = np.nan


@pytest.fixture(scope="module")
def eth():
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_w1_trees_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic: one holder per class
# ---------------------------------------------------------------------------

COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
    pl_crop_code="crop_code", pl_month="month",
    s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval",
    s12_crop_code="crop_code", s12_sold_flag="q06", s12_qty="q07",
    s12_value="q08", s12_harvest="q03",
)


def _harvest():   # §9: field crops only (W1 excludes tree / root crops)
    rows = [("HH1", "H1", 1, 1, 7, 50.0),     # teff
            ("HH6", "H6", 1, 1, 56, 4.0)]     # kale -- 'Leafy Greens'
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "qty"])


def _planting():  # §4: crops by field
    rows = [("HH1", "H1", 1, 1, 7, 2), ("HH1", "H1", 1, 2, 72, 1),   # coffee, ONE field
            ("HH2", "H2", 1, 1, 72, 1), ("HH2", "H2", 1, 2, 72, 1),  # coffee, TWO fields
            ("HH4", "H4", 1, 1, 42, 3),                              # banana, one field
            ("HH5", "H5", 1, 1, 74, 3),                              # enset, one field
            ("HH6", "H6", 1, 1, 56, 1), ("HH6", "H6", 1, 1, 69, 1),  # kale + spinach
            ("HH7", "H7", 1, 1, 71, 1),                              # chat
            ("HH8", "H8", 1, 1, 46, 1)]                              # mango
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "month"])


def _sale11():
    return pd.DataFrame([("H1", 7, 1, 20.0, 100.0), ("H7", 71, 1, 1.0, 9.0)],
                        columns=["holder_id", "crop_code", "sold", "sqty", "sval"])


def _sale12():
    return pd.DataFrame(
        [("HH1", "H1", 72, 30.0, 1, 10.0, 60.0),   # one field   -> minted, both keys
         ("HH2", "H2", 72, 40.0, 1, 5.0, 50.0),    # two fields  -> no row
         ("HH3", "H3", 84, 9.0, 1, 1.0, 5.0),      # no §4 field -> no row
         ("HH4", "H4", 42, 0.0, 2, nan, nan),      # Q3 = 0, No  -> 0 Kg, sale 0
         ("HH5", "H5", 74, nan, nan, nan, nan),    # nothing     -> no row
         ("HH6", "H6", 69, 3.0, 1, 1.0, 2.0),      # spinach folds onto the kale row
         ("HH7", "H7", 71, 8.0, 1, 2.0, 20.0),     # in §11      -> §11 wins, no row
         ("HH8", "H8", 46, nan, 1, 2.0, 15.0)],    # sale only   -> minted, sale key
        columns=["household_id", "holder_id", "crop_code", "q03", "q06", "q07", "q08"])


def _build(eth, colmap=COLMAP):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = eth.crop_production_for_wave("2011-12", _harvest(), _planting(),
                                          _sale11(), colmap, sale12=_sale12())
    msgs = [str(w.message) for w in caught if w.category.__name__ == "Sect12SaleWarning"]
    return df, msgs


def _rows(df, holder):
    return df[df.index.get_level_values("plot_id").str.startswith(holder + "_")]


def test_a_crop_on_one_field_becomes_a_row_with_q3_and_its_sale(eth):
    df, _ = _build(eth)
    r = _rows(df, "H1")
    r = r[r.index.get_level_values("j") == "Coffee"]
    n = int(len(r))
    assert n == 1, n
    assert list(r.index.get_level_values("plot_id")) == ["H1_1_2"]   # the §4 field
    assert list(r.index.get_level_values("u")) == ["Kg"]
    assert r["Quantity"].tolist() == [30.0]
    assert r["Value_sold"].tolist() == [60.0] and r["Quantity_sold"].tolist() == [10.0]
    assert r["planting_month"].notna().all()          # joined from its §4 line
    assert r["harvest_month"].isna().all()            # §12 asks no harvest month
    assert list(r["Derivation"]) == [BOTH]


def test_several_fields_no_field_nothing_and_section_11_get_no_row(eth):
    df, msgs = _build(eth)
    for h in ("H2", "H3", "H5"):
        n = int(len(_rows(df, h)))
        assert n == 0, (h, n)
    chat = _rows(df, "H7")
    n = int(len(chat))
    assert n == 0, n                                   # §11 wins; no §9 row either
    m = msgs[0]
    for cls, lines in [("several_s4_fields", 1), ("no_s4_field", 1),
                       ("nothing_to_serve", 1), ("key_occupied", 1)]:
        assert f"{cls} {lines}/" in m, cls
    assert "ROWS MINTED from §12 for crops with no §9 row: 3 of 7 candidate" in m


def test_a_recorded_zero_harvest_is_served_and_a_no_is_a_sale_of_zero(eth):
    df, _ = _build(eth)
    b = _rows(df, "H4")
    assert b["Quantity"].tolist() == [0.0]
    assert b["Value_sold"].tolist() == [0.0] and b["Quantity_sold"].tolist() == [0.0]
    assert list(b["Derivation"]) == [BOTH]


def test_a_folded_label_never_duplicates_a_key(eth):
    """Spinach (69) on the field where §9 has kale (56): both are 'Leafy
    Greens', so a minted row would duplicate the §9 row's key."""
    df, _ = _build(eth)
    g = _rows(df, "H6")
    assert g["Quantity"].tolist() == [4.0]             # §9's kale row only
    assert g["Derivation"].isna().all()
    n_dup = int(df.index.duplicated().sum())
    assert n_dup == 0, n_dup


def test_a_sale_only_line_carries_only_the_sale_key(eth):
    df, _ = _build(eth)
    m = _rows(df, "H8")
    assert m["Quantity"].isna().all() and m["Value_sold"].tolist() == [15.0]
    assert list(m["Derivation"]) == [SALE]


def test_minting_moves_no_pre_existing_row(eth):
    """Built with and without Q3 named: every §9 row is identical."""
    with_mint, _ = _build(eth)
    plain, _ = _build(eth, {k: v for k, v in COLMAP.items() if k != "s12_harvest"})
    n_plain = int(len(plain))
    assert n_plain == 2, n_plain
    pd.testing.assert_frame_equal(with_mint.loc[plain.index], plain)


def test_without_q3_no_wave_mints(eth):
    plain, msgs = _build(eth, {k: v for k, v in COLMAP.items() if k != "s12_harvest"})
    assert "ROWS MINTED" not in msgs[0]


def test_the_w1_script_names_q3_and_only_w1_does():
    assert "s12_harvest='ph_s12q03'" in (ROOT / "2011-12" / "_" / "crop_production.py").read_text()
    for w in ("2013-14", "2015-16", "2018-19", "2021-22"):
        assert "s12_harvest" not in (ROOT / w / "_" / "crop_production.py").read_text(), w


def test_the_window_key_is_widened_to_w1():
    from lsms_library.derivations import derivation_records
    derivation_records.cache_clear()
    rec = derivation_records("Ethiopia")[WINDOW]
    assert rec.waves == ["2011-12", "2018-19", "2021-22"]
    assert rec.resolve("inputs").__name__ == "inputs_harvest_window"


# ---------------------------------------------------------------------------
# Delivered + raw oracle
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


def _w1(cp):
    return cp[cp.index.get_level_values("t") == "2011-12"]


@pytest.mark.requires_s3
def test_w1_serves_the_minted_rows(cp):
    """Measured cold 2026-10-23: 3,437 minted wave rows (3,270 with a Q3
    harvest, 345,805 kg; 828 with a positive sale, 484,385 Birr)."""
    w1 = _w1(cp)
    d = w1["Derivation"].astype("string")
    win = d.str.contains(WINDOW, regex=False).fillna(False).to_numpy(dtype=bool)
    n_window = int(win.sum())
    assert n_window == 3270, n_window
    kg = float(w1.loc[win, "Quantity"].sum())
    assert round(kg) == 345805, kg
    n_both = int((d == BOTH).fillna(False).sum())
    assert n_both == 3249, n_both
    n_dup = int(cp.index.duplicated().sum())
    assert n_dup == 0, n_dup


@pytest.mark.requires_s3
def test_raw_oracle_minted_rows_are_q3_at_the_unique_s4_field(cp):
    """Recompute, from the raw §12 / §4 files, every stamped W1 row's harvest
    and key: Q3 of its (holder, crop), on that crop's only §4 field."""
    from lsms_library import Country
    from lsms_library.local_tools import format_id
    c = Country("Ethiopia")
    raw = c.derivation_inputs(WINDOW, wave="2011-12")
    s12 = raw[raw["module"] == "sect12_ph"].reset_index()
    s4 = raw[raw["module"] == "sect4_pp"].reset_index()
    s4["plot_id"] = (s4["holder_id"].apply(format_id).astype(str) + "_"
                     + s4["parcel_id"].apply(format_id).astype(str) + "_"
                     + s4["field_id"].apply(format_id).astype(str))
    s4["h"] = s4["holder_id"].apply(format_id).astype(str)
    nf = s4.groupby(["h", "crop_code"])["plot_id"].nunique()
    one = s4.drop_duplicates(["h", "crop_code"]).set_index(["h", "crop_code"])["plot_id"]
    s12["h"] = s12["holder_id"].apply(format_id).astype(str)
    s12 = s12.dropna(subset=["crop_code"])
    s12 = s12[~s12.duplicated(["h", "crop_code"], keep=False)]
    key = pd.MultiIndex.from_frame(s12[["h", "crop_code"]])
    s12["plot_id"] = one.reindex(key).to_numpy()
    s12 = s12[(nf.reindex(key).to_numpy() == 1)]
    want = s12.set_index(["i", "plot_id", "j"])["ph_s12q03"].astype(float)
    # two crop codes folded onto one label on one field are never minted
    want = want[~want.index.duplicated(keep=False)]

    w1 = _w1(cp)
    st = w1[w1["Derivation"].astype("string").str.contains(WINDOW, regex=False).fillna(False)]
    got = st.reset_index().set_index(["i", "plot_id", "j"])["Quantity"].astype(float)
    n_missing = int((~got.index.isin(want.index)).sum())
    assert n_missing == 0, n_missing
    diff = got - want.reindex(got.index)
    n_bad = int((diff.abs() > 1e-9).sum())
    assert n_bad == 0, n_bad


@pytest.mark.requires_s3
def test_the_window_inputs_join_to_the_served_w1_rows(cp):
    import lsms_library as ll
    raw = ll.Country("Ethiopia").derivation_inputs(WINDOW, wave="2011-12")
    assert list(raw.index.names) == ["t", "i", "j"]
    assert set(raw["module"]) == {"sect12_ph", "sect4_pp"}
    w1 = _w1(cp)
    st = w1[w1["Derivation"].astype("string").str.contains(WINDOW, regex=False).fillna(False)]
    sk = set(st.reset_index().set_index(["t", "i", "j"]).index)
    n_missing = len(sk - set(raw.index))
    assert n_missing == 0, (n_missing, len(sk))
