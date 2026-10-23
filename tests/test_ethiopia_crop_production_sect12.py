"""Ethiopia ``crop_production`` reads the ESS §12 tree/fruit/root sale module (GH #820).

In ESS1-ESS3 the post-harvest §11 ("CROP DISPOSITION") is headed *EXCLUDE
TREE/FRUIT/VEGETABLE/ROOT CROPS*; those crops' sales are in §12
(``sect12_ph_w{1,2,3}.dta``), which ``crop_production`` did not read.  In
2015-16 §12 holds 2,627 positive sale values against §11's 1,984, so Coffee,
Chat, Banana, Mango ... had essentially no ``Value_sold`` in W1-W3.

§12 is keyed (household, holder, crop), the grain of §11, so it is attached by
the same SALES GRAIN CAVEAT rule (a holder-level sale lands only on a crop
grown on exactly one §9 plot).  It never adds a row and never overwrites a
cell §11 filled.  ``ethiopia._attach_sect12_sales`` documents the classes of
§12 line it does NOT attach; each is counted.

W4 (2018-19) / W5 (2021-22) ship no ``sect12_ph`` file: ESS4 / ESPS-5 fold the
module into §11 (W4 questionnaire: "LIST ALL CROPS HARVESTED ... FOR
PERMANENT CROPS PLEASE INCLUDE ...").  That is VERIFIED below against the raw
files, not assumed, and those two waves' scripts are unchanged.

Tiers: the synthetic and static tests need no data; ``requires_s3`` tests
read the raw ``.dta`` blobs or build the table.
"""
from __future__ import annotations

import importlib.util
import re
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
W13 = {"2011-12": 1, "2013-14": 2, "2015-16": 3}
W45 = {"2018-19": 4, "2021-22": 5}


@pytest.fixture(scope="module")
def eth():
    """Import ``Ethiopia/_/ethiopia.py`` as a module, wherever the tree is."""
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_sect12_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic: the attachment rules, one holder per class
# ---------------------------------------------------------------------------

COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
    s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval",
    s12_crop_code="crop_code", s12_sold_flag="q06", s12_qty="q07",
    s12_value="q08",
)


def _harvest():
    rows = [  # household, holder, parcel, field, crop code, kg
        ("HH1", "H1", 1, 1, 72, 10.0),   # coffee, one plot     -> attached
        ("HH1", "H1", 1, 2, 7, 50.0),    # teff, sold in §11
        ("HH2", "H2", 1, 1, 72, 5.0),    # coffee on two plots  -> several
        ("HH2", "H2", 1, 2, 72, 6.0),
        ("HH3", "H3", 1, 1, 56, 2.0),    # kale + spinach fold onto ONE key
        ("HH3", "H3", 1, 1, 69, 3.0),    #   'Leafy Greens'     -> folded
        ("HH4", "H4", 1, 1, 42, 7.0),    # banana, also in §11  -> collides
        ("HH5", "H5", 1, 1, 46, 4.0),    # mango, two §12 lines -> repeated
        ("HH6", "H6", 1, 1, 74, 9.0),    # enset, Q6 No, blank  -> 0
        ("HH7", "H7", 1, 1, 71, 1.0),    # chat, Q6 No, Q8 > 0  -> kept
        ("HH9", "H9", 1, 1, 76, np.nan), # sugar cane, NO unit, NO quantity:
    ]                                    #   attached, then dropped (class 7)
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "qty"])


def _sale11():
    return pd.DataFrame(
        [("H1", 7, 1, 20.0, 100.0), ("H4", 42, 1, 1.0, 11.0)],
        columns=["holder_id", "crop_code", "sold", "sqty", "sval"])


def _sale12():
    nan = np.nan
    return pd.DataFrame(
        [("H1", 72, 1, 3.0, 60.0),
         ("H2", 72, 1, 2.0, 40.0),
         ("H3", 56, 1, 1.0, 5.0),
         ("H4", 42, 1, 9.0, 99.0),
         ("H5", 46, 1, 1.0, 8.0),
         ("H5", 46, 1, 1.0, 9.0),
         ("H6", 74, 2, nan, nan),
         ("H7", 71, 2, nan, 30.0),
         ("H8", 84, 1, 2.0, 50.0),     # no §9 row at all
         ("H9", 76, 1, 2.0, 25.0),     # its only §9 row is then dropped
         ("", nan, nan, nan, nan)],    # blank roster line
        columns=["holder_id", "crop_code", "q06", "q07", "q08"])


def _harvest_units():
    """Every §9 row is in kilograms except H9's, which records no unit."""
    h = _harvest()
    return pd.Series(np.where(h["holder_id"] == "H9", None, "1. Kilogram"),
                     dtype=object)


def _build(eth, with12=True, unit_labels=None, sale12=None):
    kw = {}
    if with12:
        kw = dict(sale12=_sale12() if sale12 is None else sale12,
                  sale12_unit_labels=unit_labels)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = eth.crop_production_for_wave("2011-12", _harvest(), None, _sale11(),
                                          COLMAP, unit_labels=_harvest_units(),
                                          **kw)
    msgs = [str(w.message) for w in caught
            if w.category.__name__ == "Sect12SaleWarning"]
    return df, msgs


def _row(df, holder, crop_label):
    sub = df[df.index.get_level_values("plot_id").str.startswith(holder + "_")
             & (df.index.get_level_values("j") == crop_label)]
    assert len(sub) >= 1, (holder, crop_label)
    return sub


def test_sect12_fills_only_the_unambiguous_single_plot_pair(eth):
    df, msgs = _build(eth)
    coffee = _row(df, "H1", "Coffee")
    assert coffee["Value_sold"].tolist() == [60.0]
    assert coffee["Quantity_sold"].tolist() == [3.0]        # Kg sale on a Kg row
    # several plots: the SALES GRAIN CAVEAT, never split
    assert _row(df, "H2", "Coffee")["Value_sold"].isna().all()
    # folded key: kale/spinach -> one 'Leafy Greens' key, not attributable
    assert _row(df, "H3", "Leafy Greens")["Value_sold"].isna().all()
    # repeated (holder, crop) lines: ambiguous, neither picked nor summed
    assert _row(df, "H5", "Mango")["Value_sold"].isna().all()
    # Q6 = No with Q7/Q8 blank is a sale of 0; a positive Q8 next to No is KEPT
    assert _row(df, "H6", "Enset")["Value_sold"].tolist() == [0.0]
    assert _row(df, "H6", "Enset")["Quantity_sold"].tolist() == [0.0]
    assert _row(df, "H7", "Chat/Kat")["Value_sold"].tolist() == [30.0]
    assert len(msgs) == 1


def test_sect11_wins_a_collision_and_is_never_summed_with_sect12(eth):
    df, _ = _build(eth)
    banana = _row(df, "H4", "Banana")
    assert banana["Value_sold"].tolist() == [11.0]           # §11, not 99 or 110
    assert _row(df, "H1", "Teff")["Value_sold"].tolist() == [100.0]


def test_sect12_adds_no_row_and_moves_no_other_cell(eth):
    """§12 adds no §9 row and moves no cell §11 filled.  Since the 2026-10-23
    ruling that a row carrying a sale value is never dropped, a §9 row that
    would have been dropped as EMPTY (no unit, no quantity) is kept when §12
    gives it a value -- H9, the one such row here; every other row is equal."""
    base, _ = _build(eth, with12=False)
    new, _ = _build(eth)
    h9 = new.index.get_level_values("plot_id").str.startswith("H9_")
    assert int(h9.sum()) == 1
    new = new[~h9]
    assert base.index.equals(new.index)
    other = [c for c in base.columns if c not in ("Quantity_sold", "Value_sold")]
    pd.testing.assert_frame_equal(base[other], new[other])
    for col in ("Quantity_sold", "Value_sold"):
        was = base[col].notna()
        pd.testing.assert_series_equal(base.loc[was, col], new.loc[was, col])


def test_every_unattached_class_is_counted_in_the_warning(eth):
    """Eight classes partition the lines read (the builder asserts it)."""
    _, msgs = _build(eth)
    m = msgs[0]
    assert "read 11 lines" in m
    assert "attached and served 4 lines" in m          # H1, H6, H7, H9
    for cls, lines in [("placeholder", 1), ("repeated", 2), ("collides_s11", 1),
                       ("no_s9_row", 1), ("several_s9_plots", 1),
                       ("folded_key", 1), ("attached_then_dropped", 0)]:
        assert re.search(rf"\b{cls} {lines}/", m), (cls, m)
    # H9 used to be class 7 (1/1/25): its only §9 row has no unit and no
    # quantity, and the empty-row filter ignored Value_sold.  A row carrying a
    # sale value is never dropped now (ruling 2026-10-23).
    assert "attached_then_dropped 0/0/0" in m
    assert "kept as reported: 1" in m


def test_a_row_carrying_a_sect12_sale_is_no_longer_dropped(eth):
    """Former class 7 (GH #820 round 2): H9's §12 sale attaches to its only §9
    row, which records neither a unit nor a quantity.  The empty-row filter
    used to ignore Value_sold and remove the row; since the 2026-10-23 ruling
    a row carrying a sale value is never dropped, so H9 is served with its
    25 Birr, the no-unit tag 'Other (Specify)' and the §12 stamp.  Without
    §12 the row carries nothing and is still dropped."""
    base, _ = _build(eth, with12=False)
    new, _ = _build(eth)
    assert not base.index.get_level_values("plot_id").str.startswith("H9_").any()
    h9 = _row(new, "H9", "Sugar Cane")
    assert h9["Value_sold"].tolist() == [25.0]
    assert h9["Quantity"].isna().all() and h9["Quantity_sold"].isna().all()
    assert list(h9.index.get_level_values("u")) == ["Other (Specify)"]
    assert list(h9["Derivation"]) == ["Ethiopia::crop_production::sect12-sale-period"]


def test_derivation_stamp_is_on_exactly_the_rows_sect12_filled(eth):
    base, _ = _build(eth, with12=False)
    new, _ = _build(eth)
    key = eth.SECT12_SALE_DERIVATION
    assert key == "Ethiopia::crop_production::sect12-sale-period"
    # H9's row exists only because §12 gave it a value (ruling 2026-10-23: a
    # row carrying a sale value is never dropped); it carries the key.
    h9 = new.index.get_level_values("plot_id").str.startswith("H9_")
    assert list(new.loc[h9, "Derivation"]) == [key]
    new = new[~h9]
    filled = ((base["Value_sold"].isna() & new["Value_sold"].notna())
              | (base["Quantity_sold"].isna() & new["Quantity_sold"].notna()))
    stamped = new["Derivation"] == key
    assert (stamped.fillna(False).to_numpy(dtype=bool)
            == filled.to_numpy(dtype=bool)).all()
    assert set(new["Derivation"].dropna()) == {key}
    assert int(filled.sum()) == 3                         # H1, H6, H7
    # the section-11 rows carry no key
    assert _row(new, "H1", "Teff")["Derivation"].isna().all()
    assert _row(new, "H4", "Banana")["Derivation"].isna().all()
    # a wave without section 12 gets no Derivation column at all
    assert "Derivation" not in base.columns


def test_derive_sect12_sale_is_the_identity_but_for_a_blank_no(eth):
    nan = np.nan
    out = eth.derive_sect12_sale([1, 2, 2, 2, nan, 1],
                                 [3.0, nan, nan, 4.0, 5.0, nan],
                                 [60.0, nan, 30.0, nan, 7.0, nan])
    assert out["Quantity_sold"].tolist()[:2] == [3.0, 0.0]
    assert out["Value_sold"].tolist()[:3] == [60.0, 0.0, 30.0]
    assert pd.isna(out["Quantity_sold"].iloc[2])          # No + Q8 > 0: as reported
    assert pd.isna(out["Value_sold"].iloc[3])             # No + Q7 > 0: as reported
    assert out.iloc[4].tolist() == [5.0, 7.0]             # flag missing: as reported
    assert out.iloc[5].isna().all()                       # Yes, blank: stays blank


def test_the_derivation_is_registered_and_resolves(eth):
    from lsms_library.derivations import derivation_records
    derivation_records.cache_clear()
    rec = derivation_records("Ethiopia")[eth.SECT12_SALE_DERIVATION]
    assert rec.waves == ["2011-12", "2013-14", "2015-16"]
    assert rec.columns == ["Quantity_sold", "Value_sold"]
    assert rec.function == "lsms_library.countries.Ethiopia._.ethiopia:derive_sect12_sale"
    assert rec.inputs == "lsms_library.countries.Ethiopia._.ethiopia:inputs_sect12_sale"
    assert "questionnaire" in rec.bases and "modelling-choice" in rec.bases
    assert rec.resolve().__name__ == "derive_sect12_sale"
    assert rec.resolve("inputs").__name__ == "inputs_sect12_sale"


def test_a_sect12_sale_without_a_unit_attaches_value_but_no_quantity(eth):
    """W3 records Q7's unit (ph_s12q0b).  No unit is NOT assumed to be Kg."""
    labels = pd.Series(["1. Kilogram", "1. Kilogram", "1. Kilogram",   # H1 H2 H3
                        "1. Kilogram", "1. Kilogram", "1. Kilogram",   # H4 H5 H5
                        None, None, "1. Kilogram", "1. Kilogram",      # H6 H7 H8 H9
                        None])                                         # blank
    s12 = _sale12()
    df, _ = _build(eth, unit_labels=labels, sale12=s12)
    assert _row(df, "H1", "Coffee")["Quantity_sold"].tolist() == [3.0]
    s12.loc[0, "q07"] = 3.0
    labels.iloc[0] = None
    df, _ = _build(eth, unit_labels=labels, sale12=s12)
    coffee = _row(df, "H1", "Coffee")
    assert coffee["Value_sold"].tolist() == [60.0]
    assert coffee["Quantity_sold"].isna().all()


# ---------------------------------------------------------------------------
# Static: which waves read §12, and the crop dictionary
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wave,n", sorted(W13.items()))
def test_w1_w3_scripts_read_sect12(wave, n):
    src = (ROOT / wave / "_" / "crop_production.py").read_text()
    assert f"sect12_ph_w{n}.dta" in src
    assert "sale12=sale12" in src
    assert (ROOT / wave / "Data" / f"sect12_ph_w{n}.dta.dvc").exists()


@pytest.mark.parametrize("wave,n", sorted(W45.items()))
def test_w4_w5_ship_no_sect12_ph_and_do_not_read_one(wave, n):
    assert not list((ROOT / wave / "Data").glob("sect12_ph*"))
    src = (ROOT / wave / "_" / "crop_production.py").read_text()
    assert "sect12" not in src and "sale12" not in src


def test_harmonize_crop_decodes_every_sect12_code_label_in_w1(eth):
    """103 'WEPAL' / 105 'BUYA' are the W1 §12 roster's two undecoded codes."""
    m = eth._eth_crop_label_map()
    assert m[103] == "Wepal" and m[105] == "Buya"


# ---------------------------------------------------------------------------
# Raw sources: the issue's numbers, and the W4/W5 verification
# ---------------------------------------------------------------------------

def _raw(wave, fn):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / fn), convert_categoricals=False)


@pytest.mark.requires_s3
def test_issue_counts_2015_16_sect12_2627_vs_sect11_1984():
    s12 = _raw("2015-16", "sect12_ph_w3.dta")
    s11 = _raw("2015-16", "sect11_ph_w3.dta")
    assert int((pd.to_numeric(s12["ph_s12q08"], errors="coerce") > 0).sum()) == 2627
    assert int((pd.to_numeric(s11["ph_s11q04"], errors="coerce") > 0).sum()) == 1984


@pytest.mark.requires_s3
def test_w4_w5_sect11_carries_the_crops_only_sect12_carried_in_w1_w3():
    """Codes recorded in W1-W3 §12 and NEVER in W1-W3 §11 (avocado, lemon,
    orange, papaya, ...) are absent from W1-W3 §11 by construction, and
    present -- with positive sales -- in W4/W5 §11.  And the share of §9
    (holder, crop) pairs of those crops that have a §11 record is 0 in W1-W3
    and ~0.74 / ~0.80 in W4 / W5 (field crops: 0.90-0.98 in every wave)."""
    from lsms_library.local_tools import format_id
    s12_codes, s11_codes = set(), set()
    for w, n in W13.items():
        s12_codes |= set(pd.to_numeric(_raw(w, f"sect12_ph_w{n}.dta")["crop_code"],
                                       errors="coerce").dropna().astype(int))
        s11_codes |= set(pd.to_numeric(_raw(w, f"sect11_ph_w{n}.dta")["crop_code"],
                                       errors="coerce").dropna().astype(int))
    only12 = s12_codes - s11_codes
    assert len(only12) == 24, sorted(only12)

    s9_crop = {1: "crop_code", 2: "crop_code", 3: "crop_code", 4: "s9q00b", 5: "s9q00b"}
    s11_crop = {1: "crop_code", 2: "crop_code", 3: "crop_code", 4: "s11q01", 5: "s11q01b"}
    share = {}
    for w, n in {**W13, **W45}.items():
        s9 = _raw(w, f"sect9_ph_w{n}.dta")
        s11 = _raw(w, f"sect11_ph_w{n}.dta")
        k9 = pd.DataFrame({"h": s9["holder_id"].apply(format_id).astype(str),
                           "c": pd.to_numeric(s9[s9_crop[n]], errors="coerce")}
                          ).dropna().drop_duplicates()
        k11 = pd.DataFrame({"h": s11["holder_id"].apply(format_id).astype(str),
                            "c": pd.to_numeric(s11[s11_crop[n]], errors="coerce")}
                           ).dropna()
        k9 = k9[k9["c"].isin(only12)]
        hit = pd.MultiIndex.from_frame(k9).isin(pd.MultiIndex.from_frame(k11))
        share[w] = float(hit.mean())
        if n >= 4:
            val = pd.to_numeric(s11["s11q12"], errors="coerce")
            code = pd.to_numeric(s11[s11_crop[n]], errors="coerce")
            sold = set(code[(val > 0) & code.isin(only12)].astype(int))
            assert len(sold) >= 12, (w, sorted(sold))
    for w in W13:
        assert share[w] == 0.0, share
    for w in W45:
        assert share[w] > 0.7, share


# ---------------------------------------------------------------------------
# Delivered: the served table (builds Ethiopia crop_production)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


@pytest.mark.requires_s3
def test_tree_crops_carry_sale_values_in_w2_w3(cp):
    """Coffee / Chat had 0-1 positive Value_sold rows per wave before GH #820."""
    v = cp["Value_sold"]
    t = cp.index.get_level_values("t")
    j = cp.index.get_level_values("j")
    for wave in ("2013-14", "2015-16"):
        for crop in ("Coffee", "Chat/Kat", "Banana", "Hops (gesho)"):
            n = int(((v > 0) & (t == wave) & (j == crop)).sum())
            assert n >= 100, (wave, crop, n)


@pytest.mark.requires_s3
def test_the_stamp_rides_to_the_served_table(cp):
    """Measured 2026-10-23: rows carrying the key, after the core collapse.
    W1 544, W2 4,463, W3 4,495 (W2: 4,748 in the wave parquet, 285 of them
    on identical duplicate §9 rows the core collapse de-duplicates
    losslessly); none in W4/W5.  Equal, row for row, to the cells §12 filled
    against development (9,502).  Every stamped row carries a sale cell, and
    the attrs summary counts exactly the stamped rows."""
    key = "Ethiopia::crop_production::sect12-sale-period"
    window = "Ethiopia::crop_production::permanent-crop-12-month-window"
    d = cp["Derivation"].astype("string")
    t = cp.index.get_level_values("t")
    carries = d.str.contains(key, regex=False).fillna(False)
    got = {w: int((carries & (t == w)).sum()) for w in sorted(set(t))}
    # 4,463 / 4,495 before the 2026-10-23 empty-row-filter ruling: W2's 209 and
    # W3's 62 stamped rows with no unit and no quantity are no longer dropped.
    # W1 544 -> 3,960 on the W1 tree-crop ruling (PR #1257): the 3,416 rows
    # minted from §12 that serve a §12 sale (3,437 minted; 21 serve Q3 only).
    assert got == {"2011-12": 3960, "2013-14": 4672, "2015-16": 4557,
                   "2018-19": 0, "2021-22": 0}, got
    stamped = carries.to_numpy(dtype=bool)
    assert (cp.loc[stamped, ["Quantity_sold", "Value_sold"]].notna().any(axis=1)).all()
    # GH #1256 adds a second key, on W4/W5 rows; since the W1 ruling it is also
    # on W1's minted rows that serve a §12 Q3 harvest, joined with this key
    # where the row also serves a §12 sale ('+').  Never on W2/W3.
    assert set(d.dropna()) <= {key, window, window + "+" + key}
    w23 = np.isin(np.asarray(t, dtype=object), ["2013-14", "2015-16"])
    assert not d.str.contains(window, regex=False).fillna(False)[w23].any()
    summ = cp.attrs["derivations"]["Ethiopia"][key]
    assert summ["rows"] == int(stamped.sum())
    assert summ["columns"] == ["Quantity_sold", "Value_sold"]


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(W13))
def test_derivation_inputs_are_the_raw_lines_and_join_to_the_served_rows(cp, wave):
    """Every §12 line, by its original names, indexed (t, i, j); every stamped
    served row's (t, i, j) is among them (``derivation_inputs`` walks ``i``
    through ``updated_ids`` as the served table is walked)."""
    import lsms_library as ll
    c = ll.Country("Ethiopia")
    raw = c.derivation_inputs("Ethiopia::crop_production::sect12-sale-period",
                              wave=wave)
    n = W13[wave]
    assert list(raw.index.names) == ["t", "i", "j"]
    assert len(raw) == len(_raw(wave, f"sect12_ph_w{n}.dta"))
    assert {"holder_id", "crop_code", "ph_s12q02_a", "ph_s12q02_d",
            "ph_s12q06", "ph_s12q07", "ph_s12q08"} <= set(raw.columns)
    served = cp[(cp["Derivation"].notna()) &
                (cp.index.get_level_values("t") == wave)]
    sk = set(served.reset_index().set_index(["t", "i", "j"]).index)
    missing = sk - set(raw.index)
    assert not missing, (len(missing), len(sk))


@pytest.mark.requires_s3
def test_positive_sale_rows_per_wave(cp):
    """Measured 2026-10-23 (development 9a0ed5f3c -> this change):
    W1 694 -> 854, W2 1,347 -> 2,737, W3 1,017 -> 2,300; W4 / W5 unchanged.
    W3 serves 2,300 of the 1,984 + 2,627 raw positive sale lines: the rest are
    holder-crops grown on several plots (the SALES GRAIN CAVEAT), counted by
    ``Sect12SaleWarning``."""
    v = cp["Value_sold"]
    t = cp.index.get_level_values("t")
    got = {w: int(((v > 0) & (t == w)).sum()) for w in sorted(set(t))}
    # 2,737 / 2,300 / 1,368 / 1,292 before the 2026-10-23 ruling that a row
    # carrying a sale value is never dropped (+23 / +9 / +27 / +31 rows whose
    # §9 row has no unit and no quantity; GH #1253 moved no positive value).
    # W1 854 -> 1,682 on the W1 tree-crop ruling (PR #1257): +828 rows minted
    # from §12 that carry a positive sale (484,385 Birr).
    assert got == {"2011-12": 1682, "2013-14": 2760, "2015-16": 2309,
                   "2018-19": 1395, "2021-22": 1323}, got
