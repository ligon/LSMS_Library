"""Ethiopia ``crop_production``: a row carrying a sale value is never dropped.

Ruling (@ligon, 2026-10-23), on the GH #820 review's finding.  The build's
empty-row filter in ``ethiopia.crop_production_for_wave`` drops a §9 row with
no unit and no quantity as an empty placeholder.  It used to look at
``Quantity`` and ``Quantity_sold`` but not ``Value_sold``, so it also dropped
rows that §11 or §12 had given a sale value: on development, §11 rows with a
positive value 1 / 2 / 27 / 32 (W2-W5; 1,500 / 7,750 / 41,442 / 33,482 Birr)
and 29 positive §12 lines (11,339 Birr, #1257's "attached, then dropped"
class).  The filter now also requires ``Value_sold`` to be missing; such a row
is served with the no-unit tag ``u = 'Other (Specify)'`` and only its
(unit-free) ``Value_sold``.

Tiers: the synthetic tests need no data; ``requires_s3`` tests build the table.
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
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_filter_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
    s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval",
    s12_crop_code="crop_code", s12_sold_flag="q06", s12_qty="q07", s12_value="q08",
)
nan = np.nan


def _build(eth, with12=True):
    """Every §9 row records NO unit and NO quantity: only a sale can keep it."""
    h = pd.DataFrame([("HH1", "H1", 1, 1, 7, nan),     # teff: §11 Yes, 300 Birr
                      ("HH2", "H2", 1, 1, 2, nan),     # maize: §11 No -> 0
                      ("HH3", "H3", 1, 1, 6, nan),     # sorghum: no sale at all
                      ("HH4", "H4", 1, 1, 72, nan)],   # coffee: §12 sale, 80 Birr
                     columns=["household_id", "holder_id", "parcel_id",
                              "field_id", "crop_code", "qty"])
    units = pd.Series([None] * len(h), dtype=object)
    sale = pd.DataFrame([("H1", 7, 1, 20.0, 300.0), ("H2", 2, 2, nan, nan)],
                        columns=["holder_id", "crop_code", "sold", "sqty", "sval"])
    s12 = pd.DataFrame([("H4", 72, 1, 4.0, 80.0)],
                       columns=["holder_id", "crop_code", "q06", "q07", "q08"])
    kw = dict(sale12=s12) if with12 else {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = eth.crop_production_for_wave("2013-14", h, None, sale, COLMAP,
                                          unit_labels=units, **kw)
    msgs = [str(w.message) for w in caught if w.category.__name__ == "Sect12SaleWarning"]
    return df, msgs


def _row(df, holder):
    return df[df.index.get_level_values("plot_id").str.startswith(holder + "_")]


def test_a_row_with_a_sect11_sale_value_is_kept(eth):
    df, _ = _build(eth)
    r = _row(df, "H1")
    assert r["Value_sold"].tolist() == [300.0]
    assert list(r.index.get_level_values("u")) == ["Other (Specify)"]
    assert r["Quantity"].isna().all() and r["Quantity_sold"].isna().all()


def test_a_recorded_no_sale_is_a_sale_value_too(eth):
    """A "No" serves Value_sold 0, a recorded answer: the row is kept."""
    assert _row(_build(eth)[0], "H2")["Value_sold"].tolist() == [0.0]


def test_a_row_with_nothing_to_serve_is_still_dropped(eth):
    """Control, true before and after: no unit, no quantity, no sale."""
    assert len(_row(_build(eth)[0], "H3")) == 0


def test_a_sect12_sale_keeps_its_row_and_no_line_is_attached_then_dropped(eth):
    """#1257's class 7 ("attached, then dropped") holds no line with a value."""
    df, msgs = _build(eth)
    r = _row(df, "H4")
    assert r["Value_sold"].tolist() == [80.0]
    assert list(r["Derivation"]) == ["Ethiopia::crop_production::sect12-sale-period"]
    assert "attached_then_dropped 0/0/0" in msgs[0]


@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


@pytest.mark.requires_s3
def test_served_rows_whose_only_number_is_a_sale_value(cp):
    """Measured cold 2026-10-23 (the GH #1256 commit -> this ruling): 884 rows
    added, all u = 'Other (Specify)' with no Quantity / Quantity_sold and a
    Value_sold -- W2 361 (§11 152, §12 209), W3 79 (17, 62), W4 168, W5 276;
    every pre-existing row unchanged.  §11 rows kept reconcile with the #820
    review's development count of sale rows the filter dropped (155 / 17 /
    171 / 277), less the 3 W2 and 3 W4 all-blank groups GH #1253 stopped
    serving as 0 (still empty, still dropped); W5's 277 wave rows serve as 276
    (two identical-sale rows on one key, differing only in ``intercropped``:
    the core collapse keeps one, +1 GrainCollapseWarning row).  §12: 29
    positive lines / 11,339 Birr, all now served."""
    t = np.asarray(cp.index.get_level_values("t"), dtype=object)
    u = np.asarray(cp.index.get_level_values("u"), dtype=object)
    only_value = ((u == "Other (Specify)")
                  & cp["Quantity"].isna().to_numpy()
                  & cp["Quantity_sold"].isna().to_numpy())
    got = {w: int((only_value & cp["Value_sold"].notna().to_numpy() & (t == w)).sum())
           for w in sorted(set(t))}
    assert got == {"2011-12": 0, "2013-14": 361, "2015-16": 79,
                   "2018-19": 168, "2021-22": 276}, got
    # and the filter still drops the genuinely empty rows
    assert not (only_value & cp["Value_sold"].isna().to_numpy()).any()
    # 89,840 since the W1 tree-crop ruling (PR #1257): +3,437 W1 rows minted
    # from §12, all u = 'Kg' -- none is a value-only 'Other (Specify)' row.
    assert len(cp) == 89840
