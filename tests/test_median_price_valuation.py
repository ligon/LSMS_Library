"""``median_price_valuation``: the default ladder, and the ``weight_col`` path.

There was no test of this transform before ``weight_col`` was added, so the
first test here is the *default-path pin*: hand-computed ladder prices on a
synthetic frame, which is what proves the unweighted behaviour did not move.

The weighted path is the **lower weighted median** -- the smallest observed
price whose cumulative weight (over the cell's prices sorted ascending)
reaches half the cell's total weight.  Two properties are pinned:

* under equal positive weights it reproduces the unweighted median EXACTLY
  for cells with an ODD count, and
* for an EVEN count it returns the lower of the two central prices, where the
  unweighted path averages them.  That divergence is the definition's, not an
  accident, so it is pinned rather than worked around.

All weights in these tests are small integers, so the cumulative sums are
exact and the tie at the half-total is a genuine tie.
"""

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import median_price_valuation


def _frame(rows):
    """Build an item frame indexed ``(t, i, j)``.

    Each row is ``(v, Region, j, price, quantity[, weight])``.  ``Value_sold``
    is set to the price and the sold quantity to 1 kg, so the observed unit
    price computed by the transform IS the ``price`` column given here.
    """
    recs = []
    for n, row in enumerate(rows):
        rec = dict(t='2019', i=f'hh{n:03d}', j=row['j'],
                   v=row['v'], Region=row.get('Region', 'R1'),
                   Value_sold=row['price'],
                   Quantity=row.get('quantity', 1.0))
        if 'weight' in row:
            rec['weight'] = row['weight']
        recs.append(rec)
    df = pd.DataFrame(recs).set_index(['t', 'i', 'j'])
    return df


def _kg(df):
    """One kg sold per row -- makes unit price == ``Value_sold``."""
    return pd.Series(1.0, index=df.index)


def _prices(out):
    return out['_unit_price'].to_numpy(dtype='float64')


# ---------------------------------------------------------------------------
# Default (unweighted) path -- the pin that proves nothing moved.
# ---------------------------------------------------------------------------

def test_default_path_hand_computed_ladder():
    """EA cell qualifies; the short cell borrows the Region median."""
    rows = ([{'v': 'A', 'j': 'maize', 'price': p} for p in (1, 2, 3, 4, 5)]
            + [{'v': 'B', 'j': 'maize', 'price': p} for p in (10, 20)])
    df = _frame(rows)
    out = median_price_valuation(df, ['v', 'Region'], kg_qty=_kg(df),
                                 threshold=3)
    # v='A' has 5 priced obs >= 3: its own median, 3.
    # v='B' has 2 < 3, so it takes Region R1's median over all seven
    # prices [1,2,3,4,5,10,20] -> 4.
    assert _prices(out).tolist() == [3, 3, 3, 3, 3, 4, 4]
    # Value = imputed price x Quantity (1.0 here).
    assert out['Value'].to_numpy().tolist() == [3, 3, 3, 3, 3, 4, 4]


def test_weight_col_none_is_the_default_path():
    rows = [{'v': 'A', 'j': 'maize', 'price': p} for p in (1, 2, 3, 4)]
    df = _frame(rows)
    kg = _kg(df)
    a = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3)
    b = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                               weight_col=None)
    pd.testing.assert_frame_equal(a, b)


# ---------------------------------------------------------------------------
# Equal weights: exact agreement on odd cells, documented split on even ones.
# ---------------------------------------------------------------------------

def test_equal_weights_reproduce_unweighted_exactly_odd_cells():
    """Both adopted cells have an odd count, so the two paths agree exactly."""
    rows = ([{'v': 'A', 'j': 'maize', 'price': p, 'weight': 1.0}
             for p in (1, 2, 3, 4, 5)]
            + [{'v': 'B', 'j': 'maize', 'price': p, 'weight': 1.0}
               for p in (10, 20, 60)])
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v', 'Region'], kg_qty=kg, threshold=3)
    wtd = median_price_valuation(df, ['v', 'Region'], kg_qty=kg, threshold=3,
                                 weight_col='weight')
    pd.testing.assert_frame_equal(plain, wtd)
    assert _prices(wtd).tolist() == [3, 3, 3, 3, 3, 20, 20, 20]


def test_equal_weights_but_different_positive_constant():
    """The weighted median is scale-invariant in the weights."""
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': 7.5}
            for p in (1, 2, 3, 4, 5)]
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3)
    wtd = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                                 weight_col='weight')
    pd.testing.assert_frame_equal(plain, wtd)


def test_even_cell_lower_median_vs_averaged_median():
    """The one documented divergence under equal weights."""
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': 1.0}
            for p in (1, 2, 3, 4)]
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3)
    wtd = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                                 weight_col='weight')
    assert set(_prices(plain)) == {2.5}   # (2+3)/2
    assert set(_prices(wtd)) == {2.0}     # the LOWER of the two central prices


# ---------------------------------------------------------------------------
# Hand-computed weighted cases where the two genuinely differ.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('prices, weights, expected_wtd, expected_plain', [
    # total 14, half 7; cum 1,2,3,4,14 -> first to reach 7 is the price 5.
    ((1, 2, 3, 4, 5), (1, 1, 1, 1, 10), 5.0, 3.0),
    # total 7, half 3.5; cum 5 -> the very first price already reaches it.
    ((10, 20, 30), (5, 1, 1), 10.0, 20.0),
    # total 8, half 4; cum 1,2,3,9 -> the price 40.
    ((10, 20, 30, 40), (1, 1, 1, 6), 40.0, 25.0),
])
def test_hand_computed_weighted_median(prices, weights, expected_wtd,
                                       expected_plain):
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip(prices, weights)]
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3)
    wtd = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                                 weight_col='weight')
    assert set(_prices(plain)) == {expected_plain}
    assert set(_prices(wtd)) == {expected_wtd}


def test_weight_from_an_index_level():
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip((1, 2, 3, 4, 5), (1, 1, 1, 1, 10))]
    df = _frame(rows)
    kg = _kg(df)
    as_col = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                                    weight_col='weight')
    as_level = median_price_valuation(
        df.set_index('weight', append=True), ['v'],
        kg_qty=pd.Series(1.0, index=df.set_index('weight',
                                                 append=True).index),
        threshold=3, weight_col='weight')
    assert set(_prices(as_col)) == set(_prices(as_level)) == {5.0}


def test_item_keys_stratify_the_weighted_median():
    """Two crops in one cell keep separate weighted medians."""
    rows = ([{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
             for p, w in zip((1, 2, 3), (1, 1, 1))]
            + [{'v': 'A', 'j': 'beans', 'price': p, 'weight': w}
               for p, w in zip((10, 20, 30), (5, 1, 1))])
    df = _frame(rows)
    out = median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                                 weight_col='weight')
    assert _prices(out).tolist() == [2, 2, 2, 10, 10, 10]


# ---------------------------------------------------------------------------
# Null / non-positive weights.
# ---------------------------------------------------------------------------

def test_null_and_zero_weights_are_excluded_but_still_valued():
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip((1, 2, 3, 4, 5), (1.0, 1.0, 1.0, np.nan, 0.0))]
    df = _frame(rows)
    with pytest.warns(UserWarning, match=r"2 of 5 priced rows"):
        out = median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                                     weight_col='weight')
    # Pool is [1,2,3] with equal weights -> 2.  Every row, including the two
    # excluded ones, is valued at the cell's price.
    assert _prices(out).tolist() == [2, 2, 2, 2, 2]


def test_negative_weight_is_excluded_like_a_zero():
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip((1, 2, 3, 4), (1.0, 1.0, 1.0, -100.0))]
    df = _frame(rows)
    with pytest.warns(UserWarning, match=r"1 of 4 priced rows"):
        out = median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                                     weight_col='weight')
    assert set(_prices(out)) == {2.0}


def test_no_warning_when_every_weight_is_usable():
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': 1.0}
            for p in (1, 2, 3)]
    df = _frame(rows)
    with warnings_as_errors():
        median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                               weight_col='weight')


def warnings_as_errors():
    import warnings as _w
    ctx = _w.catch_warnings()

    class _C:
        def __enter__(self):
            ctx.__enter__()
            _w.simplefilter('error')
            return self

        def __exit__(self, *a):
            return ctx.__exit__(*a)
    return _C()


# ---------------------------------------------------------------------------
# The threshold counts ROWS, not weight.
# ---------------------------------------------------------------------------

def test_threshold_counts_rows_not_summed_weight():
    """Three heavy rows do NOT clear threshold=4; the cell borrows Region.

    ``v='A'`` has three usable rows of weight 100 (sum 300) plus two rows the
    weight rule excludes.  If the threshold consulted summed weight, A would
    qualify on its own; it must not.  ``v='B'`` has four unit-weight rows and
    does qualify.
    """
    rows = ([{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
             for p, w in zip((1, 2, 3, 4, 5), (100.0, 100.0, 100.0,
                                               np.nan, 0.0))]
            + [{'v': 'B', 'j': 'maize', 'price': p, 'weight': 1.0}
               for p in (100, 200, 300, 400)])
    df = _frame(rows)
    with pytest.warns(UserWarning, match=r"2 of 9 priced rows"):
        out = median_price_valuation(df, ['v', 'Region'], kg_qty=_kg(df),
                                     threshold=4, weight_col='weight')
    # B qualifies at the v rung: equal weights, even count -> lower of the
    # two central prices, 200.
    # A falls through to Region R1, whose pool is
    #   1,2,3 at weight 100 and 100,200,300,400 at weight 1: total 304,
    #   half 152, cum 100,200 -> the price 2.
    assert _prices(out).tolist() == [2, 2, 2, 2, 2, 200, 200, 200, 200]


def test_national_fallback_is_unconditional_on_both_paths():
    """A cell that clears nothing still gets a price (the WB/ours delta)."""
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip((1, 2, 3), (1, 1, 10))]
    df = _frame(rows)
    kg = _kg(df)
    # threshold well above the pool size: no rung qualifies.
    plain = median_price_valuation(df, ['v', 'Region'], kg_qty=kg,
                                   threshold=99)
    wtd = median_price_valuation(df, ['v', 'Region'], kg_qty=kg, threshold=99,
                                 weight_col='weight')
    assert set(_prices(plain)) == {2.0}          # median of 1,2,3
    assert set(_prices(wtd)) == {3.0}            # total 12, half 6; cum 1,2,12


def test_unpriced_rows_take_no_part_but_are_valued():
    """A row with no sale is pooled nowhere yet still receives a price."""
    rows = ([{'v': 'A', 'j': 'maize', 'price': p, 'weight': 1.0}
             for p in (1, 2, 3)]
            + [{'v': 'A', 'j': 'maize', 'price': 0.0, 'weight': 1.0,
                'quantity': 10.0}])
    df = _frame(rows)
    out = median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                                 weight_col='weight')
    assert _prices(out).tolist() == [2, 2, 2, 2]
    assert out['Value'].to_numpy().tolist() == [2, 2, 2, 20]


def test_duplicate_index_labels_are_safe():
    """The helper's working frame is built on a fresh RangeIndex for this.

    Item features repeat a household across its item rows, so a caller can
    hand us an index with duplicate labels.  A DataFrame assembled from
    Series would try to align on it; pin that we do not.
    """
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': w}
            for p, w in zip((1, 2, 3, 4, 5), (1, 1, 1, 1, 10))]
    df = _frame(rows)
    # Collapse every row onto ONE household id: the index is now 5x 'hh'.
    df.index = pd.MultiIndex.from_arrays(
        [['2019'] * 5, ['hh'] * 5, ['maize'] * 5], names=['t', 'i', 'j'])
    assert df.index.duplicated().sum() == 4
    out = median_price_valuation(df, ['v'], kg_qty=_kg(df), threshold=3,
                                 weight_col='weight')
    assert set(_prices(out)) == {5.0}   # total 14, half 7; cum 1,2,3,4,14
