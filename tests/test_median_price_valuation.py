"""``median_price_valuation``: the default ladder, and the ``weight_col`` path.

There was no test of this transform before ``weight_col`` was added, so the
first test here is the *default-path pin*: hand-computed ladder prices on a
synthetic frame, which is what proves the unweighted behaviour did not move.

The weighted path is a **weighted median**: sort the cell's prices ascending,
let ``W`` be the total weight and ``C`` the cumulative weight, and take the
price at the first row where ``C >= W/2`` -- except where ``C == W/2`` exactly
at that row, where it is the mean of that price and the next row's.

The property that rule exists for, and the one these tests are mostly about:
``weight_col`` is a strict GENERALISATION of the default path.  Under equal
(or all-equal positive) weights the weighted median reproduces
``Series.median()`` EXACTLY, for BOTH odd and even cell counts -- the exact-tie
exception fires precisely on an even count and averages the two central prices.

Weights here are small integers or exact binary fractions, so the cumulative
sums are exact and the tie at the half-total is a genuine tie.
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
    """Odd-count cells: the half-total falls strictly inside the middle row."""
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


def test_equal_weights_reproduce_unweighted_exactly_even_cells():
    """Even-count cells: the exact-tie exception averages the two central
    prices, exactly as ``Series.median()`` does.  Without it the weighted
    path would return the LOWER central price and ``weight_col`` would not be
    a generalisation of the default."""
    rows = [{'v': 'A', 'j': 'maize', 'price': p, 'weight': 1.0}
            for p in (1, 2, 3, 4)]
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3)
    wtd = median_price_valuation(df, ['v'], kg_qty=kg, threshold=3,
                                 weight_col='weight')
    pd.testing.assert_frame_equal(plain, wtd)
    assert set(_prices(wtd)) == {2.5}     # (2+3)/2, NOT the lower 2.0


@pytest.mark.parametrize('n', list(range(1, 13)))
@pytest.mark.parametrize('w', [1.0, 0.5, 7.0, 1e6])
def test_equal_weights_reproduce_unweighted_at_every_parity(n, w):
    """The generalisation property, swept over both parities and scales."""
    rows = [{'v': 'A', 'j': 'maize', 'price': float(p) * 1.5, 'weight': w}
            for p in range(1, n + 1)]
    df = _frame(rows)
    kg = _kg(df)
    plain = median_price_valuation(df, ['v'], kg_qty=kg, threshold=1)
    wtd = median_price_valuation(df, ['v'], kg_qty=kg, threshold=1,
                                 weight_col='weight')
    pd.testing.assert_frame_equal(plain, wtd)


# ---------------------------------------------------------------------------
# Hand-computed weighted cases where the two genuinely differ.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('prices, weights, expected_wtd, expected_plain', [
    # total 14, half 7; cum 1,2,3,4,14 -> first to reach 7 is the price 5,
    # and 4 != 7 so no tie.
    ((1, 2, 3, 4, 5), (1, 1, 1, 1, 10), 5.0, 3.0),
    # total 7, half 3.5; cum 5 -> the very first price already reaches it.
    ((10, 20, 30), (5, 1, 1), 10.0, 20.0),
    # total 8, half 4; cum 1,2,3,9 -> the price 40; 3 != 4 so no tie.
    ((10, 20, 30, 40), (1, 1, 1, 6), 40.0, 25.0),
    # EXACT TIE on unequal weights: total 10, half 5; cum 3,5,10 -> the tie
    # lands on the price 20, so the answer is (20+30)/2 = 25.
    ((10, 20, 30), (3, 2, 5), 25.0, 20.0),
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
    # B qualifies at the v rung: equal weights, even count -> the exact-tie
    # exception averages the two central prices, (200+300)/2 = 250, which is
    # what Series.median() gives too.
    # A falls through to Region R1, whose pool is
    #   1,2,3 at weight 100 and 100,200,300,400 at weight 1: total 304,
    #   half 152, cum 100,200 -> the price 2 (200 != 152, so no tie).
    assert _prices(out).tolist() == [2, 2, 2, 2, 2, 250, 250, 250, 250]


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
    # (cum 2 != 6, so no tie; the weighted answer is the price 3 itself)


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


# GH #824 owner ruling: Quantity_sold has its OWN unit. These are literal
# price oracles, independent of the resolver and its implementation.
def _sold_frame(units=('Kg',), harvest=None, **columns):
    n = len(units)
    df = pd.DataFrame({
        'j': ['crop'] * n, 'u': harvest if harvest is not None else ['Kg'] * n,
        'Unit_sold': pd.array(units, dtype='string'),
        'Quantity_sold': pd.array([2.] * n, dtype='Float64'),
        'Value_sold': pd.array([200.] * n, dtype='Float64'),
        'Quantity': pd.array([10.] * n, dtype='Float64'),
    })
    for key, values in columns.items():
        df[key] = values
    return df


def _sold_price(df, **kwargs):
    return median_price_valuation(df, [], threshold=1, **kwargs)


@pytest.mark.parametrize('unit_axis', ['column', 'index'])
def test_sold_unit_literal_price_oracle(unit_axis):
    df = _sold_frame(['Sack (100 kgs)', 'Kg'], ['Kg', 'Sack (100 kgs)'])
    if unit_axis == 'index':
        df = df.set_index(['j', 'u'])
    # 200/(2*100)=1 and 200/(2*1)=100; median=50.5, value=505.
    out = _sold_price(df)
    assert out['_unit_price'].tolist() == [50.5, 50.5]
    assert out['Value'].tolist() == [505., 505.]
    assert out.attrs['sold_quantity_conversion'] == {
        'mode': 'units', 'rows': 2, 'supplied_factor': 0, 'sold_unit': 2,
        'harvest_fallback': 0, 'unresolved': 0,
        'harvest_fallback_attempted': 0, 'known_unit_pairs': 2,
        'unit_disagreement': 2, 'supplied_factor_invalid': 0,
        'supplied_factor_rejected': 0,
    }


def test_sold_different_unit_one_row_negative_control():
    df = _sold_frame(['Sack (100 kgs)'])
    # Public baseline uses harvest Kg and returns 100, not the ruled 1.
    assert _sold_price(df)['_unit_price'].tolist() == [1.]


@pytest.mark.parametrize('unit', ['Basket', None, 'Unknown'])
def test_sold_reported_factor_precedes_label_and_missing_label(unit):
    df = _sold_frame([unit], KgFactor_sold=[25.], KgFactor=[99.])
    out = _sold_price(df, unit_kg={'Basket': 50.})
    assert out['_unit_price'].tolist() == [4.]  # 200/(2*25)
    count = out.attrs['sold_quantity_conversion']
    assert count['supplied_factor'] == 1
    assert count['harvest_fallback_attempted'] == 0
    assert count['harvest_fallback'] == 0


@pytest.mark.parametrize('unit', ['Unknown', 'Manquant', 'Basket', '', 'NA'])
def test_recorded_unconvertible_sold_unit_never_falls_back(unit):
    df = _sold_frame([unit], KgFactor=[20.])
    out = _sold_price(df)
    assert out['_unit_price'].isna().all()
    count = out.attrs['sold_quantity_conversion']
    assert count['unresolved'] == 1
    assert count['harvest_fallback_attempted'] == 0


@pytest.mark.parametrize('absent_column', [False, True])
def test_absent_sold_unit_falls_back_with_exact_count(absent_column):
    df = _sold_frame([None, None], ['Sack (100 kgs)', 'Basket'])
    if absent_column:
        df = df.drop(columns='Unit_sold')
    out = _sold_price(df)
    assert out['_unit_price'].tolist() == [1., 1.]
    c = out.attrs['sold_quantity_conversion']
    assert (c['harvest_fallback_attempted'], c['harvest_fallback'],
            c['unresolved'], c['known_unit_pairs']) == (2, 1, 1, 0)


@pytest.mark.parametrize('bad', [0, -2, np.inf, -np.inf, np.nan, pd.NA, 'bad'])
def test_invalid_sold_factors_never_become_mass(bad):
    df = _sold_frame(['Basket'], KgFactor_sold=[bad])
    out = _sold_price(df)
    assert out['_unit_price'].isna().all()
    c = out.attrs['sold_quantity_conversion']
    assert c['supplied_factor'] == 0
    assert c['supplied_factor_invalid'] == int(pd.notna(bad))
    assert c['supplied_factor_rejected'] == 0
    assert c['harvest_fallback'] == 0


@pytest.mark.parametrize('unit,factor,price', [
    ('Kg', 2., 100.), ('Basket', 251., None), ('Basket', 2017., None),
    ('Tonne', 1000., .1), ('Kg', 1., 100.), ('Basket', 250., .4),
])
def test_sold_reported_screen_uses_sold_context(unit, factor, price):
    df = _sold_frame([unit], ['Sack (100 kgs)'], KgFactor_sold=[factor])
    out = _sold_price(df)
    if price is None:
        assert out['_unit_price'].isna().all()
    else:
        assert out['_unit_price'].tolist() == [price]
    c = out.attrs['sold_quantity_conversion']
    assert c['supplied_factor_rejected'] == int(factor not in [1., 250.])


def test_zero_missing_sales_and_exact_disclosure_population():
    df = _sold_frame(['Kg', None, 'Basket', None, 'Sack (100 kgs)', 'Kg'],
                     ['Kg', 'Kg', 'Kg', 'Basket', 'Kg', 'Kg'],
                     Quantity_sold=pd.array([2, 0, None, 2, 2, 2], dtype='Float64'),
                     KgFactor_sold=[np.nan, np.nan, np.nan, np.nan, np.nan, 1.])
    out = _sold_price(df)
    # Only rows 0,4,5 speak: prices 100,1,100; others still receive median.
    assert out['_unit_price'].tolist() == [100.] * 6
    assert out.attrs['sold_quantity_conversion'] == {
        'mode': 'units', 'rows': 6, 'supplied_factor': 1, 'sold_unit': 2,
        'harvest_fallback': 1, 'unresolved': 2,
        'harvest_fallback_attempted': 2, 'known_unit_pairs': 4,
        'unit_disagreement': 2, 'supplied_factor_invalid': 0,
        'supplied_factor_rejected': 0,
    }


def test_explicit_sold_kg_bypasses_all_unit_work():
    df = _sold_frame(['Basket'], KgFactor_sold=['bad']).drop(columns=['u', 'Quantity_sold'])
    out = _sold_price(df, kg_qty=pd.Series([50.]), unit_kg='invalid mapping')
    assert out['_unit_price'].tolist() == [4.]
    assert out.attrs['sold_quantity_conversion'] == {'mode': 'explicit', 'rows': 1}


@pytest.mark.parametrize('origin', ['flat', 'nested', 'attrs', 'empty', 'none'])
def test_sold_declarations_precedence(origin):
    df = _sold_frame(['Basket'])
    kwargs = {}
    if origin in ['attrs', 'empty']:
        df.attrs['unit_kg'] = {'A': {'basket': 25.}}
    if origin == 'flat':
        kwargs['unit_kg'] = {'BASKET': 25.}
    if origin == 'nested':
        kwargs['unit_kg'] = {'A': {'basket': 25.}}
    if origin == 'empty':
        kwargs['unit_kg'] = {}
    out = _sold_price(df, **kwargs)
    if origin in ['empty', 'none']:
        assert out['_unit_price'].isna().all()
    else:
        assert out['_unit_price'].tolist() == [4.]


@pytest.mark.parametrize('country_axis', ['index', 'column', 'attrs'])
def test_sold_country_qualified_collision(country_axis):
    df = _sold_frame(['Basket', 'Basket'], country=['A', 'B'], j=['a', 'b'])
    df.attrs['unit_kg'] = {'A': {'basket': 25.}, 'B': {'basket': 50.}}
    if country_axis == 'index':
        df = df.set_index(['country', 'u'])
    if country_axis == 'attrs':
        df = df.drop(columns='country')
        df.attrs['country'] = 'B'
    assert _sold_price(df)['_unit_price'].tolist() == (
        [2., 2.] if country_axis == 'attrs' else [4., 2.])


@pytest.mark.parametrize('volume', [False, True])
def test_sold_volume_behavior(volume):
    out = _sold_price(_sold_frame(['Litre']), volume_as_mass=volume)
    if volume:
        assert out['_unit_price'].tolist() == [100.]
    else:
        assert out['_unit_price'].isna().all()


def test_sold_missing_harvest_key_and_required_keys():
    df = _sold_frame(['Sack (100 kgs)']).drop(columns='u')
    assert _sold_price(df)['_unit_price'].tolist() == [1.]
    for col in ['Quantity_sold', 'Value_sold', 'Quantity', 'j']:
        with pytest.raises(ValueError):
            _sold_price(df.drop(columns=col))
    with pytest.raises(ValueError, match="unit level or column"):
        _sold_price(df.drop(columns='Unit_sold'))


def _strict_typed_equal(a, b):
    assert type(a) is type(b)
    if isinstance(a, dict):
        assert list(a) == list(b)
        for key in a:
            _strict_typed_equal(a[key], b[key])
    elif isinstance(a, (list, tuple)):
        assert len(a) == len(b)
        for x, y in zip(a, b):
            _strict_typed_equal(x, y)
    else:
        assert a == b


def _strict_frame_payload(a, b):
    pd.testing.assert_frame_equal(a, b, check_exact=True)
    _strict_typed_equal(a.attrs, b.attrs)
    for col in a.columns:
        x, y = a[col], b[col]
        if isinstance(x.dtype, pd.api.extensions.ExtensionDtype):
            pd.testing.assert_extension_array_equal(x.array, y.array, check_exact=True)
        np.testing.assert_array_equal(x.isna().to_numpy(), y.isna().to_numpy())
        np.testing.assert_array_equal(x[x.notna()].to_numpy(), y[y.notna()].to_numpy())


def test_sold_preserves_duplicate_axes_payload_and_typed_attrs():
    df = _sold_frame(['Sack (100 kgs)', 'Kg'], j=['a', 'b'])
    df.index = pd.MultiIndex(levels=[['same', 'unused'], ['Kg', 'unused']],
                             codes=[[0, 0], [0, 0]], names=['i', 'u'])
    df = df.drop(columns='u')
    df.columns.name = 'measure'
    df.attrs = {'record': {'tuple': (np.int64(1), 'x'), 'list': [True]},
                'unit_kg': {'A': {'Sack (100 kgs)': 100.}}}
    before = df.copy(deep=True)
    out = _sold_price(df)
    assert out['_unit_price'].tolist() == [1., 100.]
    pd.testing.assert_index_equal(out.index, before.index, exact=True)
    for a, b in zip(out.index.levels, before.index.levels):
        pd.testing.assert_index_equal(a, b, exact=True)
    for a, b in zip(out.index.codes, before.index.codes):
        np.testing.assert_array_equal(a, b)
    _strict_frame_payload(df, before)


@pytest.mark.parametrize('delta', ['nextafter', 'tiny', 'mask', 'attrs_type'])
def test_strict_payload_comparator_calibration(delta):
    df = _sold_frame(['Kg'])
    df.attrs = {'nested': {'value': (np.int64(1),)}}
    changed = df.copy(deep=True)
    if delta == 'attrs_type':
        changed.attrs['nested']['value'] = (1,)
    elif delta == 'mask':
        changed.loc[0, 'Quantity'] = pd.NA
    else:
        changed.loc[0, 'Quantity'] = (np.nextafter(10., np.inf)
                                     if delta == 'nextafter' else 10. + 1e-12)
    with pytest.raises(AssertionError):
        _strict_frame_payload(changed, df)


def test_sold_units_keep_weighted_ladder_and_target_quantity():
    df = _sold_frame(['Sack (100 kgs)', 'Kg', 'Kg'],
                     weight=[8., 1., 1.], v=['local', 'local', 'far'],
                     Value_sold=[200., 200., 400.], Quantity=[7., 9., 11.])
    # Observed prices 1,100,200. Local clears threshold=2; far uses national.
    out = median_price_valuation(df, ['v'], threshold=2, weight_col='weight')
    assert out['_unit_price'].tolist() == [1., 1., 1.]
    assert out['Value'].tolist() == [7., 9., 11.]
    plain = median_price_valuation(df, ['v'], threshold=2)
    assert plain['_unit_price'].tolist() == [50.5, 50.5, 100.]
    assert plain['Value'].tolist() == [353.5, 454.5, 1100.]


def test_sold_agreement_case_and_invalid_declaration():
    df = _sold_frame(['kg', 'Basket'], ['Kg', 'Kg'], j=['a', 'b'])
    out = _sold_price(df, unit_kg={'kg': -1, 'basket': np.inf})
    assert out['_unit_price'].iloc[0] == 100.
    assert pd.isna(out['_unit_price'].iloc[1])
    c = out.attrs['sold_quantity_conversion']
    assert (c['known_unit_pairs'], c['unit_disagreement']) == (2, 1)


def test_sold_factor_without_either_unit_and_no_gram_heuristic():
    df = _sold_frame([None], KgFactor_sold=[.001]).drop(columns=['u', 'Unit_sold'])
    assert _sold_price(df)['_unit_price'].tolist() == [100000.]


def test_sold_sentinel_declaration_cannot_fabricate_a_unit():
    df = _sold_frame(['Unknown', 'Manquant'])
    out = _sold_price(df, unit_kg={'Unknown': 100, 'Manquant': 100})
    assert out['_unit_price'].isna().all()
    assert out.attrs['sold_quantity_conversion']['harvest_fallback_attempted'] == 0
