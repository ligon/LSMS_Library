"""Which rows are evidence for a food kg factor, and which rows are a price.

Two rulings by @ligon, 2026-11-12, both in ``lsms_library/transformations.py``.

GH #1509, "all purchase rows".  ``food_kg_factors``' price-ratio inference
reads only rows whose ``s`` is exactly ``'purchased'``.  Own production and
gifts are valued below market on staples, so pooled with purchases they moved
a cell by how often each regime lands on the ``kg`` rows and on the bunch rows.
Every row is still SERVED: a produced row reads the factor its ``(j, u)`` cell
gets from purchases.

GH #1068 / #1129, "drop and count".  ``food_prices(units='unitvalue')`` drops a
row on a currency-denominated unit (``u='Value'``, where ``Expenditure /
Quantity`` is the amount over itself, 1.0) and a row on a missing unit
(``u='Unknown'``), tallied in ``attrs['price_rows_dropped']`` as ``currency``
and ``unknown``.  ``food_quantities(units='units')`` drops the missing-unit
rows and counts them in ``attrs['quantity_rows_dropped']``.

Data-free: every frame here is synthetic.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    U_UNKNOWN,
    UnpriceableRowsWarning,
    food_kg_factors,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)

IDX = ['t', 'i', 'j', 'u', 's']


def frame(rows):
    """``(t, i, j, u, s)`` frame from ``(t, i, j, u, s, Quantity, Expenditure)``."""
    idx = pd.MultiIndex.from_tuples([r[:5] for r in rows], names=IDX)
    return pd.DataFrame({'Quantity': [float(r[5]) for r in rows],
                         'Expenditure': [float(r[6]) for r in rows]}, index=idx)


def banana(n=20, *, t='2020', produced_kg_price=20.0, other_s=None):
    """Purchases price a kg at 100 and a bunch at 300, so a bunch is 3 kg.

    Own production values a kg at *produced_kg_price* and a bunch at 300 too,
    so pooled with purchases it would say a bunch weighs 300 / 20 = 15 kg.
    """
    rows = []
    for k in range(n):
        h = f'h{k}'
        rows += [(t, h, 'banana', 'kg', 'purchased', 2, 200),
                 (t, h, 'banana', 'Bunch', 'purchased', 1, 300),
                 (t, h, 'banana', 'kg', other_s or 'produced', 2,
                  2 * produced_kg_price),
                 (t, h, 'banana', 'Bunch', other_s or 'produced', 1, 300)]
    return frame(rows)


def quiet(fn, *a, **k):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return fn(*a, **k)


# ---------------------------------------------------------------------------
# GH #1509: purchases are the evidence
# ---------------------------------------------------------------------------

def test_self_valued_rows_do_not_enter_the_estimate():
    df = banana()
    f = quiet(food_kg_factors, df)
    bunch = f.xs('Bunch', level='u')
    # Every Bunch row, purchased AND produced, is served the purchase factor.
    assert bunch['kg_per_unit'].to_numpy() == pytest.approx(np.full(40, 3.0))
    assert set(bunch['KgFactorSource']) == {'item_unit'}
    # ... which is exactly what the purchases alone estimate.
    only = df[df.index.get_level_values('s') == 'purchased']
    alone = quiet(food_kg_factors, only).xs('Bunch', level='u')['kg_per_unit']
    assert alone.to_numpy() == pytest.approx(np.full(20, 3.0))


def test_scaling_nonpurchase_values_moves_no_factor():
    """The #1442 control: a value on a produced / inkind row is not evidence."""
    df = banana()
    scaled = df.copy()
    not_bought = df.index.get_level_values('s') != 'purchased'
    scaled.loc[not_bought, 'Expenditure'] *= 1000
    a = quiet(food_kg_factors, df)
    b = quiet(food_kg_factors, scaled)
    pd.testing.assert_series_equal(a['kg_per_unit'], b['kg_per_unit'])
    pd.testing.assert_series_equal(a['KgFactorSource'], b['KgFactorSource'])
    assert a.attrs['kg_factor_sources'] == b.attrs['kg_factor_sources']


@pytest.mark.parametrize('other_s', ['inkind', 'other', 'Purchased'])
def test_a_row_not_known_to_be_a_purchase_is_not_evidence(other_s):
    """``inkind``, ``other`` and a non-canonical spelling are not purchases."""
    f = quiet(food_kg_factors, banana(other_s=other_s))
    assert f.xs('Bunch', level='u')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(40, 3.0))
    excluded = f.attrs['kg_factor_evidence']['priced_rows_excluded']
    assert excluded == {other_s: 40}


def test_an_na_source_is_not_evidence():
    df = banana()
    s = np.asarray(df.index.get_level_values('s'), dtype=object)
    s[s == 'produced'] = None
    df.index = pd.MultiIndex.from_arrays(
        [df.index.get_level_values(n) for n in IDX[:4]] + [s], names=IDX)
    f = quiet(food_kg_factors, df)
    assert f.xs('Bunch', level='u')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(40, 3.0))
    assert f.attrs['kg_factor_evidence']['priced_rows_excluded'] == {'<NA>': 40}


def test_no_purchase_in_a_cell_means_no_purchase_factor_only_the_disclosed_one():
    """No purchase rung reads produced rows when purchases are absent.

    Until the 2026-11-12 amendment ("Fallback rung, disclosed") such a row was
    ``none``.  It is now served by the separately labelled
    ``item_unit_nonpurchase`` rung, never by a purchase rung; the fallback's
    own tests are in ``test_gh1509_nonpurchase_fallback.py``.
    """
    rows = []
    for k in range(20):
        rows += [('2020', f'h{k}', 'yam', 'kg', 'produced', 2, 200),
                 ('2020', f'h{k}', 'yam', 'Heap', 'produced', 1, 300)]
    f = quiet(food_kg_factors, frame(rows))
    heap = f.xs('Heap', level='u')
    assert heap['kg_item_unit'].isna().all()
    assert heap['kg_unit'].isna().all()
    assert set(heap['KgFactorSource']) == {'item_unit_nonpurchase'}
    # The read rungs still serve the produced kg rows.
    assert set(f.xs('kg', level='u')['KgFactorSource']) == {'metric'}
    assert f.attrs['kg_factor_evidence']['priced_rows_read'] == 0


def test_survey_kg_on_a_produced_row_is_still_served():
    df = banana()
    df['Quantity_kg'] = np.nan
    produced_bunch = ((df.index.get_level_values('s') == 'produced')
                      & (df.index.get_level_values('u') == 'Bunch'))
    df.loc[produced_bunch, 'Quantity_kg'] = 4.0
    f = quiet(food_kg_factors, df)
    got = f[produced_bunch]
    assert set(got['KgFactorSource']) == {'survey_kg'}
    assert got['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 4.0))


def test_a_frame_without_s_is_all_evidence_and_says_so():
    df = banana().reset_index('s', drop=True)
    f = quiet(food_kg_factors, df)
    ledger = f.attrs['kg_factor_evidence']
    assert ledger['source_level'] is False
    assert ledger['priced_rows_excluded'] == {}
    assert ledger['priced_rows_read'] == 80
    # Pooled, the Bunch is no longer 3 kg: that is what the filter removes.
    assert not np.allclose(f.xs('Bunch', level='u')['kg_per_unit'], 3.0)


def test_s_as_a_column_is_read_too():
    df = banana().reset_index('s')
    f = quiet(food_kg_factors, df)
    assert f.xs('Bunch', level='u')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(40, 3.0))
    assert f.attrs['kg_factor_evidence']['source_level'] is True


def test_the_ledger_counts_every_row_read_under_serve_waves():
    df = pd.concat([banana(t='2020'), banana(t='2021')])
    f = quiet(food_kg_factors, df, serve_waves=['2021'])
    assert len(f) == 80
    ledger = f.attrs['kg_factor_evidence']
    assert ledger['priced_rows_read'] == 80          # both waves' purchases
    assert ledger['priced_rows_excluded'] == {'produced': 80}


@pytest.mark.parametrize('fn, kw', [
    (food_quantities_from_acquired, {'units': 'kgs'}),
    (food_prices_from_acquired, {'units': 'kgvalue'}),
])
def test_the_ledger_rides_on_the_kg_tables(fn, kw):
    out = quiet(fn, banana(), **kw)
    assert out.attrs['kg_factor_evidence']['priced_rows_excluded'] == \
        {'produced': 40}


def test_kgvalue_uses_the_purchase_factor_for_every_row():
    out = quiet(food_prices_from_acquired, banana(), units='kgvalue')
    bunch = out.xs('Bunch', level='u')
    # purchased: 300 / 3 kg; produced: its own 300 over the same 3 kg.
    assert bunch['Price'].to_numpy() == pytest.approx(np.full(40, 100.0))


# ---------------------------------------------------------------------------
# GH #1129 / #1068: unitvalue drops currency and missing-unit rows
# ---------------------------------------------------------------------------

def priced_frame():
    rows = []
    for k in range(10):
        h = f'h{k}'
        rows += [('2020', h, 'rice', 'kg', 'purchased', 2, 100),
                 ('2020', h, 'meals', 'Value', 'purchased', 50, 50),
                 ('2020', h, 'salt', U_UNKNOWN, 'purchased', 3, 60),
                 ('2020', h, 'beans', 'Manquant', 'purchased', 2, 40)]
    return frame(rows)


def test_unitvalue_drops_and_counts_currency_and_unknown_rows():
    out = quiet(food_prices_from_acquired, priced_frame(), units='unitvalue')
    assert set(out.index.get_level_values('u')) == {'kg'}
    assert out['Price'].to_numpy() == pytest.approx(np.full(10, 50.0))
    tally = out.attrs['price_rows_dropped']
    assert tally['currency'] == tally['currency_expected'] == 10
    assert tally['unknown'] == tally['unknown_expected'] == 20  # Unknown + Manquant
    assert tally['expected_gross'] == 40
    assert tally['expected'] == 10
    assert tally['dropped_expected'] == 0
    assert tally['lost_fraction'] == 0.0


def test_unitvalue_drops_a_nan_unit_too():
    df = priced_frame()
    u = np.asarray(df.index.get_level_values('u'), dtype=object)
    u[u == U_UNKNOWN] = None
    df.index = pd.MultiIndex.from_arrays(
        [df.index.get_level_values(n) if n != 'u' else u for n in IDX], names=IDX)
    out = quiet(food_prices_from_acquired, df, units='unitvalue')
    assert out.attrs['price_rows_dropped']['unknown'] == 20


def test_no_unitvalue_price_is_the_currency_tautology():
    out = quiet(food_prices_from_acquired, priced_frame(), units='unitvalue')
    assert not (out.index.get_level_values('u') == 'Value').any()


def test_a_wave_of_only_value_rows_serves_no_unitvalue():
    """GhanaLSS GLSS1-3: every row u='Value', so unitvalue is empty there."""
    rows = [('1987-88', f'h{k}', 'maize', 'Value', 'purchased', 30, 30)
            for k in range(10)]
    rows += [('2016-17', f'h{k}', 'maize', 'kg', 'purchased', 2, 30)
             for k in range(10)]
    out = quiet(food_prices_from_acquired, frame(rows), units='unitvalue')
    assert set(out.index.get_level_values('t')) == {'2016-17'}
    assert out.attrs['price_rows_dropped']['currency'] == 10


def test_an_unknown_row_with_a_zero_quantity_stays_the_loud_inf_defect():
    """The ``unknown`` bucket holds only rows the label rule removed."""
    rows = [('2020', f'z{k}', 'rice', U_UNKNOWN, 'purchased', 0, 500)
            for k in range(9)]
    rows += [('2020', 'g', 'rice', 'kg', 'purchased', 10, 500)]
    with pytest.warns(UnpriceableRowsWarning, match=r'dropped 9 of 10'):
        out = food_prices_from_acquired(frame(rows), units='unitvalue')
    tally = out.attrs['price_rows_dropped']
    assert tally['inf'] == 9
    assert tally['unknown'] == 0


def test_buckets_are_disjoint_and_money_only_wins_over_unknown():
    """Burkina 2014 passages 2-4: money-only AND u='Unknown' -> money_only."""
    rows = [(1, f'g{k}', 'rice', 'kg', 'purchased', 10, 500) for k in range(10)]
    rows += [(1, f'v{k}', 'meals', 'Value', 'purchased', 5, 5) for k in range(5)]
    rows += [(1, f'n{k}', 'salt', U_UNKNOWN, 'purchased', 2, 50) for k in range(7)]
    rows += [(2, f'm{k}', 'rice', U_UNKNOWN, 'purchased', np.nan, 500)
             for k in range(100)]
    idx = pd.MultiIndex.from_tuples(
        [('2014', r[1], r[2], r[3], r[4], r[0]) for r in rows],
        names=IDX + ['visit'])
    df = pd.DataFrame({'Quantity': [r[5] for r in rows],
                       'Expenditure': [float(r[6]) for r in rows]}, index=idx)
    out = quiet(food_prices_from_acquired, df, units='unitvalue')
    tally = out.attrs['price_rows_dropped']
    assert tally['money_only'] == 100
    assert tally['unknown'] == 7
    assert tally['currency'] == 5
    assert (tally['expected_gross'] - tally['expected']
            == tally['currency_expected'] + tally['money_only_expected']
            + tally['unknown_expected'])
    assert tally['expected'] == 10 and tally['dropped_expected'] == 0
    assert len(out) == 10


def test_unitprice_and_kg_modes_are_not_ruled_on():
    """``unitprice`` keeps Unknown / Value rows; the kg modes' tallies stand."""
    df = priced_frame()
    df['Price'] = 7.0
    up = quiet(food_prices_from_acquired, df, units='unitprice')
    assert {'Value', U_UNKNOWN, 'Manquant', 'kg'} <= set(
        up.index.get_level_values('u'))
    kv = quiet(food_prices_from_acquired, df, units='kgvalue')
    tally = kv.attrs['price_rows_dropped']
    assert tally['currency'] == 10
    assert tally['unknown'] == 0                 # sentinel rows stay `nan`
    assert tally['nan'] == 20


# ---------------------------------------------------------------------------
# GH #1068: units='units' drops missing-unit rows; keeps currency rows
# ---------------------------------------------------------------------------

def test_units_mode_drops_and_counts_unknown_rows():
    out = food_quantities_from_acquired(priced_frame(), units='units')
    assert set(out.index.get_level_values('u')) == {'kg', 'Value'}
    assert out.attrs['quantity_rows_dropped'] == {
        'units': 'units', 'rows_in': 40, 'unknown': 20}
    # The currency rows are the recorded amount, kept as the kgs carry rule does.
    assert out.xs('Value', level='u')['Quantity'].to_numpy() == \
        pytest.approx(np.full(10, 50.0))


def test_units_mode_does_not_count_an_unknown_row_with_no_quantity():
    rows = [('2020', 'h', 'salt', U_UNKNOWN, 'purchased', np.nan, 5),
            ('2020', 'h', 'rice', 'kg', 'purchased', 2, 5)]
    out = food_quantities_from_acquired(frame(rows), units='units')
    assert out.attrs['quantity_rows_dropped']['unknown'] == 0
    assert len(out) == 1


def test_kgs_mode_still_carries_unknown_rows_natively():
    """Not ruled on: the default mode carries them with their native unit."""
    out = quiet(food_quantities_from_acquired, priced_frame(), units='kgs')
    assert U_UNKNOWN in set(out.index.get_level_values('u'))
    assert 'quantity_rows_dropped' not in out.attrs
