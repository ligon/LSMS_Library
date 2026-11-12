"""The second, unit-level non-purchase rung of GH #1509: ``unit_nonpurchase``.

Ruling by @ligon, 2026-11-12 ("Add a unit-level rung").  Below the item-level
fallback ``item_unit_nonpurchase`` sits a separately labelled rung, pooled
across items per unit: the non-purchase twin of the purchase ``unit`` rung.
It serves a row under the same condition as the item fallback -- the row's own
``(j, u)`` cell has no priced purchase in any wave -- and only where no rung
above serves it.

What these tests pin:

- its place in the ladder, and that it is labelled and counted (the partition
  sums, and ``attrs['kg_factor_evidence']['fallback']['unit_rung']`` agrees);
- it serves only no-purchase cells the item rungs could not serve, pooling the
  non-purchase rows of every item sharing the label;
- a cell with any purchase evidence never reaches it, even where the purchase
  evidence was gated out and the label has a unit-level non-purchase factor;
- it has the purchase ``unit`` rung's gates: none of the item floors, but the
  currency, missing-unit and ``no_kg`` refusals, and ``survey_kg`` /
  ``metric`` / every rung above outrank it;
- its ratio is self-valued on both sides, so a non-purchase price scale
  cancels;
- a row the item fallback's dispersion gate refuses lands on it, and the gate
  ledger says so.

The item-level rung's own tests are in ``test_gh1509_nonpurchase_fallback.py``.
Data-free: every frame here is synthetic.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    FOOD_KG_FACTOR_LAYERS,
    FOOD_KG_NONPURCHASE_LAYER,
    FOOD_KG_NONPURCHASE_LAYERS,
    FOOD_KG_UNIT_NONPURCHASE_LAYER,
    U_UNKNOWN,
    food_kg_factors,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)

IDX = ['t', 'i', 'j', 'u', 's']
NP = FOOD_KG_NONPURCHASE_LAYER
UNP = FOOD_KG_UNIT_NONPURCHASE_LAYER


def frame(rows):
    """``(t, i, j, u, s)`` frame from ``(t, i, j, u, s, Quantity, Expenditure)``."""
    idx = pd.MultiIndex.from_tuples([r[:5] for r in rows], names=IDX)
    return pd.DataFrame({'Quantity': [float(r[5]) for r in rows],
                         'Expenditure': [float(r[6]) for r in rows]}, index=idx)


def quiet(fn, *a, **k):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return fn(*a, **k)


def households(item, unit, n, *, s='produced', t='2020', prefix='g',
               kg_price=100.0, unit_price=300.0, with_kg=True):
    """*n* households, each with a *unit* row and (if *with_kg*) a kg row.

    Within each household the unit is ``unit_price / kg_price`` kilograms,
    which is the ratio the u-pooled estimator reads.
    """
    rows = []
    for k in range(n):
        h = f'{prefix}{k}'
        if with_kg:
            rows.append((t, h, item, 'kg', s, 2, 2 * kg_price))
        rows.append((t, h, item, unit, s, 1, unit_price))
    return rows


def orphans(item, unit, n, *, s='produced', t='2020', prefix='o',
            unit_price=300.0):
    """*n* households reporting only a priced *unit* row: no kg row of any kind."""
    return [(t, f'{prefix}{k}', item, unit, s, 1, unit_price) for k in range(n)]


def factors(rows, **kw):
    df = frame(rows)
    f = quiet(food_kg_factors, df, **kw)
    counts = f.attrs['kg_factor_sources']
    assert set(counts) == set(FOOD_KG_FACTOR_LAYERS)
    assert sum(counts.values()) == len(df)
    assert set(f['KgFactorSource']) <= set(FOOD_KG_FACTOR_LAYERS)
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['rows_served'] == counts[NP]
    assert fb['unit_rung']['layer'] == UNP
    assert fb['unit_rung']['rows_served'] == counts[UNP]
    assert fb['rows_refused_spread'] == (fb['refused_spread_to_unit_nonpurchase']
                                         + fb['refused_spread_to_none'])
    assert sum(fb['none_by_reason'].values()) == counts['none']
    pool = f.attrs['kg_factor_size_pool']
    assert pool['rows_served'] == (pool['served_by_item_rung']
                                   + pool['served_by_unit_rung'])
    return df, f


def cell(f, j, u):
    sub = f[f.index.get_level_values('j') == j]
    return sub[sub.index.get_level_values('u') == u]


# ---------------------------------------------------------------------------
# The ladder
# ---------------------------------------------------------------------------

def test_the_rung_sits_between_the_item_fallback_and_none():
    # Pinned 2026-11-12 ("Add a unit-level rung"): the second non-purchase rung
    # goes directly below the first and directly above none.
    layers = FOOD_KG_FACTOR_LAYERS
    assert UNP == 'unit_nonpurchase'
    assert FOOD_KG_NONPURCHASE_LAYERS == (NP, UNP)
    assert layers.index(UNP) == layers.index(NP) + 1
    assert layers.index('none') == layers.index(UNP) + 1
    assert layers.index('unit') < layers.index(NP)


# ---------------------------------------------------------------------------
# What it serves
# ---------------------------------------------------------------------------

def test_it_serves_a_no_purchase_cell_the_item_rung_cannot_and_is_labelled():
    """Two kg households: below ``min_baseline``, so the item rung has no factor.

    The u-pooled estimator reads each household's own Heap over its own
    kilogram, 300 / 100 = 3, and the rung serves every Heap row with it.
    """
    rows = households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18)
    df, f = factors(rows)
    heap = cell(f, 'yam', 'Heap')
    assert len(heap) == 20
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    assert heap['kg_item_unit'].isna().all() and heap['kg_unit'].isna().all()
    assert set(heap['KgFactorSource']) == {UNP}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    assert heap['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    assert not heap['kg_cell_has_purchase'].any()
    counts = f.attrs['kg_factor_sources']
    assert counts == {**{k: 0 for k in FOOD_KG_FACTOR_LAYERS},
                      'metric': 2, UNP: 20}
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['ran'] is True
    assert fb['rows_served'] == 0
    assert fb['unit_rung'] == {'layer': UNP, 'rows_served': 20,
                               'rows_served_by_s': {'produced': 20},
                               'cells_served': 1, 'labels_served': 1}
    assert fb['none_by_reason']['no_estimate'] == 0


def test_it_pools_across_items_sharing_the_label():
    """Yam has no kg row at all; cassava's households price a Heap at 2 kg.

    The item fallback cannot serve yam (no ``(t, yam)`` baseline), and the
    unit-level rung serves it cassava's label factor -- which is what pooled
    across items means, and why it is the weaker of the two rungs.
    """
    rows = orphans('yam', 'Heap', 20) + households(
        'cassava', 'Heap', 10, prefix='c', kg_price=50.0, unit_price=100.0)
    _, f = factors(rows)
    yam = cell(f, 'yam', 'Heap')
    assert set(yam['KgFactorSource']) == {UNP}
    assert yam['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 2.0))
    # Cassava's own cell is served by the item fallback, which outranks it.
    cassava = cell(f, 'cassava', 'Heap')
    assert set(cassava['KgFactorSource']) == {NP}
    assert cassava['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(10, 2.0))
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['unit_rung']['rows_served'] == 20
    assert fb['unit_rung']['cells_served'] == 1
    # Without cassava there is no household ratio to read: yam stays none.
    _, g = factors(orphans('yam', 'Heap', 20))
    assert set(cell(g, 'yam', 'Heap')['KgFactorSource']) == {'none'}
    assert g.attrs['kg_factor_evidence']['fallback']['none_by_reason'][
        'no_estimate'] == 20


def test_the_item_fallback_outranks_it_where_both_have_a_factor():
    """Yam's own cell says 3 kg; the label, pooled over 30 cassava households, 2."""
    rows = households('yam', 'Heap', 20) + households(
        'cassava', 'Heap', 30, prefix='c', kg_price=50.0, unit_price=100.0)
    _, f = factors(rows)
    yam = cell(f, 'yam', 'Heap')
    assert set(yam['KgFactorSource']) == {NP}
    assert yam['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    assert yam['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 2.0))
    assert f.attrs['kg_factor_sources'][UNP] == 0


def test_a_purchase_unit_factor_outranks_it():
    """Cassava purchases give the label a purchase ``unit`` factor of 4 kg."""
    rows = households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18)
    rows += households('cassava', 'Heap', 10, s='purchased', prefix='p',
                       kg_price=25.0, unit_price=100.0)
    _, f = factors(rows)
    yam = cell(f, 'yam', 'Heap')
    assert set(yam['KgFactorSource']) == {'unit'}
    assert yam['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 4.0))
    # The candidate is still the non-purchase households' 3.0.
    assert yam['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    assert f.attrs['kg_factor_sources'][UNP] == 0


def test_survey_kg_and_metric_outrank_it():
    df = frame(households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18))
    df['Quantity_kg'] = np.nan
    heap = df.index.get_level_values('u') == 'Heap'
    first = np.flatnonzero(heap)[0]
    df.iloc[first, df.columns.get_loc('Quantity_kg')] = 4.0
    f = quiet(food_kg_factors, df)
    assert f['KgFactorSource'].iloc[first] == 'survey_kg'
    assert (f['KgFactorSource'].to_numpy()[heap] == UNP).sum() == 19
    assert set(f.xs('kg', level='u')['KgFactorSource']) == {'metric'}


# ---------------------------------------------------------------------------
# A cell with ANY purchase evidence never reaches it
# ---------------------------------------------------------------------------

def test_a_purchase_cell_below_the_floors_never_reaches_it():
    """Two Heap purchases clear no floor and give the label no purchase factor.

    The produced households give the label a unit-level factor of 3.0, and the
    yam Heap cell still never reads it, because the cell has purchases.
    """
    rows = households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18)
    rows += orphans('yam', 'Heap', 2, s='purchased', prefix='p', unit_price=400.0)
    _, f = factors(rows)
    heap = cell(f, 'yam', 'Heap')
    assert set(heap['KgFactorSource']) == {'none'}
    assert heap['kg_cell_has_purchase'].all()
    assert heap['kg_unit_nonpurchase'].isna().all()
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['cell_has_purchase'] == 22
    assert f.attrs['kg_factor_sources'][UNP] == 0
    # Take the two purchases away and the same produced rows read the rung.
    _, g = factors(households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18))
    assert set(cell(g, 'yam', 'Heap')['KgFactorSource']) == {UNP}


def _wild(item, s, n=10, prefix='w'):
    """kg rows whose prices span 1 to 1,000 across households: p90/p10 ~ 254."""
    return [('2020', f'{prefix}{k}', item, 'kg', s, 1, 10 ** (k / 3))
            for k in range(n)]


def _control(s):
    """A clean rice cell (a Bag is 10 kg) that survives every gate."""
    rows = []
    for k in range(20):
        rows += [('2020', f'r{k}', 'rice', 'kg', s, 1, 50),
                 ('2020', f'r{k}', 'rice', 'Bag', s, 1, 500)]
    return rows


def test_a_purchase_cell_gated_out_never_reaches_it():
    """The dispersion gate refuses banana's purchase baseline, and no household
    bought both a kg and a Bunch, so the purchase ``unit`` arm has nothing.

    Plantain's produced households give the Bunch label a unit-level
    non-purchase factor of 3.0.  The banana Bunch cell has purchases, so its
    rows -- purchased and produced -- stay ``none``; plantain's cell, which has
    none, is served by the item fallback.
    """
    rows = _wild('banana', 'purchased', prefix='h')
    rows += orphans('banana', 'Bunch', 10, s='purchased', prefix='b')
    rows += orphans('banana', 'Bunch', 10, prefix='q')           # produced
    rows += households('plantain', 'Bunch', 20, prefix='g')
    rows += _control('purchased')
    _, f = factors(rows)
    banana = cell(f, 'banana', 'Bunch')
    assert len(banana) == 20
    assert set(banana['KgFactorSource']) == {'none'}
    assert banana['kg_cell_has_purchase'].all()
    assert banana['baseline_refused_spread'].all()
    assert banana['kg_unit_nonpurchase'].isna().all()
    gate = f.attrs['kg_factor_baseline_gate']
    assert gate['rows_refused'] == gate['refused_to_unit'] + gate['refused_to_none']
    assert gate['refused_to_none'] == 20
    plantain = cell(f, 'plantain', 'Bunch')
    assert set(plantain['KgFactorSource']) == {NP}
    assert plantain['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    assert f.attrs['kg_factor_sources'][UNP] == 0
    # The same produced banana rows, with the purchases gone, read the rung.
    _, g = factors(orphans('banana', 'Bunch', 10, prefix='q')
                   + households('plantain', 'Bunch', 20, prefix='g'))
    assert set(cell(g, 'banana', 'Bunch')['KgFactorSource']) == {UNP}


def test_purchase_evidence_in_another_wave_blocks_it():
    rows = households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18)
    rows += orphans('yam', 'Heap', 2, s='purchased', t='2021', prefix='p')
    _, f = factors(rows)
    early = cell(f, 'yam', 'Heap')
    early = early[early.index.get_level_values('t') == '2020']
    assert set(early['KgFactorSource']) == {'none'}
    sub = quiet(food_kg_factors, frame(rows), serve_waves=['2020'])
    assert sub.attrs['kg_factor_sources'][UNP] == 0


def test_an_unsized_label_with_a_purchased_sized_sibling_never_reaches_it():
    rows = households('mil', 'Tas', 2) + orphans('mil', 'Tas', 18)
    rows += orphans('mil', 'Tas (Petit)', 2, s='purchased', prefix='p')
    _, f = factors(rows)
    tas = cell(f, 'mil', 'Tas')
    assert set(tas['KgFactorSource']) == {'none'}
    assert tas['kg_cell_has_purchase'].all()


def test_an_unsized_label_reads_its_size_pooled_family_on_this_rung():
    """Bare ``Tas`` rows of mil have no kg household; sorgho's ``Tas (Petit)``
    households price it at 1.5 kg.  The bare label reads its family, sizes
    stripped, so it is served 1.5 on this rung and is counted as pooled."""
    rows = orphans('mil', 'Tas', 20)
    rows += households('sorgho', 'Tas (Petit)', 10, prefix='q', unit_price=150.0)
    _, f = factors(rows)
    tas = cell(f, 'mil', 'Tas')
    assert set(tas['KgFactorSource']) == {UNP}
    assert tas['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 1.5))
    assert tas['kg_size_pooled'].all()
    pool = f.attrs['kg_factor_size_pool']
    assert pool['served_by_unit_rung'] == 20
    # Without the sized sibling the bare label has no household ratio.
    _, g = factors(orphans('mil', 'Tas', 20))
    assert set(cell(g, 'mil', 'Tas')['KgFactorSource']) == {'none'}


# ---------------------------------------------------------------------------
# Its gates
# ---------------------------------------------------------------------------

def test_the_item_fallbacks_dispersion_gate_refusal_lands_on_it():
    """The twin of a refused purchase row landing on ``unit``.

    Yam's produced kg rows disagree 1,000-fold across ten households, so the
    item fallback's baseline is refused; each of those households also reports
    a Heap, and the u-pooled estimator (which has no dispersion gate, like
    ``unit``) serves the median of their own ratios.
    """
    rows = _wild('yam', 'produced', prefix='h')
    rows += [('2020', f'h{k}', 'yam', 'Heap', 'produced', 1, 300)
             for k in range(10)]
    rows += _control('produced')
    _, f = factors(rows)
    heap = cell(f, 'yam', 'Heap')
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    assert set(heap['KgFactorSource']) == {UNP}
    expected = np.median([300 / 10 ** (k / 3) for k in range(10)])
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(10, expected))
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['rows_refused_spread'] == 10
    assert fb['refused_spread_to_unit_nonpurchase'] == 10
    assert fb['refused_spread_to_none'] == 0
    # The purchase gate's ledger is about purchase baselines only.
    assert f.attrs['kg_factor_baseline_gate']['rows_refused'] == 0


def test_a_declared_no_kg_cell_refuses_it():
    rows = households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18)
    _, f = factors(rows, no_kg=[('yam', 'Heap')])
    heap = cell(f, 'yam', 'Heap')
    assert set(heap['KgFactorSource']) == {'none'}
    assert heap['kg_unit_nonpurchase'].isna().all()
    declared = f.attrs['kg_factor_declared_none']
    assert declared['rows_matched'] == 20
    assert declared['rows_refused'] == 20        # this rung WOULD have served
    assert f.attrs['kg_factor_evidence']['fallback']['none_by_reason'][
        'declared_no_kg'] == 20


def test_a_missing_unit_never_reads_it():
    rows = households('yam', U_UNKNOWN, 2) + orphans('yam', U_UNKNOWN, 18)
    _, f = factors(rows)
    rows_u = cell(f, 'yam', U_UNKNOWN)
    assert set(rows_u['KgFactorSource']) == {'none'}
    assert rows_u['kg_unit_nonpurchase'].isna().all()
    assert f.attrs['kg_factor_evidence']['fallback']['none_by_reason'][
        'missing_unit'] == 20


def test_a_currency_unit_never_reads_it():
    rows = households('yam', 'Heap', 2) + [
        ('2020', f'g{k}', 'yam', 'Value', 'produced', 50, 50) for k in range(2)]
    _, f = factors(rows)
    value = cell(f, 'yam', 'Value')
    assert set(value['KgFactorSource']) == {'none'}
    assert value['kg_unit_nonpurchase'].isna().all()
    assert f.attrs['kg_factor_evidence']['fallback']['none_by_reason'][
        'currency_unit'] == 2


# ---------------------------------------------------------------------------
# Self-valued on both sides
# ---------------------------------------------------------------------------

def test_its_ratio_is_self_valued_on_both_sides():
    """The household bought a kg at 100 and valued its own kg at 20, Heap at 60.

    Over its own produced kg the Heap is 3 kg; over the purchased kg it would
    be 0.6.  The yam Heap cell has no purchase, so this rung serves it, at 3.
    """
    rows = []
    for k in range(2):
        rows += [('2020', f'g{k}', 'yam', 'kg', 'purchased', 1, 100),
                 ('2020', f'g{k}', 'yam', 'kg', 'produced', 2, 40),
                 ('2020', f'g{k}', 'yam', 'Heap', 'produced', 1, 60)]
    rows += orphans('yam', 'Heap', 18, unit_price=60.0)
    df, f = factors(rows)
    heap = cell(f, 'yam', 'Heap')
    assert set(heap['KgFactorSource']) == {UNP}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    scaled = df.copy()
    not_bought = df.index.get_level_values('s') != 'purchased'
    scaled.loc[not_bought, 'Expenditure'] *= 1000
    g = quiet(food_kg_factors, scaled)
    pd.testing.assert_series_equal(f['kg_per_unit'], g['kg_per_unit'])
    pd.testing.assert_series_equal(f['KgFactorSource'], g['KgFactorSource'])


def test_adding_it_moves_no_row_a_purchase_rung_serves():
    banana = []
    for k in range(20):
        banana += [('2020', f'h{k}', 'banana', 'kg', 'purchased', 2, 200),
                   ('2020', f'h{k}', 'banana', 'Bunch', 'purchased', 1, 300)]
    _, alone = factors(banana)
    _, both = factors(banana + households('yam', 'Heap', 2)
                      + orphans('yam', 'Heap', 18))
    is_banana = both.index.get_level_values('j') == 'banana'
    np.testing.assert_array_equal(both['kg_per_unit'].to_numpy()[is_banana],
                                  alone['kg_per_unit'].to_numpy())
    np.testing.assert_array_equal(both['KgFactorSource'].to_numpy()[is_banana],
                                  alone['KgFactorSource'].to_numpy())
    assert both.attrs['kg_factor_sources'][UNP] == 20


# ---------------------------------------------------------------------------
# What carries it
# ---------------------------------------------------------------------------

def test_a_subset_is_a_slice_of_the_all_waves_result():
    rows = (households('yam', 'Heap', 2, t='2020') + orphans('yam', 'Heap', 18, t='2020')
            + households('yam', 'Heap', 2, t='2021', prefix='w', unit_price=360.0)
            + orphans('yam', 'Heap', 18, t='2021', prefix='x'))
    df = frame(rows)
    whole = quiet(food_kg_factors, df)
    sub = quiet(food_kg_factors, df, serve_waves=['2021'])
    in_2021 = whole.index.get_level_values('t') == '2021'
    np.testing.assert_allclose(sub['kg_per_unit'].to_numpy(),
                               whole['kg_per_unit'].to_numpy()[in_2021])
    np.testing.assert_array_equal(sub['KgFactorSource'].to_numpy(),
                                  whole['KgFactorSource'].to_numpy()[in_2021])
    # One number per label: the median of the four households' 3.0 and 3.6.
    assert cell(sub, 'yam', 'Heap')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(20, 3.3))
    assert sub.attrs['kg_factor_evidence']['fallback']['unit_rung'][
        'rows_served'] == 20


def test_the_derived_tables_convert_and_disclose_its_rows():
    df = frame(households('yam', 'Heap', 2) + orphans('yam', 'Heap', 18))
    q = quiet(food_quantities_from_acquired, df, units='kgs')
    # 2 households x 2 kg, plus 20 Heaps x 3 kg.
    assert q.xs('kg', level='u')['Quantity'].sum() == pytest.approx(64.0)
    assert q.attrs['kg_factor_sources'][UNP] == 20
    assert q.attrs['kg_factor_evidence']['fallback']['unit_rung'][
        'rows_served'] == 20
    p = quiet(food_prices_from_acquired, df, units='kgvalue')
    assert p.attrs['kg_factor_sources'][UNP] == 20
    assert p['Price'].to_numpy() == pytest.approx(np.full(len(p), 100.0))
