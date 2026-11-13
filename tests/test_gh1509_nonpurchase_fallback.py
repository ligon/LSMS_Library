"""The disclosed fallback rung of GH #1509: ``item_unit_nonpurchase``.

Amendment by @ligon, 2026-11-12 ("Fallback rung, disclosed").  The food kg
price ratio reads purchases only, EXCEPT that a ``(j, u)`` cell with no
purchase evidence in ANY wave may fall back to non-purchase (self-valued)
evidence, on a NEW, separately labelled rung of ``FOOD_KG_FACTOR_LAYERS``, so
those rows are visibly weaker evidence.

What these tests pin:

- the rung's place in the ladder (@ligon 2026-11-12, "Item self-valued
  first"): below the purchase item rungs, ABOVE the u-pooled purchase rung
  ``unit``, then ``unit_nonpurchase``, then ``none``.  The purchase item rungs
  never compete with it (they need purchases in the cell, it needs none);
  against ``unit`` it wins;
- a cell with ANY purchase evidence never reads it, even when that evidence
  fails a floor or the dispersion gate, and "any" means any wave and, for an
  unsized label, its size-pooled family;
- the fallback's ratio is self-valued on BOTH sides, so a non-purchase price
  scale cancels in it;
- it respects ``min_reports``, ``min_baseline`` (and its tight exception), the
  dispersion gate, ``no_kg`` declarations and missing-unit sentinels;
- the partition still sums to the frame, and ``attrs['kg_factor_evidence']``
  carries the rung's own account.

The second, unit-level rung below it (``unit_nonpurchase``, @ligon
2026-11-12, "Add a unit-level rung") has its own file,
``test_gh1509_unit_nonpurchase_rung.py``.  Where a frame here leaves the item
fallback gated out and a household ratio is there to read, the row now lands
on that rung rather than ``none``, and the tests below say so.

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


def own(item='yam', unit='Heap', n=20, *, s='produced', t='2020', kg_hh=None,
        kg_price=100.0, unit_price=300.0, prefix='g'):
    """Self-valued rows: a kg at *kg_price* and a *unit* at *unit_price*.

    ``kg_hh`` households report the kg row (default: all *n*), so the
    non-purchase factor is ``unit_price / kg_price``.
    """
    kg_hh = n if kg_hh is None else kg_hh
    rows = [(t, f'{prefix}{k}', item, 'kg', s, 2, 2 * kg_price)
            for k in range(kg_hh)]
    rows += [(t, f'{prefix}{k}', item, unit, s, 1, unit_price)
             for k in range(n)]
    return rows


def factors(rows, **kw):
    df = frame(rows)
    f = quiet(food_kg_factors, df, **kw)
    counts = f.attrs['kg_factor_sources']
    # The partition still sums to the frame, over the new layer too.
    assert set(counts) == set(FOOD_KG_FACTOR_LAYERS)
    assert sum(counts.values()) == len(df)
    assert set(f['KgFactorSource']) <= set(FOOD_KG_FACTOR_LAYERS)
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['layer'] == NP
    assert fb['rows_served'] == counts[NP]
    assert fb['unit_rung']['layer'] == UNP
    assert fb['unit_rung']['rows_served'] == counts[UNP]
    # "Item self-valued first" put ``unit`` below the item fallback, so a row
    # its dispersion gate refuses can land there too: a third term.
    assert fb['rows_refused_spread'] == (fb['refused_spread_to_unit']
                                         + fb['refused_spread_to_unit_nonpurchase']
                                         + fb['refused_spread_to_none'])
    assert sum(fb['none_by_reason'].values()) == counts['none']
    # The purchase item rungs and the item fallback never both hold a factor
    # for one row: the first needs purchases in the cell, the second none.
    assert not (f['kg_item_unit'].notna()
                & f['kg_item_unit_nonpurchase'].notna()).any()
    return df, f


def rows_of(f, u, s=None):
    sub = f.xs(u, level='u', drop_level=False)
    if s is not None:
        sub = sub[sub.index.get_level_values('s') == s]
    return sub


# ---------------------------------------------------------------------------
# The ladder
# ---------------------------------------------------------------------------

def test_the_layer_tuple_and_the_rungs_place_in_it():
    # Moved 2026-11-12, twice.  "Add a unit-level rung" added
    # unit_nonpurchase above none.  Then "Item self-valued first" (@ligon,
    # after the red team showed the order had been an implementation choice)
    # moved item_unit_nonpurchase ABOVE the u-pooled purchase rung ``unit``;
    # unit_nonpurchase stays below ``unit``.
    assert FOOD_KG_FACTOR_LAYERS == (
        'survey_kg', 'metric', 'item_unit', 'item_unit_tight',
        'item_unit_nonpurchase', 'unit', 'unit_nonpurchase', 'none')
    assert NP == 'item_unit_nonpurchase'


@pytest.mark.parametrize('s', ['produced', 'inkind', 'other'])
def test_a_cell_without_purchases_reads_the_fallback_and_is_labelled(s):
    df, f = factors(own(s=s))
    heap = rows_of(f, 'Heap')
    assert set(heap['KgFactorSource']) == {NP}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    assert heap['kg_item_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    # The unit-level candidate is there too; the item rung outranks it.
    assert heap['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    assert not heap['kg_cell_has_purchase'].any()
    # No purchase rung read it.
    assert heap['kg_item_unit'].isna().all() and heap['kg_unit'].isna().all()
    counts = f.attrs['kg_factor_sources']
    assert counts == {**{k: 0 for k in FOOD_KG_FACTOR_LAYERS},
                      'metric': 20, NP: 20}
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['ran'] is True
    assert fb['priced_rows_read'] == {s: 40}
    assert fb['rows_served'] == 20
    assert fb['rows_served_by_s'] == {s: 20}
    assert fb['cells_served'] == 1
    assert fb['rows_tight'] == 0
    assert fb['rows_single_wave'] == 20
    # The purchase rungs' own account is unchanged by the fallback.
    ev = f.attrs['kg_factor_evidence']
    assert ev['priced_rows_read'] == 0
    assert ev['priced_rows_excluded'] == {s: 40}


def test_the_fallback_outranks_a_purchase_unit_rung():
    """Above the u-pooled purchase rung (@ligon 2026-11-12, "Item self-valued
    first").

    Moved 2026-11-12: this test was ``test_a_purchase_unit_rung_outranks_the_
    fallback`` and pinned the opposite order, which the red team showed was an
    implementation choice no ruling had made; the maintainer then ruled that
    a cell's own item, valued by the household, beats a label factor pooled
    over other items' purchases.
    """
    rows = own()
    # Cassava purchases price a Heap at twice a kg, household by household, so
    # the u-pooled purchase rung has a factor of 2.0 kg for the label.
    for k in range(10):
        rows += [('2020', f'c{k}', 'cassava', 'kg', 'purchased', 1, 50),
                 ('2020', f'c{k}', 'cassava', 'Heap', 'purchased', 1, 100)]
    _, f = factors(rows)
    yam = f[f.index.get_level_values('j') == 'yam'].xs('Heap', level='u')
    assert set(yam['KgFactorSource']) == {NP}
    assert yam['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    # The candidates are still there, beside the rung that won.
    assert yam['kg_unit'].to_numpy() == pytest.approx(np.full(20, 2.0))
    assert yam['kg_unit_nonpurchase'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))
    # Cassava's cell has purchases: it keeps its purchase item rung.
    cassava = f[f.index.get_level_values('j') == 'cassava'].xs('Heap', level='u')
    assert set(cassava['KgFactorSource']) == {'item_unit'}
    assert f.attrs['kg_factor_sources'][NP] == 20
    assert f.attrs['kg_factor_sources']['unit'] == 0
    assert f.attrs['kg_factor_sources'][UNP] == 0
    # Without the item fallback's estimate (two kg households, below
    # min_baseline) the same yam rows fall to the purchase ``unit`` rung, not
    # to the unit-level fallback, which ``unit`` outranks.
    _, g = factors(own(kg_hh=2) + rows[len(own()):])
    yam = g[g.index.get_level_values('j') == 'yam'].xs('Heap', level='u')
    assert yam['kg_item_unit_nonpurchase'].isna().all()
    assert set(yam['KgFactorSource']) == {'unit'}
    assert yam['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 2.0))


def test_the_dispersion_gates_refusal_falls_to_a_purchase_unit_factor():
    """Refused by its own dispersion gate, the item fallback's row falls to
    ``unit`` where the label has a purchase factor, and the ledger counts it
    under ``refused_spread_to_unit`` (the term "Item self-valued first" added:
    the gate cost the row its own item's factor)."""
    rows = [('2020', f'h{k}', 'yam', 'kg', 'produced', 1, 10 ** (k / 3))
            for k in range(10)]
    rows += [('2020', f'g{k}', 'yam', 'Heap', 'produced', 1, 300)
             for k in range(20)]
    for k in range(10):
        rows += [('2020', f'c{k}', 'cassava', 'kg', 'purchased', 1, 50),
                 ('2020', f'c{k}', 'cassava', 'Heap', 'purchased', 1, 100)]
    rows += _control('produced')
    _, f = factors(rows)
    heap = f[f.index.get_level_values('j') == 'yam'].xs('Heap', level='u')
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    assert set(heap['KgFactorSource']) == {'unit'}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 2.0))
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['rows_refused_spread'] == 20
    assert fb['refused_spread_to_unit'] == 20
    assert fb['refused_spread_to_unit_nonpurchase'] == 0
    assert fb['refused_spread_to_none'] == 0
    # The purchase gate's ledger is about purchase baselines only.
    assert f.attrs['kg_factor_baseline_gate']['rows_refused'] == 0


def test_the_fallback_ratio_is_self_valued_on_both_sides():
    """A produced Heap over a produced kilogram, not over a purchase baseline.

    Purchases price a kg at 100; own production values a kg at 20 and a Heap
    at 60.  Self-valued on both sides the Heap is 3 kg; over the purchase (or
    the pooled) baseline it would be 0.6 kg.
    """
    rows = [('2020', f'p{k}', 'yam', 'kg', 'purchased', 1, 100)
            for k in range(20)]
    rows += own(n=10, kg_price=20.0, unit_price=60.0)
    df, f = factors(rows)
    heap = rows_of(f, 'Heap')
    assert set(heap['KgFactorSource']) == {NP}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(10, 3.0))
    # So a scale on the self-valued amounts cancels: no factor moves.
    scaled = df.copy()
    not_bought = df.index.get_level_values('s') != 'purchased'
    scaled.loc[not_bought, 'Expenditure'] *= 1000
    g = quiet(food_kg_factors, scaled)
    pd.testing.assert_series_equal(f['kg_per_unit'], g['kg_per_unit'])
    pd.testing.assert_series_equal(f['KgFactorSource'], g['KgFactorSource'])


def test_the_fallback_moves_no_purchase_served_row():
    """Adding a produced-only cell moves no purchase-served row."""
    banana = []
    for k in range(20):
        banana += [('2020', f'h{k}', 'banana', 'kg', 'purchased', 2, 200),
                   ('2020', f'h{k}', 'banana', 'Bunch', 'purchased', 1, 300)]
    _, alone = factors(banana)
    _, both = factors(banana + own())
    is_banana = both.index.get_level_values('j') == 'banana'
    np.testing.assert_array_equal(both['kg_per_unit'].to_numpy()[is_banana],
                                  alone['kg_per_unit'].to_numpy())
    np.testing.assert_array_equal(both['KgFactorSource'].to_numpy()[is_banana],
                                  alone['KgFactorSource'].to_numpy())


# ---------------------------------------------------------------------------
# A cell with ANY purchase evidence never reads it
# ---------------------------------------------------------------------------

def test_a_cell_with_purchases_never_reads_the_fallback_even_below_the_floors():
    """Two purchases clear no floor, and still the cell is a purchase cell."""
    rows = own() + [('2020', f'p{k}', 'yam', 'Heap', 'purchased', 1, 400)
                    for k in range(2)]
    _, f = factors(rows)
    heap = rows_of(f, 'Heap')
    assert set(heap['KgFactorSource']) == {'none'}
    assert heap['kg_cell_has_purchase'].all()
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    # The produced households price a Heap at 3 kg, so a unit-level
    # non-purchase factor exists for the label; the purchase cell never reads it.
    assert heap['kg_unit_nonpurchase'].isna().all()
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['cell_has_purchase'] == 22
    assert reasons['no_estimate'] == 0
    # Take the two purchases away and the same produced rows read the rung.
    _, g = factors(own())
    assert set(rows_of(g, 'Heap')['KgFactorSource']) == {NP}


def _wild_purchase_kg(item, n=10, prefix='h'):
    """Purchased kg rows whose prices span 1 to 1,000: p90/p10 ~ 254."""
    return [('2020', f'{prefix}{k}', item, 'kg', 'purchased', 1, 10 ** (k / 3))
            for k in range(n)]


def _control(s='purchased'):
    """A clean rice cell (a Bag is 10 kg) that survives every gate.

    The gate's counterfactual (``baseline_refused_spread``) is estimated only
    when the gated estimate leaves at least one item cell, a property of
    ``_inferred_kg_rungs`` that predates this rung, so each gate test carries
    one surviving cell of the regime it gates.
    """
    rows = []
    for k in range(20):
        rows += [('2020', f'r{k}', 'rice', 'kg', s, 1, 50),
                 ('2020', f'r{k}', 'rice', 'Bag', s, 1, 500)]
    return rows


def test_a_purchase_cell_refused_by_the_dispersion_gate_falls_to_none():
    """The gate's refusal stands; the self-valued estimate does not replace it."""
    rows = _wild_purchase_kg('banana')
    rows += [('2020', f'b{k}', 'banana', 'Bunch', 'purchased', 1, 300)
             for k in range(10)]
    rows += own('banana', 'Bunch')                # a clean 3.0 kg on its own
    rows += _control()
    _, f = factors(rows)
    bunch = rows_of(f, 'Bunch')
    assert set(bunch['KgFactorSource']) == {'none'}
    assert bunch['baseline_refused_spread'].all()
    assert bunch['kg_item_unit_nonpurchase'].isna().all()
    # Nor the unit-level one, although the produced households give it 3.0.
    assert bunch['kg_unit_nonpurchase'].isna().all()
    gate = f.attrs['kg_factor_baseline_gate']
    assert gate['rows_refused'] == 30
    assert gate['rows_refused'] == gate['refused_to_unit'] + gate['refused_to_none']
    assert gate['refused_to_none'] == 30
    assert f.attrs['kg_factor_evidence']['fallback']['none_by_reason'][
        'cell_has_purchase'] == 30
    # Without the purchases, the produced Bunch rows read the rung at 3.0.
    _, g = factors(own('banana', 'Bunch'))
    assert rows_of(g, 'Bunch')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(20, 3.0))


def test_a_purchase_cell_refused_by_the_gate_still_reaches_unit():
    """Gated out of ``item_unit``, a purchase cell keeps ``unit``, not the rung."""
    rows = [('2020', f'h{k}', 'banana', 'kg', 'purchased', 1, 10 ** (k / 3))
            for k in range(10)]
    rows += [('2020', f'h{k}', 'banana', 'Bunch', 'purchased', 1, 300)
             for k in range(10)]
    rows += own('banana', 'Bunch')
    rows += _control()
    _, f = factors(rows)
    bunch = rows_of(f, 'Bunch')
    assert set(bunch['KgFactorSource']) == {'unit'}
    gate = f.attrs['kg_factor_baseline_gate']
    assert gate['refused_to_unit'] == 30 and gate['refused_to_none'] == 0
    assert f.attrs['kg_factor_sources'][NP] == 0


def test_purchase_evidence_in_any_wave_blocks_the_fallback():
    later = [('2021', f'p{k}', 'yam', 'Heap', 'purchased', 1, 400)
             for k in range(2)]
    _, f = factors(own() + later)
    early = f[f.index.get_level_values('t') == '2020'].xs('Heap', level='u')
    assert set(early['KgFactorSource']) == {'none'}
    assert early['kg_cell_has_purchase'].all()
    # And so for a request that serves only the wave without the purchases.
    df = frame(own() + later)
    sub = quiet(food_kg_factors, df, serve_waves=['2020'])
    assert len(sub) == 40
    assert set(sub.xs('Heap', level='u')['KgFactorSource']) == {'none'}
    assert sub.attrs['kg_factor_sources'][NP] == 0


def test_an_unsized_label_is_judged_on_its_size_pooled_family():
    """``Tas`` reads its family's cell; a purchase of ``Tas (Petit)`` is in it."""
    bare = own('mil', 'Tas')
    # (a) No sized sibling: the bare cell is its own, and produced-only.
    _, f = factors(bare)
    assert set(rows_of(f, 'Tas')['KgFactorSource']) == {NP}
    # (b) A PURCHASED sized sibling puts purchase evidence in the family.
    sized = [('2020', f'p{k}', 'mil', 'Tas (Petit)', 'purchased', 1, 150)
             for k in range(2)]
    _, g = factors(bare + sized)
    tas = rows_of(g, 'Tas')
    assert set(tas['KgFactorSource']) == {'none'}
    assert tas['kg_cell_has_purchase'].all()
    # (c) A PRODUCED sized sibling leaves the family produced-only: the bare
    #     rows read the pooled non-purchase cell, and are marked pooled.
    own_sized = [('2020', f'q{k}', 'mil', 'Tas (Petit)', 'produced', 1, 150)
                 for k in range(2)]
    _, h = factors(bare + own_sized)
    tas = rows_of(h, 'Tas')
    assert set(tas['KgFactorSource']) == {NP}
    assert tas['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    assert tas['kg_size_pooled'].all()
    pool = h.attrs['kg_factor_size_pool']
    assert pool['rows_served'] == pool['served_by_item_rung'] + \
        pool['served_by_unit_rung'] == 20
    # The two sized rows read their own two-report cell: below the floor.
    assert set(rows_of(h, 'Tas (Petit)')['KgFactorSource']) == {'none'}


# ---------------------------------------------------------------------------
# The fallback respects the existing gates
# ---------------------------------------------------------------------------

def test_min_baseline_gates_the_fallback():
    # Moved 2026-11-12 ("Add a unit-level rung"): gated out of the item
    # fallback, the rows fall to the ungated unit-level rung (as a gated
    # purchase row falls to ``unit``), which the two kg households price at 3.
    _, f = factors(own(kg_hh=2))
    heap = rows_of(f, 'Heap')
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    assert set(heap['KgFactorSource']) == {UNP}
    assert heap['kg_per_unit'].to_numpy() == pytest.approx(np.full(20, 3.0))
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['no_estimate'] == 0


def test_the_tight_baseline_exception_serves_the_fallback_and_is_counted():
    _, f = factors(own(kg_hh=3))
    assert set(rows_of(f, 'Heap')['KgFactorSource']) == {NP}
    assert f.attrs['kg_factor_evidence']['fallback']['rows_tight'] == 20


def test_min_reports_gates_the_fallback():
    # Moved 2026-11-12 ("Add a unit-level rung"): min_reports gates only the
    # item fallback; the four rows fall to the ungated unit-level rung.
    _, f = factors(own(n=4, kg_hh=20))      # 20 kg reports, 4 Heap reports
    heap = rows_of(f, 'Heap')
    assert heap['kg_item_unit_nonpurchase'].isna().all()
    assert set(heap['KgFactorSource']) == {UNP}


def test_the_dispersion_gate_refuses_a_nonpurchase_baseline():
    rows = [('2020', f'h{k}', 'yam', 'kg', 'produced', 1, 10 ** (k / 3))
            for k in range(10)]
    rows += [('2020', f'g{k}', 'yam', 'Heap', 'produced', 1, 300)
             for k in range(20)]
    rows += _control('produced')
    _, f = factors(rows)
    assert set(rows_of(f, 'Heap')['KgFactorSource']) == {'none'}
    assert set(rows_of(f, 'Bag')['KgFactorSource']) == {NP}
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['rows_refused_spread'] == 20
    # The Heap households report no kg, so no household ratio exists for the
    # unit-level rung either: they stay none.
    assert fb['refused_spread_to_none'] == 20
    assert fb['refused_spread_to_unit'] == 0       # no purchase of a Heap
    assert fb['refused_spread_to_unit_nonpurchase'] == 0
    assert fb['none_by_reason']['no_estimate'] == 20
    # The purchase rungs' gate ledger is about purchase baselines only.
    assert f.attrs['kg_factor_baseline_gate']['rows_refused'] == 0
    # Gate off: the same rows read the rung at 300 over the median kg price.
    _, g = factors(rows, baseline_max_spread=None)
    median = np.median([10 ** (k / 3) for k in range(10)])
    assert set(rows_of(g, 'Heap')['KgFactorSource']) == {NP}
    assert rows_of(g, 'Heap')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(20, 300 / median))


def test_a_declared_no_kg_cell_refuses_the_fallback():
    _, f = factors(own(), no_kg=[('yam', 'Heap')])
    assert set(rows_of(f, 'Heap')['KgFactorSource']) == {'none'}
    assert rows_of(f, 'Heap')['kg_unit_nonpurchase'].isna().all()
    declared = f.attrs['kg_factor_declared_none']
    assert declared['rows_matched'] == 20
    assert declared['rows_refused'] == 20        # the rung WOULD have served
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['declared_no_kg'] == 20


def test_a_missing_unit_never_reads_the_fallback():
    _, f = factors(own(unit=U_UNKNOWN))
    assert set(rows_of(f, U_UNKNOWN)['KgFactorSource']) == {'none'}
    assert rows_of(f, U_UNKNOWN)['kg_unit_nonpurchase'].isna().all()
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['missing_unit'] == 20


def test_a_currency_unit_never_reads_the_fallback():
    rows = own() + [('2020', f'v{k}', 'yam', 'Value', 'produced', 50, 50)
                    for k in range(10)]
    _, f = factors(rows)
    assert set(rows_of(f, 'Value')['KgFactorSource']) == {'none'}
    reasons = f.attrs['kg_factor_evidence']['fallback']['none_by_reason']
    assert reasons['currency_unit'] == 10


def test_survey_kg_and_metric_outrank_the_fallback():
    df = frame(own())
    df['Quantity_kg'] = np.nan
    heap = (df.index.get_level_values('u') == 'Heap')
    first = np.flatnonzero(heap)[0]
    df.iloc[first, df.columns.get_loc('Quantity_kg')] = 4.0
    f = quiet(food_kg_factors, df)
    assert f['KgFactorSource'].iloc[first] == 'survey_kg'
    assert (f['KgFactorSource'].to_numpy()[heap] == NP).sum() == 19
    assert set(rows_of(f, 'kg')['KgFactorSource']) == {'metric'}


# ---------------------------------------------------------------------------
# What reads no fallback at all, and what carries it
# ---------------------------------------------------------------------------

def test_a_frame_without_s_makes_no_second_estimate():
    df = frame(own()).reset_index('s', drop=True)
    f = quiet(food_kg_factors, df)
    assert NP not in set(f['KgFactorSource'])
    assert UNP not in set(f['KgFactorSource'])
    fb = f.attrs['kg_factor_evidence']['fallback']
    assert fb['ran'] is False and fb['priced_rows_read'] == {}
    # Every row is evidence there, so the PURCHASE item rung serves the Heap.
    assert set(f.xs('Heap', level='u')['KgFactorSource']) == {'item_unit'}


def test_a_purchases_only_frame_makes_no_second_estimate():
    rows = []
    for k in range(20):
        rows += [('2020', f'h{k}', 'banana', 'kg', 'purchased', 2, 200),
                 ('2020', f'h{k}', 'banana', 'Bunch', 'purchased', 1, 300)]
    rows += [('2020', f'n{k}', 'yam', 'Heap', 'produced', 1, np.nan)
             for k in range(5)]                  # unpriced: nothing to read
    _, f = factors(rows)
    assert f.attrs['kg_factor_evidence']['fallback']['ran'] is False
    assert f.attrs['kg_factor_sources'][NP] == 0
    assert f.attrs['kg_factor_sources'][UNP] == 0


def test_a_subset_is_a_slice_of_the_all_waves_fallback():
    df = frame(own(t='2020', unit_price=300.0)
               + own(t='2021', unit_price=360.0, prefix='w'))
    whole = quiet(food_kg_factors, df)
    sub = quiet(food_kg_factors, df, serve_waves=['2021'])
    in_2021 = whole.index.get_level_values('t') == '2021'
    np.testing.assert_allclose(sub['kg_per_unit'].to_numpy(),
                               whole['kg_per_unit'].to_numpy()[in_2021])
    np.testing.assert_array_equal(sub['KgFactorSource'].to_numpy(),
                                  whole['KgFactorSource'].to_numpy()[in_2021])
    # One number per (j, u): the median of the two waves' 3.0 and 3.6.
    assert sub.xs('Heap', level='u')['kg_per_unit'].to_numpy() == \
        pytest.approx(np.full(20, 3.3))
    fb = sub.attrs['kg_factor_evidence']['fallback']
    assert fb['rows_served'] == 20
    assert fb['priced_rows_read'] == {'produced': 80}   # both waves read


def test_the_derived_tables_convert_and_disclose_fallback_rows():
    df = frame(own())
    q = quiet(food_quantities_from_acquired, df, units='kgs')
    assert q.xs('kg', level='u')['Quantity'].sum() == pytest.approx(100.0)
    assert q.attrs['kg_factor_sources'][NP] == 20
    assert q.attrs['kg_factor_evidence']['fallback']['rows_served'] == 20
    p = quiet(food_prices_from_acquired, df, units='kgvalue')
    assert p.attrs['kg_factor_sources'][NP] == 20
    # 300 per Heap over 3 kg, and 200 over 2 kg: 100 per kg throughout.
    assert p['Price'].to_numpy() == pytest.approx(np.full(len(p), 100.0))
