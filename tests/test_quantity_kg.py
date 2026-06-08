"""GH #378 / DESIGN_per_row_kg_quantity: exact per-row Quantity_kg carry.

A wave that supplies a per-row ``kg_factor`` (e.g. Nigeria's s10bq2_cvn)
gets a summable ``Quantity_kg`` through the canonical melt + finalize, and
``food_quantities`` prefers it over the unit->factor estimate.  Countries
that don't supply it are byte-identical to before (additive).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from lsms_library.transformations import (
    _apply_kg_conversion,
    food_acquired_to_canonical,
    food_quantities_from_acquired,
)


def _wide(kg_factor=None):
    # one household, one item, unit 'Loaf' (no metric factor), Total=4 Produced=1
    idx = pd.MultiIndex.from_tuples([('2019', 'hh1', 'Bread', 'Loaf')],
                                    names=['t', 'i', 'j', 'u'])
    d = {'Quantity': [4.0], 'Produced': [1.0], 'Expenditure': [200.0]}
    if kg_factor is not None:
        d['kg_factor'] = [kg_factor]
    return pd.DataFrame(d, index=idx)


def test_melt_carries_quantity_kg_per_source():
    out = food_acquired_to_canonical(_wide(kg_factor=0.5), drop_columns=())
    assert 'Quantity_kg' in out.columns
    # purchased = Total-Produced = 3 -> 1.5 kg; produced = 1 -> 0.5 kg
    by_s = out.reset_index().set_index('s')['Quantity_kg'].to_dict()
    assert abs(by_s['purchased'] - 1.5) < 1e-9
    assert abs(by_s['produced'] - 0.5) < 1e-9


def test_no_kg_factor_means_no_quantity_kg_column():
    out = food_acquired_to_canonical(_wide(kg_factor=None), drop_columns=())
    assert 'Quantity_kg' not in out.columns


def test_apply_kg_conversion_prefers_precomputed():
    idx = pd.MultiIndex.from_tuples(
        [('2019', 'hh1', 'Bread', 'loaf'), ('2019', 'hh1', 'Rice', 'kg')],
        names=['t', 'i', 'j', 'u'])
    # row0: precomputed Quantity_kg=1.5 (exact); row1: NaN -> factor fallback
    df = pd.DataFrame({'Quantity': [3.0, 2.0], 'Quantity_kg': [1.5, np.nan]},
                      index=idx)
    v = _apply_kg_conversion(df, {'kg': 1.0, 'loaf': 99.0})
    # loaf row keeps the exact 1.5 (NOT 3*99); kg row filled from factor = 2.0
    assert abs(v['Quantity_kg'].iloc[0] - 1.5) < 1e-9
    assert abs(v['Quantity_kg'].iloc[1] - 2.0) < 1e-9


def test_food_quantities_uses_exact_kg():
    out = food_acquired_to_canonical(_wide(kg_factor=0.5), drop_columns=())
    fq = food_quantities_from_acquired(out, units='kgs')
    # both sources convert to kg (u='kg'); total = 1.5 + 0.5 = 2.0
    total = fq.xs('kg', level='u')['Quantity'].sum()
    assert abs(total - 2.0) < 1e-9
