"""Unit tests for food_expenditures_from_acquired(basis=) — GH #575.

Synthetic food_acquired frames (no data build) exercise the purchased-only
default vs the total-recorded-value basis, the no-``s`` fallback, and
validation.
"""

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import food_expenditures_from_acquired


def _fa_with_s():
    """A canonical (t,i,j,u,s) food_acquired: HH 'h1' buys + grows item j1;
    HH 'h2' ONLY grows j1 (no purchased row — a subsistence household)."""
    rows = [
        # t,   i,    j,    u,     s,           Quantity, Expenditure
        ('2020', 'h1', 'j1', 'kg', 'purchased', 10.0, 100.0),
        ('2020', 'h1', 'j1', 'kg', 'produced',   5.0,  40.0),   # source recorded a value
        ('2020', 'h1', 'j2', 'kg', 'purchased',  2.0,  30.0),
        ('2020', 'h2', 'j1', 'kg', 'produced',   8.0,  60.0),   # subsistence-only HH
    ]
    idx = pd.MultiIndex.from_tuples([r[:5] for r in rows],
                                    names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [r[5] for r in rows],
                         'Expenditure': [r[6] for r in rows]}, index=idx)


class TestBasisDefault:
    def test_default_is_purchased(self):
        """Default basis keeps only s=='purchased' rows."""
        out = food_expenditures_from_acquired(_fa_with_s())
        assert set(out.index.get_level_values('s')) == {'purchased'}

    def test_purchased_drops_subsistence_only_household(self):
        """A HH whose food is entirely own-production has no cash expenditure
        and is absent from the purchased (default) view."""
        out = food_expenditures_from_acquired(_fa_with_s(), basis='purchased')
        assert 'h2' not in set(out.index.get_level_values('i'))
        # h1 purchased total = 100 (j1) + 30 (j2)
        assert out['Expenditure'].sum() == pytest.approx(130.0)

    def test_total_includes_recorded_nonpurchased(self):
        """basis='total' sums recorded value across all sources, incl. the
        subsistence HH, and is >= purchased."""
        out = food_expenditures_from_acquired(_fa_with_s(), basis='total')
        assert {'purchased', 'produced'} <= set(out.index.get_level_values('s'))
        assert 'h2' in set(out.index.get_level_values('i'))
        # 100 + 40 + 30 + 60
        assert out['Expenditure'].sum() == pytest.approx(230.0)


class TestBasisEdgeCases:
    def test_no_s_level_bases_coincide(self):
        """With no s level there is no split — both bases sum all rows."""
        idx = pd.MultiIndex.from_tuples(
            [('2020', 'h1', 'j1'), ('2020', 'h1', 'j2')],
            names=['t', 'i', 'j'])
        df = pd.DataFrame({'Quantity': [1.0, 2.0], 'Expenditure': [10.0, 20.0]},
                          index=idx)
        a = food_expenditures_from_acquired(df, basis='purchased')
        b = food_expenditures_from_acquired(df, basis='total')
        assert a['Expenditure'].sum() == b['Expenditure'].sum() == pytest.approx(30.0)

    def test_invalid_basis_raises(self):
        with pytest.raises(ValueError, match="basis="):
            food_expenditures_from_acquired(_fa_with_s(), basis='bogus')

    def test_missing_expenditure_raises(self):
        df = _fa_with_s().drop(columns=['Expenditure'])
        with pytest.raises(ValueError, match="Expenditure"):
            food_expenditures_from_acquired(df)
