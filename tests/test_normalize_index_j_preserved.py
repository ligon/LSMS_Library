"""GH #438/#275: _normalize_dataframe_index must restore a map_index()-swapped
j->i for cluster-level item features (community_prices), so the food item is
NOT dropped + collapsed away (silent data loss).

map_index() renames a 'j' index level to 'i' when a table has no household 'i'.
For a (t, v, j, u) feature that swap makes 'j' undeclared, and the old
level-drop + duplicate-collapse discarded the item entirely (one price per
cluster).  These tests pin the restore + that household 'i' is untouched.
"""
from __future__ import annotations

import pandas as pd

from lsms_library.country import _normalize_dataframe_index


def test_restores_swapped_j_for_cluster_item_feature():
    # community_prices AFTER map_index swapped j->i: index (t, v, i, u) where
    # 'i' is really the food item (no household i).
    idx = pd.MultiIndex.from_tuples(
        [('2009', '1', 'Teff', 'Kg'),
         ('2009', '1', 'Barley', 'Kg'),
         ('2009', '2', 'Teff', 'Kg')],
        names=['t', 'v', 'i', 'u'],
    )
    df = pd.DataFrame({'Price': [9.5, 3.0, 9.6]}, index=idx)
    out = _normalize_dataframe_index(df, {'index': '(t, v, j, u)'}, None)
    assert list(out.index.names) == ['t', 'v', 'j', 'u'], "i must be restored to j"
    assert len(out) == 3, "no collapse: all (t,v,j,u) rows survive"


def test_household_i_is_not_renamed():
    # A household table that genuinely declares 'i' must keep its 'i' level.
    idx = pd.MultiIndex.from_tuples(
        [('2009', 'h1'), ('2009', 'h2')], names=['t', 'i'])
    df = pd.DataFrame({'x': [1, 2]}, index=idx)
    out = _normalize_dataframe_index(df, {'index': '(t, i)'}, None)
    assert list(out.index.names) == ['t', 'i']
    assert len(out) == 2


def test_table_declaring_both_i_and_j_untouched():
    # crop_production-like (t, i, j): both present, no rename.
    idx = pd.MultiIndex.from_tuples(
        [('2009', 'h1', 'Maize'), ('2009', 'h1', 'Teff')],
        names=['t', 'i', 'j'])
    df = pd.DataFrame({'Quantity': [10, 20]}, index=idx)
    out = _normalize_dataframe_index(df, {'index': '(t, i, j)'}, None)
    assert set(out.index.names) == {'t', 'i', 'j'}
    assert len(out) == 2
