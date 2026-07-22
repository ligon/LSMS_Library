#!/usr/bin/env python
"""Concatenate Panama wave-level food_acquired parquets into the country table.

Each wave script (``Panama/<wave>/_/food_acquired.py``) emits the canonical
long form with index ``(t, i, j, u, s)`` and columns
``[Quantity, Expenditure, Price]`` (Phase 3 of GH #218).  ``i`` is the
household, ``j`` the harmonized food item, ``u`` the native unit, and ``s``
the acquisition source (``{purchased, produced, inkind, other}``).  ``v`` is
NOT baked in -- it is joined from ``sample()`` at API time.

The three Panama waves (1997, 2003, 2008) are independent cross-sections (no
cross-wave household panel linkage), so this is a plain concatenation.

``food_expenditures``/``food_prices``/``food_quantities`` are no longer built
here -- they are auto-derived at runtime by the framework's ``_FOOD_DERIVED``
from this canonical ``food_acquired``.
"""
import sys

sys.path.append('../../_')

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['1997', '2003', '2008']

dfs = []
for t in WAVES:
    df = get_dataframe('../' + t + '/_/food_acquired.parquet')
    if 't' not in df.index.names:
        df = df.reset_index()
        df['t'] = t
        df = df.set_index(['t', 'i', 'j', 'u', 's'])
    dfs.append(df)

fa = pd.concat(dfs)

to_parquet(fa, '../var/food_acquired.parquet')
