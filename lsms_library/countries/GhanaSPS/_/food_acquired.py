#!/usr/bin/env python
"""Concatenate wave-level food_acquired for GhanaSPS.

Each wave script ({wave}/_/food_acquired.py) emits canonical (t, i, j, u, s)
with [Quantity, Expenditure]; this just stacks the waves and writes the
country-level parquet.  food_prices / food_quantities / food_expenditures are
derived at API time by _FOOD_DERIVED -- there is no per-country derivation
script (the retired food_prices_quantities_and_expenditures.py).

GhanaSPS has no `sample` table (cluster identity unavailable), so `v` is not
joined: the canonical index here is (t, i, j, u, s) without v.  This is an
accepted data gap; a sample/v follow-up will fold GhanaSPS into the default
Feature() assembly later.
"""
import pandas as pd

from lsms_library.local_tools import to_parquet, get_dataframe

X = []
for t in ['2009-10', '2013-14', '2017-18']:
    X.append(get_dataframe('../%s/_/food_acquired.parquet' % t))

x = pd.concat(X, axis=0)

to_parquet(x, '../var/food_acquired.parquet')
