#!/usr/bin/env python
"""Concatenate wave-level food_acquired for Cambodia.

The wave script (2019-20/_/food_acquired.py) already emits canonical
(t, i, j, u, s) with [Quantity, Expenditure]; this just stacks the waves and
writes the country-level parquet.  food_prices / food_quantities /
food_expenditures are derived at API time by _FOOD_DERIVED -- no per-country
derivation script (the retired food_prices_quantities_and_expenditures.py).
"""
import pandas as pd

from lsms_library.local_tools import to_parquet, get_dataframe

X = []
for t in ['2019-20']:
    X.append(get_dataframe('../%s/_/food_acquired.parquet' % t))

x = pd.concat(X, axis=0)

to_parquet(x, '../var/food_acquired.parquet')
