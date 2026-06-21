from lsms_library.local_tools import to_parquet, get_dataframe
"""Concatenate the per-wave canonical food_acquired parquets for Guatemala.

Each wave's ``{wave}/_/food_acquired.py`` already emits the canonical long
shape ``(t, i, j, u, s)`` with columns ``[Quantity, Expenditure, Price]``;
this just concatenates the (single) wave and writes the country-level
``var/food_acquired.parquet``.  The framework's ``_FOOD_DERIVED`` transforms
derive food_expenditures / food_prices / food_quantities from it at API time.
"""

import pandas as pd
import numpy as np

fa = []
for t in ['2000']:
    df = get_dataframe('../' + t + '/_/food_acquired.parquet')
    fa.append(df)

fa = pd.concat(fa)

to_parquet(fa, '../var/food_acquired.parquet')
