#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Calculate unit values for different items across rounds.
"""

import pandas as pd
import numpy as np

q = get_dataframe('../var/food_quantities.parquet').squeeze()
# Use_units turns out to almost always be kilograms...
# Case-insensitive 'kg' to handle both legacy parquets (u='Kg') and
# Phase-4 derived ones (u='kg').
mask = q.index.get_level_values('u').astype(str).str.lower() == 'kg'
q = q[mask].droplevel('u')
q = q.replace(0.0,np.nan).dropna()

x = get_dataframe('../var/food_expenditures.parquet').stack('i')

unitvalues = (x/q).dropna().unstack('i')

to_parquet(unitvalues, '../var/unitvalues.parquet')
