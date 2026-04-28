#!/usr/bin/env python3
from lsms_library.local_tools import to_parquet, get_dataframe
import numpy as np
import pandas as pd

fn = '../Data/gsec14.dta'

df = get_dataframe(fn)

# Filter to items owned (has_it == 'yes' or similar)
# Note: h14q3/h14q03 contains 'yes'/'Yes'/'No' values; filter to keep owned items
df = df[df['h14q3'].str.lower().isin(['yes', 'yes,individually', 'yes,jointly'])]

# Set multi-level index: (household, item)
df = df.set_index(['hhid', 'h14q2'])
df.index.names = ['i', 'j']

# Extract Value column, drop NaN
assets = df['h14q5'].replace(0, np.nan).dropna()

# Write canonical format: (i, j) index with 'Value' column
to_parquet(pd.DataFrame({"Value": assets}), 'assets.parquet')

# Quantity/Age/Purchase Price columns deferred — see canonical schema for future expansion.
