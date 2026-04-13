#!/usr/bin/env python3
from lsms_library.local_tools import to_parquet
import numpy as np
import pandas as pd
import dvc.api
from ligonlibrary.dataframes import from_dta

fn = '../Data/gsec14.dta'

with dvc.api.open(fn, mode='rb') as dta:
    df = from_dta(dta)

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
