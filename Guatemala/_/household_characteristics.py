#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2000']
for t in years:
    x.append(pd.read_parquet('../'+t+'/_/household_characteristics.parquet'))

concatenated = pd.concat(x)

concatenated.to_parquet('household_characteristics.parquet')
