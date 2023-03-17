#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2000']
for t in years:
    x.append(pd.read_parquet('../'+t+'/_/household_characteristics.parquet'))

hc = pd.concat(x)

of = pd.read_parquet('../var/other_features.parquet')

hc = hc.join(of.reset_index('m')['m'],on=['j','t'])
hc = hc.reset_index().set_index(['j','t','m'])

hc.to_parquet('household_characteristics.parquet')
