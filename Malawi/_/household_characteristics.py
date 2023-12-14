#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2004-05', '2010-11', '2016-17', '2019-20']
for t in years:
    x.append(pd.read_parquet('../'+t+'/_/household_characteristics.parquet'))

hc = pd.concat(x)

if 'm' not in hc.index.names:
    of = pd.read_parquet('../var/other_features.parquet')

    hc = hc.join(of,on=['j','t'])
    hc = hc.reset_index().set_index(['j','t','m'])
hc.columns.name = 'k'

hc = hc.filter(regex='ales [0-9]')

hc.to_parquet('../var/household_characteristics.parquet')
