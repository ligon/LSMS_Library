#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2018-19']
for t in years:
    x.append(get_dataframe('../'+t+'/_/household_characteristics.parquet'))

hc = pd.concat(x)

if 'm' not in hc.index.names:
    of = get_dataframe('../var/other_features.parquet')

    hc = hc.join(of.reset_index('m'), on=['j','t'])
    hc = hc.drop(columns = 'Rural')
    hc = hc.reset_index().set_index(['j','t','m'])
hc.columns.name = 'k'

to_parquet(hc, '../var/household_characteristics.parquet')
