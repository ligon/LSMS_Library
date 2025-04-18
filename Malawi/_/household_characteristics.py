#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2004-05', '2010-11', '2016-17', '2019-20']
for t in years:
    x.append(get_dataframe('../'+t+'/_/household_characteristics.parquet'))

hc = pd.concat(x)

if 'm' not in hc.index.names:
    of = get_dataframe('../var/other_features.parquet')

    hc = hc.join(of,on=['j','t']).drop('Rural', axis=1)
    hc = hc.reset_index().set_index(['j','t','m'])
hc.columns.name = 'k'

to_parquet(hc, '../var/household_characteristics.parquet')
