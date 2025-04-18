#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Concatenate data on household characteristics across rounds.
"""
import pandas as pd

x = []
for t in ['2007']:
    df = get_dataframe('../'+t+'/_/household_characteristics.parquet')
    df['t'] = t
    x.append(df)

concatenated = pd.concat(x)

if 'm' not in concatenated.index.names:
    of = get_dataframe('../var/other_features.parquet')

    concatenated = concatenated.reset_index().merge(of.reset_index(),on=['j','t'], how='left').drop('Rural', axis=1)
    concatenated = concatenated.reset_index().set_index(['j','t','m'])
concatenated.columns.name = 'k'

to_parquet(concatenated, '../var/household_characteristics.parquet')
