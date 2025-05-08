#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2019-20']:
    df = get_dataframe('../'+t+'/_/other_features.parquet')
    df = df.reset_index()
    df['t'] = t
    df = df.set_index(['j', 't'])
    x.append(df)

concatenated = pd.concat(x)

to_parquet(concatenated, '../var/other_features.parquet')
