#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2014', '2018-19', '2021-22']:
    df = get_dataframe('../'+t+'/_/other_features.parquet')
    x.append(df)

concatenated = pd.concat(x).reset_index()
concatenated['m'] = concatenated['m'].astype(str)
concatenated = concatenated.set_index(['j', 't', 'm'])

to_parquet(concatenated, '../var/other_features.parquet')
