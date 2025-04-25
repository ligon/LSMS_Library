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
    x.append(get_dataframe('../'+t+'/_/other_features.parquet'))

of = pd.concat(x)

to_parquet(of, '../var/other_features.parquet')
