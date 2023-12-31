#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2004-05', '2010-11', '2016-17', '2019-20']
for t in years:
    x.append(pd.read_parquet('../'+t+'/_/other_features.parquet'))

of = pd.concat(x)

of.to_parquet('../var/other_features.parquet')
