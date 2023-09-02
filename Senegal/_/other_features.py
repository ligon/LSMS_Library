#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2018-19']
for t in years:
    x.append(pd.read_parquet('../'+t+'/_/other_features.parquet'))

of = pd.concat(x)

of.to_parquet('../var/other_features.parquet')
