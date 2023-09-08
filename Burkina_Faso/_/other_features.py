#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2014', '2018-19']:
    df = pd.read_parquet('../'+t+'/_/other_features.parquet')
    x.append(df)

concatenated = pd.concat(x)
concatenated['m'] = concatenated['m'].astype(str)

concatenated.to_parquet('../var/other_features.parquet')
