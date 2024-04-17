#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2014', '2018-19', '2021-22']:
    df = pd.read_parquet('../'+t+'/_/other_features.parquet')
    x.append(df)

concatenated = pd.concat(x).reset_index()
concatenated['m'] = concatenated['m'].astype(str)
concatenated = concatenated.set_index(['j', 't', 'm'])

concatenated.to_parquet('../var/other_features.parquet')
