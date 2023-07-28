#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2007']:
    df = pd.read_parquet('../'+t+'/_/other_features.parquet')
    df = df.reset_index()
    df['t'] = t
    df = df.set_index(['j', 't'])
    x.append(df)

concatenated = pd.concat(x)

concatenated.to_parquet('../var/other_features.parquet')
