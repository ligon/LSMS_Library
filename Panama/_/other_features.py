#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['1997', '2003', '2008']:
    df = pd.read_parquet('../'+t+'/_/other_features.parquet')
    df = df.reset_index()
    df['t'] = t
    df['j'] = t + df['j']
    df = df.set_index(['j', 't', 'm'])
    x.append(df)

concatenated = pd.concat(x)
concatenated.replace({'Comarca de San Blas': 'Comarca Kuna Yala'})

concatenated.to_parquet('../var/other_features.parquet')
