#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['1997', '2003', '2008']:
    df = pd.read_parquet('../'+t+'/_/household_characteristics.parquet')
    df = df.reset_index()
    df['j'] = t + df['j'].astype(str)
    df.replace({'Comarca de San Blas': 'Comarca Kuna Yala'})
    df = df.set_index(['j', 't', 'm'])
    df.columns.name = 'k'
    x.append(df)

concatenated = pd.concat(x)

concatenated.to_parquet('../var/household_characteristics.parquet')
