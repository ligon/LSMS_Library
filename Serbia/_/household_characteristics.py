#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

x = []
for t in ['2007']:
    df = pd.read_parquet('../'+t+'/_/household_characteristics.parquet')
    x.append(df)

concatenated = pd.concat(x)

concatenated.to_parquet('../var/other_features.parquet')
