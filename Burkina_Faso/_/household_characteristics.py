#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2014', '2018-19']:
    x.append(pd.read_parquet('../'+t+'/_/household_characteristics.parquet'))

concatenated = pd.concat(x)

concatenated.to_parquet('../var/household_characteristics.parquet')
