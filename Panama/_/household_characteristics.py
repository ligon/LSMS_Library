#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['1997', '2003', '2008']:
    x.append(pd.read_parquet('../'+t+'/_/household_characteristics.parquet'))

concatenated = pd.concat(x)

concatenated.to_parquet('household_characteristics.parquet')
