#!/usr/bin/env python3
"""
Combine income from different sources to make 'total' household income.
"""

import pandas as pd
import numpy as np

income_sources = [('earnings',['earnings']),
                  ('enterprise_income',['profits'])]

income = 0
for source,cols in income_sources:
    income = income + pd.read_parquet(f'../var/{source}.parquet')[cols].sum(axis=1)

income = income.replace(0,np.nan).dropna()

pd.DataFrame({'income':income}).to_parquet('../var/income.parquet')
