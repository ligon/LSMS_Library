#!/usr/bin/env python3
"""
Combine income from different sources to make 'total' household income.
"""

from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import pandas as pd
import numpy as np

income_sources = [('earnings',['earnings']),
                  ('enterprise_income',['profits'])]

income = 0
for source,cols in income_sources:
    per_source = get_dataframe(f'../var/{source}.parquet')[cols].sum(axis=1)
    # Collapse duplicate (t, i) rows: source tables carry per-activity rows
    # (multiple enterprises or labor activities per household).  Income is
    # the HH-level total, so sum across activities before combining sources.
    # Without this collapse, `income + income` does a pandas-cartesian on
    # duplicate indices and multiplies duplicates.
    per_source = per_source.groupby(level=list(per_source.index.names)).sum()
    income = income + per_source

income = income.replace(0,np.nan).dropna()

to_parquet(pd.DataFrame({'income':income}), '../var/income.parquet')
