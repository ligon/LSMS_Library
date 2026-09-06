#!/usr/bin/env python3
"""
Combine income from different sources to make 'total' household income.
"""

from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import pandas as pd
import numpy as np

income_sources = [('earnings',['Earnings']),
                  ('enterprise_income',['profits'])]

income = None
for source,cols in income_sources:
    per_source = get_dataframe(f'../var/{source}.parquet')[cols].sum(axis=1)
    # Collapse duplicate (t, i) rows: source tables carry per-activity rows
    # (multiple enterprises or labor activities per household).  Income is
    # the HH-level total, so sum across activities before combining sources.
    # Without this collapse, `income + income` does a pandas-cartesian on
    # duplicate indices and multiplies duplicates.
    per_source = per_source.groupby(level=list(per_source.index.names)).sum()
    # A household absent from one source earned nothing FROM THAT SOURCE; it
    # does not have undefined total income.  A plain `+` aligns on the
    # intersection and yields NaN -- dropped below -- for every household that
    # is not in every source.  That defect was invisible until GH #772: the
    # earnings table used to carry a row for every (household, wave) pair in
    # the panel, NaN outside the wave the household was actually surveyed in,
    # which `.sum(axis=1)` turned into 0.0 -- so the intersection was very
    # nearly the union.  With those 106,713 phantom rows gone, a plain `+`
    # would silently drop the enterprise-only households they had been
    # standing in for.  All-zero totals are still dropped, below.
    income = per_source if income is None else income.add(per_source, fill_value=0)

income = income.replace(0,np.nan).dropna()

to_parquet(pd.DataFrame({'income':income}), '../var/income.parquet')
