#!/usr/bin/env python
"""Concatenate household_characteristics across Nepal waves."""
import sys
import warnings
import pandas as pd
from lsms_library.local_tools import to_parquet, get_dataframe
from nepal import Waves

x = {}

for t in Waves.keys():
    print(t, file=sys.stderr)
    try:
        x[t] = get_dataframe('../' + t + '/_/household_characteristics.parquet')
        x[t] = x[t].stack('k').dropna()
        x[t] = x[t].reset_index().set_index(['i', 'k']).squeeze()
    except FileNotFoundError:
        warnings.warn(f'No household_characteristics.parquet for {t}')

z = pd.DataFrame(x)
z.columns.name = 't'

z = z.stack().unstack('k')

z['m'] = 'Nepal'
z = z.reset_index().set_index(['i', 't', 'm'])

to_parquet(z, '../var/household_characteristics.parquet')
