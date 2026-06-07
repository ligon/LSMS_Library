#!/usr/bin/env python
"""Concatenate wave-level life_satisfaction data for Tajikistan.

Only the 2007 and 2009 waves carry a subjective life/financial satisfaction
module (Module 9a / 8a "Subjective Poverty and Food Security").  Each wave's
_/life_satisfaction.py emits a LONG-form (t, i, Domain) parquet with a single
Satisfaction column.  This country-level script runs each wave script (so the
wave parquets exist on a cold build), reads them, and concatenates.
"""
import os
import subprocess
import sys

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2007', '2009']

X = []
for t in WAVES:
    script = f'../{t}/_/life_satisfaction.py'
    parquet = f'../{t}/_/life_satisfaction.parquet'
    try:
        df = get_dataframe(parquet)
    except FileNotFoundError:
        subprocess.run([sys.executable or 'python3', 'life_satisfaction.py'],
                       cwd=os.path.dirname(script), check=True)
        df = get_dataframe(parquet)
    X.append(df)

x = pd.concat(X, axis=0)

to_parquet(x, '../var/life_satisfaction.parquet')
