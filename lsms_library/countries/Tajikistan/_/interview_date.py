#!/usr/bin/env python
"""Concatenate wave-level interview_date data for Tajikistan.

Each wave's _/interview_date.py assembles an Int_t (datetime) column indexed
by (t, i).  This country-level script runs each wave script (so the wave
parquets exist on a cold build), reads them, and concatenates.  `v` is joined
from sample() by the framework at API time, yielding the canonical (t, v, i).
"""
import os
import subprocess
import sys

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['1999', '2003', '2007', '2009']

X = []
for t in WAVES:
    script = f'../{t}/_/interview_date.py'
    parquet = f'../{t}/_/interview_date.parquet'
    # Build the wave parquet if it is not already present.
    try:
        df = get_dataframe(parquet)
    except FileNotFoundError:
        subprocess.run([sys.executable or 'python3', 'interview_date.py'],
                       cwd=os.path.dirname(script), check=True)
        df = get_dataframe(parquet)
    X.append(df)

x = pd.concat(X, axis=0)

to_parquet(x, '../var/interview_date.parquet')
