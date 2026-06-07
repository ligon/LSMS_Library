#!/usr/bin/env python
"""Concatenate wave-level plot_features for Tajikistan.

Only the 2007 round (TLSS round 1, module 12a) carries a usable per-plot
land roster (plot identity + per-plot area); the 1999 / 2003 / 2009 rounds
have no plot roster, so only 2007 is wired.  Each wave's _/plot_features.py
writes a parquet indexed by (t, i, plot_id) with the canonical columns; this
script builds them on a cold cache, reads them, and concatenates.  `v` is
joined from sample() by the framework at API time.
"""
import os
import subprocess
import sys

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2007']

X = []
for t in WAVES:
    script = f'../{t}/_/plot_features.py'
    parquet = f'../{t}/_/plot_features.parquet'
    # Build the wave parquet if it is not already present.
    try:
        df = get_dataframe(parquet)
    except FileNotFoundError:
        subprocess.run([sys.executable or 'python3', 'plot_features.py'],
                       cwd=os.path.dirname(script), check=True)
        df = get_dataframe(parquet)
    X.append(df)

x = pd.concat(X, axis=0)

assert x.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(x, '../var/plot_features.parquet')
