"""Concatenate wave-level plot_labor data for Uganda (GAP 3).

Each wave's ``Uganda/<wave>/_/plot_labor.py`` produces a parquet indexed
``(t, i, plot, source, season)`` with the REPORTED plot-labor columns
(PersonDays, Wage).  This script concatenates them across waves and applies
cross-wave id_walk so the household index uses the panel canonical id scheme.

Source: AGSEC3A (season 1) + AGSEC3B (season 2) plot-input modules -- the
labor block the WB code (UGA_UNPS1.do:467-509) reads only to build the
per-parcel total_labor_days / total_family_labor_days / total_hired_labor_days
SUM columns and the median-wage hired_labor_value.  We keep the PRE-collapse
REPORTED person-days; those sums and the wage valuation are transformations,
not stored here.

2005-06 is intentionally absent: it has no plot-input/labor module (the UNPS
agriculture panel starts at wave 1 = 2009-10).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_labor.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for plot_labor (no .py / parquet, e.g. 2005-06).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_labor: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_labor.parquet')
