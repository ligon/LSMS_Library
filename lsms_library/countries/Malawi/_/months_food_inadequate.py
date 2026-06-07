"""Concatenate wave-level months_food_inadequate parquets for Malawi (#332).

Each buildable wave's ``Malawi/<wave>/_/months_food_inadequate.py``
produces a parquet indexed (t, i) with ``MonthsInadequate`` (0-12) and
``AnyInadequate`` (bool).  This script concatenates them.  Cross-wave
id_walk and the join of ``v`` are applied by the framework at API time in
_finalize_result -- as for plot_features, no id_walk is run here.

2004-05 (IHS2) is ABSENT: its Module H is a 3-day food-consumption recall,
not the H04/H05 months-of-shortage battery introduced in IHS3.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/months_food_inadequate.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        continue
    pieces.append(df)

assert pieces, "months_food_inadequate: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/months_food_inadequate.parquet')
