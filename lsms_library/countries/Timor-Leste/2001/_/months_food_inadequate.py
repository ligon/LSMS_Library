#!/usr/bin/env python
"""Timor-Leste 2001 months_food_inadequate (Family C).

Source: Section 13C 'Vulnerability' (../Data/S13C.DTA, one row per identif,
1800 HHs).  The core item is

  s13c13  "13 months not enough rice/maize past 12m"

an integer count 0-12 of the months in the past 12 during which the household
could not meet its staple (rice/maize) food needs.  We expose it as
``MonthsInadequate`` (Int64 0-12) and the boolean ``AnyInadequate``
(MonthsInadequate > 0).

The companion S13C items (s13c01..s13c12 monthly food-consumption level
low/average/high, s13c14* members affected, s13c15* coping actions) are NOT
emitted here -- the (t, i) months count is the canonical Family-C target.

``i`` matches the roster's ``i`` (identif, e.g. '0011A1').
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

T = '2001'

df = get_dataframe('../Data/S13C.DTA')[['identif', 's13c13']].copy()
df = df.rename(columns={'identif': 'i'})

# clamp implausible values into the 0-12 range (one '13' would not appear here
# but guard anyway); keep NaN as missing.
m = pd.to_numeric(df['s13c13'], errors='coerce').round()
m = m.where(m.between(0, 12))
df['MonthsInadequate'] = m.astype('Int64')
df = df[df['MonthsInadequate'].notna()]
df['AnyInadequate'] = df['MonthsInadequate'] > 0

df['t'] = T
out = (df[['t', 'i', 'MonthsInadequate', 'AnyInadequate']]
       .drop_duplicates(subset=['t', 'i'])
       .set_index(['t', 'i'])
       .sort_index())

to_parquet(df=out, fn='months_food_inadequate.parquet')
