#!/usr/bin/env python
"""Timor-Leste 2007-08 months_food_inadequate (Family C).

Source: Section 13 vulnerability block carried in the household file
(../Data/hhold.dta, one row per hh_id, 4477 HHs).  The 2007-08 questionnaire
keeps the 2001 S13C 'Vulnerability' module as q13b*; the equivalent of the
2001 s13c13 months count is

  q13b13  "Months of rice/maize shortage"

an integer 0-12 count of months in the past 12 the household could not meet
its staple food needs.  Exposed as ``MonthsInadequate`` (Int64 0-12) and
``AnyInadequate`` (MonthsInadequate > 0).

The companion items (q13b01..q13b12 monthly food-consumption level, q13b14*
members affected, q13b15* coping actions) are not emitted -- the (t, i)
months count is the canonical Family-C target.

``i`` matches the roster's ``i`` (hh_id via format_id).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

T = '2007-08'

df = get_dataframe('../Data/hhold.dta')[['hh_id', 'q13b13']].copy()
df['i'] = df['hh_id'].apply(format_id)

m = pd.to_numeric(df['q13b13'], errors='coerce').round()
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
