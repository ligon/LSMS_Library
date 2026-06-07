#!/usr/bin/env python
"""Timor-Leste 2007-08 life_satisfaction.

Source: Section 13 'Subjective wellbeing' carried in the household file
(../Data/hhold.dta, one row per hh_id, 4477 HHs).  The 2007-08 questionnaire
keeps the 2001 S13B self-evaluation ("autoevaluation") block but drops the
2001 S13A individual overall-life-satisfaction item -- there is NO overall
satisfaction question in this wave, so Domain='Overall' is absent here.

Genuine satisfaction (adequacy) ratings, 3-point ordinal
{Less than adequate / Just adequate / More than adequate}:
  q13a01 Food consumption   -> Food
  q13a02 Housing conditions -> Housing
  q13a03 Clothing           -> Clothing
  q13a04 Health care        -> Health
  q13a05 Children education  -> Education ('No children' nulled out)
  q13a06 Household total income -> Finances

Excluded (NOT satisfaction ratings):
  q13a07/q13a08 poverty-line amounts; q13a09 "better than 2001" retrospective
  comparison; q13a10 religion; q13b* monthly food-consumption level / rice
  shortage / coping actions (food-security content, not satisfaction).

Output is LONG, index (t, i, Domain), Satisfaction = native ordinal label.
``i`` matches the roster's ``i`` (hh_id as string).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

T = '2007-08'

DOMAINS = {
    'q13a01': 'Food',
    'q13a02': 'Housing',
    'q13a03': 'Clothing',
    'q13a04': 'Health',
    'q13a05': 'Education',
    'q13a06': 'Finances',
}
ADEQUACY = {'Less than adequate', 'Just adequate', 'More than adequate'}

df = get_dataframe('../Data/hhold.dta')[['hh_id'] + list(DOMAINS)].copy()
df['i'] = df['hh_id'].apply(format_id)
long = df.melt(id_vars='i', value_vars=list(DOMAINS),
               var_name='var', value_name='Satisfaction')
long['Domain'] = long['var'].map(DOMAINS)
long['Satisfaction'] = long['Satisfaction'].astype(str).str.strip()
long = long[long['Satisfaction'].isin(ADEQUACY)]
long['t'] = T

out = (long[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
