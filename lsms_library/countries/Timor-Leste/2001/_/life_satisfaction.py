#!/usr/bin/env python
"""Timor-Leste 2001 life_satisfaction.

Source: Section 13 'Subjective wellbeing'.

  * ../Data/S13A.DTA -- INDIVIDUAL level (one row per adult respondent,
    ~2-3 rows per household).  The only genuine overall-satisfaction RATING
    item is ``s13a11`` ("11 satisfied with your life in general"), a 5-point
    ordinal {very satisfied / rather satisfied / neither satisfied nor
    unsatisfied / somewhat unsatisfied / very unsatisfied}.  Because the
    canonical life_satisfaction table is HOUSEHOLD level, we reduce S13A to
    the household HEAD: the head's ``idperson`` is taken from the roster
    (S01A1, s01a03 == 'head', 1 head per HH; merge on identif+idperson hits
    all 1800 households).  -> Domain='Overall'.

    The other S13A columns are NOT satisfaction ratings and are excluded:
      - s13a01            retrospective "lived better/same/worse 2y ago"
      - s13a02a..s13a05b  pick-lists of what improved/worsened/priorities
      - s13a06            corruption perception (more/as much/less)
      - s13a07..s13a10    Cantril ladder steps (richness / rights, now vs
                          2y ago) -- these belong to the separate
                          subjective_well_being (welfare-ladder) construct.

  * ../Data/S13B.DTA -- HOUSEHOLD level (one row per identif, 1800 HHs).
    Items s13b01..s13b06 are self-evaluated adequacy ("autoevaluation")
    ratings on a 3-point ordinal {less than adequate / just adequate / more
    than adequate}.  These are domain satisfaction ratings:
      s13b01 food          -> Food
      s13b02 housing       -> Housing
      s13b03 clothing      -> Clothing
      s13b04 health care   -> Health
      s13b05 education     -> Education ('no children' nulled out)
      s13b06 total income  -> Finances
    s13b07/s13b08 (poverty-line amounts), s13b09 (religion) are excluded.

Output is LONG, index (t, i, Domain), single column Satisfaction holding the
survey's native ordinal label (no invented numeric scale).  ``i`` matches the
roster's ``i`` (identif).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

T = '2001'

# ---- S13A: overall life satisfaction, reduced to household head -----------
roster = get_dataframe('../Data/S01A1.DTA')[['identif', 'idperson', 's01a03']]
heads = roster[roster['s01a03'].astype(str) == 'head'][['identif', 'idperson']]

a = get_dataframe('../Data/S13A.DTA')[['identif', 'idperson', 's13a11']]
a = heads.merge(a, on=['identif', 'idperson'], how='left')
a['Satisfaction'] = a['s13a11'].astype(str).str.strip()
a = a[a['Satisfaction'].ne('nan')]
a['Domain'] = 'Overall'
a = a.rename(columns={'identif': 'i'})[['i', 'Domain', 'Satisfaction']]

# ---- S13B: household self-evaluated adequacy ------------------------------
B_DOMAINS = {
    's13b01': 'Food',
    's13b02': 'Housing',
    's13b03': 'Clothing',
    's13b04': 'Health',
    's13b05': 'Education',
    's13b06': 'Finances',
}
ADEQUACY = {'less than adequate', 'just adequate', 'more than adequate'}

b = get_dataframe('../Data/S13B.DTA')[['identif'] + list(B_DOMAINS)].copy()
b = b.rename(columns={'identif': 'i'})
long_b = b.melt(id_vars='i', var_name='var', value_name='Satisfaction')
long_b['Domain'] = long_b['var'].map(B_DOMAINS)
long_b['Satisfaction'] = long_b['Satisfaction'].astype(str).str.strip()
# keep only substantive adequacy responses ('no children'/missing dropped)
long_b = long_b[long_b['Satisfaction'].isin(ADEQUACY)]
long_b = long_b[['i', 'Domain', 'Satisfaction']]

out = pd.concat([a, long_b], axis=0, ignore_index=True)
out['t'] = T
out = (out[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
