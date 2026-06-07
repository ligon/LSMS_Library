#!/usr/bin/env python
"""Tanzania 2020-21 life_satisfaction -- domain-satisfaction battery (SECTION G).

Source: ``hh_sec_g.dta`` ("Subjective Welfare").  Individual-level (asked of
every member aged 15+, ~23.6k rows); the canonical feature is HOUSEHOLD-level
(index ``(t, i, Domain)``), so we REDUCE each household to its HEAD (roster
``hh_sec_b`` ``hh_b05 == 'HEAD'``) by joining on ``(y5_hhid, indidy5)``.

``i = y5_hhid`` (matches the wave's household_roster ``i``).

WIDE -> LONG: the 9 items ``hh_g03_1 .. hh_g03_9`` map to the same Domains as
2019-20 (identical questionnaire):

    hh_g03_1 Health  hh_g03_2 Finances  hh_g03_3 Housing  hh_g03_4 Job
    hh_g03_5 HealthCareAccess  hh_g03_6 Education  hh_g03_7 Safety
    hh_g03_8 TransportSafety   hh_g03_9 Overall

The 2020-21 value labels are MIXED CASE in source ("satisfied"/"dissatisfied"
lower-case for codes 2/6); ``.str.upper()`` normalizes before mapping so the
output ``Satisfaction`` labels match the other waves' Title Case.  "NOT
APPLICABLE" is nulled.  ``t`` = '2020-21'.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

DOMAINS = {
    1: 'Health',
    2: 'Finances',
    3: 'Housing',
    4: 'Job',
    5: 'HealthCareAccess',
    6: 'Education',
    7: 'Safety',
    8: 'TransportSafety',
    9: 'Overall',
}

SATISFACTION = {
    'VERY SATISFIED': 'Very satisfied',
    'SATISFIED': 'Satisfied',
    'SOMEWHAT SATISFIED': 'Somewhat satisfied',
    'NEITHER SATISFIED NOR DISSATISFIED': 'Neither satisfied nor dissatisfied',
    'SOMEWHAT DISSATISFIED': 'Somewhat dissatisfied',
    'DISSATISFIED': 'Dissatisfied',
    'VERY DISSATISFIED': 'Very dissatisfied',
}

# --- identify the household head's indid from the roster ------------------
roster = get_dataframe('../Data/hh_sec_b.dta', convert_categoricals=True)
heads = roster.loc[
    roster['hh_b05'].astype(str).str.upper() == 'HEAD', ['y5_hhid', 'indidy5']
].copy()

# --- reduce the individual-level module to the head's row -----------------
sat = get_dataframe('../Data/hh_sec_g.dta', convert_categoricals=True)
head_rows = heads.merge(sat, on=['y5_hhid', 'indidy5'], how='inner')

item_cols = [f'hh_g03_{n}' for n in DOMAINS]
wide = head_rows[['y5_hhid'] + item_cols].rename(columns={'y5_hhid': 'i'})
wide['i'] = wide['i'].astype(str)

# --- WIDE -> LONG on (i, Domain) -----------------------------------------
long = wide.melt(id_vars='i', value_vars=item_cols,
                 var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].str.removeprefix('hh_g03_').astype(int).map(DOMAINS)
long['Satisfaction'] = (
    long['Satisfaction'].astype(str).str.upper().map(SATISFACTION)
)

long = long.dropna(subset=['Satisfaction'])

long['t'] = '2020-21'
out = long[['t', 'i', 'Domain', 'Satisfaction']].set_index(['t', 'i', 'Domain'])

if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'life_satisfaction.parquet')
