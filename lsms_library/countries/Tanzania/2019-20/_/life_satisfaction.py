#!/usr/bin/env python
"""Tanzania 2019-20 life_satisfaction -- domain-satisfaction battery (SECTION G).

Source: ``HH_SEC_G.dta`` ("Subjective Welfare").  The module is asked of every
household member aged 15+, so it is INDIVIDUAL-level (~5.6k rows, one
respondent block per HH member).  The canonical ``life_satisfaction`` feature
is HOUSEHOLD-level (index ``(t, i, Domain)``), so we REDUCE each household to
its HEAD: the head is identified from the roster (``HH_SEC_B`` ``hh_b05 ==
'HEAD'``), which has exactly one head per household, and ~100% of those heads
are present in this module.  The head's row is selected by joining on
``(sdd_hhid, sdd_indid)``.

``i = sdd_hhid`` (matches the wave's household_roster ``i``).

WIDE -> LONG: the 9 battery items ``hh_g03_1 .. hh_g03_9`` each become one
``(t, i, Domain)`` row.  Domain mapping from the .dta variable labels:

    hh_g03_1  YOUR HEALTH                      -> Health
    hh_g03_2  YOUR FINANCIAL SITUATION         -> Finances
    hh_g03_3  YOUR HOUSING                     -> Housing
    hh_g03_4  YOUR JOB                         -> Job
    hh_g03_5  THE HEALTH CARE AVAILABLE        -> HealthCareAccess
    hh_g03_6  THE EDUCATION AVAILABLE          -> Education
    hh_g03_7  YOUR PROTECTION AGAINST CRIME    -> Safety
    hh_g03_8  YOUR SAFETY IN TRANSPORT         -> TransportSafety
    hh_g03_9  YOUR LIFE AS A WHOLE             -> Overall

Items 5 and 8 (health-care access, transport safety) have no canonical Domain
in the shared list; they are kept under descriptive non-canonical names rather
than discarded (harmonize the interface, not the data).

``Satisfaction`` carries the native 7-point ordinal label, normalized from the
ALL-CAPS source to Title Case.  The "NOT APPLICABLE" sentinel is nulled (it is
not a satisfaction level).  ``t`` is added here ('2019-20'); the country-level
aggregator concatenates waves and applies id_walk.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

# hh_g03_N suffix -> canonical (or descriptive) Domain.
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

# Native ordinal labels (ALL-CAPS in source) -> human-readable Title Case.
# "NOT APPLICABLE" is not a rating -> dropped (NA).
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
roster = get_dataframe('../Data/HH_SEC_B.dta', convert_categoricals=True)
heads = roster.loc[
    roster['hh_b05'].astype(str).str.upper() == 'HEAD', ['sdd_hhid', 'sdd_indid']
].copy()

# --- reduce the individual-level module to the head's row -----------------
sat = get_dataframe('../Data/HH_SEC_G.dta', convert_categoricals=True)
head_rows = heads.merge(sat, on=['sdd_hhid', 'sdd_indid'], how='inner')

item_cols = [f'hh_g03_{n}' for n in DOMAINS]
wide = head_rows[['sdd_hhid'] + item_cols].rename(columns={'sdd_hhid': 'i'})
wide['i'] = wide['i'].astype(str)

# --- WIDE -> LONG on (i, Domain) -----------------------------------------
long = wide.melt(id_vars='i', value_vars=item_cols,
                 var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].str.removeprefix('hh_g03_').astype(int).map(DOMAINS)
long['Satisfaction'] = (
    long['Satisfaction'].astype(str).str.upper().map(SATISFACTION)
)

# Drop rows with no genuine rating (NA / "not applicable" / unanswered).
long = long.dropna(subset=['Satisfaction'])

long['t'] = '2019-20'
out = long[['t', 'i', 'Domain', 'Satisfaction']].set_index(['t', 'i', 'Domain'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# What makes (t, i, Domain) a key is the head reduction, so it was checked
# directly: HH_SEC_B.dta has EXACTLY ONE hh_b05=='HEAD' row per household
# (1,183 heads over 1,183 households, 0 with more than one; one of the wave's
# 1,184 households records no head and so contributes nothing here), and
# HH_SEC_G.dta is (sdd_hhid, sdd_indid)-unique, so the join cannot fan out.
# .first() is never called on a cold build.
if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'life_satisfaction.parquet')
