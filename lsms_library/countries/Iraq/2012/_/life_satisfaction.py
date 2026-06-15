#!/usr/bin/env python3
"""Iraq 2012 life_satisfaction -- domain-satisfaction battery (SECTION 23).

Source: ``2012ihses23_life_satisfaction.dta``.  The module is asked of EVERY
household member aged 15+, so it is INDIVIDUAL-level: ~176k rows, ~7 per
household.  The canonical ``life_satisfaction`` feature is HOUSEHOLD-level
(index ``(t, i, Domain)``), so we REDUCE each household to its HEAD: the head
is identified from the roster (``2012ihses01`` ``q0105 == 'Head'``), which has
exactly one head per household, and 100% of those heads are present in this
module.  The head's row is selected on ``(questid, idcode)`` (unique here).

``i = questid`` (the sample/housing/assets key).  The roster's per-row ``hh``
field is the within-dwelling sequence number, not a household key (GH #256),
so we join the head's ``idcode`` from the roster on ``questid``.

WIDE -> LONG: the 11 battery items ``q2302_1 .. q2302_11`` each become one
``(t, i, Domain)`` row.  The questionnaire (HIES-II Part 3, Section 23) fixes
the item order (the .dta variable labels are Stata-truncated and partly
misaligned; the questionnaire is authoritative):

    q2302_1  Food                              -> Food
    q2302_2  Housing                           -> Housing
    q2302_3  ... income?                       -> Finances
    q2302_4  Health                            -> Health
    q2302_5  Work                              -> Job
    q2302_6  Local security level              -> Safety
    q2302_7  Education                         -> Education
    q2302_8  The freedom of choice ... in life -> FreedomOfChoice
    q2302_9  The control you have over life    -> Control
    q2302_10 Trust/acceptance in community     -> Community
    q2302_11 Life overall                      -> Overall

Items 8 (freedom of choice) and 9 (control over life) have no canonical
Domain in the shared list; they are autonomy items kept under descriptive
non-canonical Domain names rather than discarded (harmonize the interface,
not the data).

``Satisfaction`` carries the native ordinal label, normalized from the
ALL-CAPS source to Title Case.  The non-rating sentinel "DO NOT KNOW/NO
ANSWER" is nulled (it is not a satisfaction level).  ``t`` is added by the
framework when it concatenates waves; this script writes the
``(i, Domain)`` frame only.

2006-07 (IHSES-I) has NO life-satisfaction module, so only 2012 is wired.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

# q2302_N column suffix -> canonical (or descriptive) Domain.
DOMAINS = {
    1: 'Food',
    2: 'Housing',
    3: 'Finances',
    4: 'Health',
    5: 'Job',
    6: 'Safety',
    7: 'Education',
    8: 'FreedomOfChoice',
    9: 'Control',
    10: 'Community',
    11: 'Overall',
}

# Native ordinal labels (ALL-CAPS in source) -> human-readable Title Case.
# "DO NOT KNOW/NO ANSWER" is a non-response, not a rating -> dropped (NA).
SATISFACTION = {
    'VERY SATISFIED': 'Very satisfied',
    'FAIRLY SATISFIED': 'Fairly satisfied',
    'NOT VERY SATISFIED': 'Not very satisfied',
    'NOT AT ALL SATISFIED': 'Not at all satisfied',
}

# --- identify the household head's idcode from the roster -----------------
roster = get_dataframe('../Data/2012ihses01_household_roster.dta')
heads = roster.loc[
    roster['q0105'].astype(str) == 'Head', ['questid', 'idcode']
].copy()

# --- reduce the individual-level module to the head's row -----------------
sat = get_dataframe('../Data/2012ihses23_life_satisfaction.dta')
head_rows = heads.merge(sat, on=['questid', 'idcode'], how='left')

item_cols = [f'q2302_{n}' for n in DOMAINS]
wide = head_rows[['questid'] + item_cols].rename(columns={'questid': 'i'})
wide['i'] = wide['i'].astype(str)

# --- WIDE -> LONG on (i, Domain) -----------------------------------------
long = wide.melt(id_vars='i', value_vars=item_cols,
                 var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].str.removeprefix('q2302_').astype(int).map(DOMAINS)
long['Satisfaction'] = long['Satisfaction'].astype(str).map(SATISFACTION)

# Drop rows with no genuine rating (NA / "do not know" / item not answered).
long = long.dropna(subset=['Satisfaction'])

out = long[['i', 'Domain', 'Satisfaction']].set_index(['i', 'Domain'])

to_parquet(out, 'life_satisfaction.parquet')
