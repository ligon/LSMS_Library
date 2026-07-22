#!/usr/bin/env python
"""Tanzania 2008-15 life_satisfaction -- domain-satisfaction battery (SECTION G).

Source file: ``upd4_hh_g.dta`` -- the multi-round "Subjective Welfare" module
covering NPS rounds 1-4 (waves 2008-09, 2010-11, 2012-13, 2014-15), stacked
with a ``round`` column.  As with the other multi-round Tanzania scripts this
writes ONE parquet carrying all rounds with ``t`` in the index; Wave.grab_data
filters to the requested sub-wave.

The module is INDIVIDUAL-level (asked of every member aged 15+, ~83.7k rows).
The canonical feature is HOUSEHOLD-level (index ``(t, i, Domain)``), so each
household is REDUCED to its HEAD: heads come from the roster ``upd4_hh_b.dta``
(``hb_05 == 'HEAD'``, exactly one per (round, r_hhid)); the head's row is
selected by joining on ``(round, r_hhid, r_id)`` (16540 heads match 1:1 across
all four rounds).

``i = r_hhid`` (matches the wave's household_roster ``i``); ``t`` is the round
mapped to its wave label.

WIDE -> LONG: the 9 items ``hg_03_1 .. hg_03_9``.  NB the item ORDER differs
from the 2019-20/2020-21 questionnaire: here the overall-life item is _8 and
there is a spouse item (_9) instead of transport/health-care-access items.
Domain mapping from the .dta variable labels:

    hg_03_1  YOUR HEALTH                 -> Health
    hg_03_2  YOUR FINANCIAL SITUATION    -> Finances
    hg_03_3  YOUR HOUSING                -> Housing
    hg_03_4  YOUR JOB                    -> Job
    hg_03_5  THE HEALTH CARE AVAILABLE   -> HealthCareAccess
    hg_03_6  THE EDUCATION AVAILABLE     -> Education
    hg_03_7  YOUR PROTECTION AGAINST ... -> Safety
    hg_03_8  YOUR LIFE AS A WHOLE        -> Overall
    hg_03_9  YOUR HUSBAND/WIFE           -> Spouse

HealthCareAccess and Spouse have no canonical Domain in the shared list; they
are kept under descriptive non-canonical names rather than discarded.

``Satisfaction`` carries the native 7-point ordinal label, Title-cased.  "NOT
APPLICABLE" is nulled.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

# hg_03_N suffix -> canonical (or descriptive) Domain.  Note the _8/_9
# ordering departs from the later waves (see module docstring).
DOMAINS = {
    1: 'Health',
    2: 'Finances',
    3: 'Housing',
    4: 'Job',
    5: 'HealthCareAccess',
    6: 'Education',
    7: 'Safety',
    8: 'Overall',
    9: 'Spouse',
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

# --- identify the household head per (round, r_hhid) from the roster ------
roster = get_dataframe('../Data/upd4_hh_b.dta', convert_categoricals=True)
heads = roster.loc[
    roster['hb_05'].astype(str).str.upper() == 'HEAD',
    ['round', 'r_hhid', 'r_id'],
].copy()
heads['round'] = heads['round'].astype(float)
heads['r_id'] = heads['r_id'].astype(float)
heads['r_hhid'] = heads['r_hhid'].astype(str)

# --- reduce the individual-level module to the head's row -----------------
sat = get_dataframe('../Data/upd4_hh_g.dta', convert_categoricals=True)
sat['round'] = sat['round'].astype(float)
sat['r_id'] = sat['r_id'].astype(float)
sat['r_hhid'] = sat['r_hhid'].astype(str)

head_rows = heads.merge(sat, on=['round', 'r_hhid', 'r_id'], how='inner')

item_cols = [f'hg_03_{n}' for n in DOMAINS]
wide = head_rows[['round', 'r_hhid'] + item_cols].rename(columns={'r_hhid': 'i'})
wide['t'] = wide['round'].map(round_match)
wide = wide.drop(columns=['round'])

# --- WIDE -> LONG on (t, i, Domain) --------------------------------------
long = wide.melt(id_vars=['t', 'i'], value_vars=item_cols,
                 var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].str.removeprefix('hg_03_').astype(int).map(DOMAINS)
long['Satisfaction'] = (
    long['Satisfaction'].astype(str).str.upper().map(SATISFACTION)
)

long = long.dropna(subset=['Satisfaction'])

out = long[['t', 'i', 'Domain', 'Satisfaction']].set_index(['t', 'i', 'Domain'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# The head reduction above is what makes (t, i, Domain) a key, so it was
# checked directly rather than inferred: upd4_hh_b.dta carries EXACTLY ONE
# hb_05=='HEAD' row per (round, r_hhid) -- 16,540 head rows over 16,540
# household-rounds, 0 with more than one head -- and upd4_hh_g.dta is itself
# (round, r_hhid, UPI)-unique with no UPHI column, so the join cannot fan out.
# On a cold build .first() is never reached.
if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'life_satisfaction.parquet')
