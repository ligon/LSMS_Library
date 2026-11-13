"""Build people_last7days for Malawi IHS5 / IHPS 2019-20 (GAP 3, individual).

Item-level (t, i, pid) 7-day individual time-use feature.  IHS5 ships a
Cross_Sectional half (cs-19-prefixed case_id, pid: PID) and a Panel half
(y4_hhid, pid: id_code), concatenated into the single 2019-20 wave -- exactly like
household_roster / anthropometry.  Source: HH_MOD_E (labor module) per half,
joined to HH_MOD_B (roster) for working_age.

ID NOTE: like anthropometry, the household_roster's Cross_Sectional i is
'cs-19-' + case_id (GH #1278), so the CS half passes that i_prefix and keys
pid on PID (the roster's CS pid).  The framework id_walk remaps the Panel
half to its IHS3 baseline id + joins v at API time.

Module E layout is the IHS4/IHS5 (modern) generation: farm = hh_e07a (+
hh_e07b, hh_e07c), SOB = hh_e08 (+ hh_e09), wage = hh_e11 (+ ganyu
hh_e10); industry hh_e20b; working_age = (roster hh_b05a >= 5).  This is
the SAME Module E the WB code (MWI_IHPS4.do:1314-1365) reads.  See
lsms_library/countries/Malawi/_/malawi.py:_people_last7days_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _people_last7days_block, assemble_people_last7days


WAVE = '2019-20'

pieces = []

# --- Cross_Sectional half (cs-19 prefix; pid: PID) ---
e_xs = get_dataframe('../Data/Cross_Sectional/HH_MOD_E.dta',
                     convert_categoricals=False)
b_xs = get_dataframe('../Data/Cross_Sectional/HH_MOD_B.dta',
                     convert_categoricals=False)
pieces.append(_people_last7days_block(
    e_xs, t=WAVE, hhid='case_id', pid='PID', layout='modern',
    i_prefix='cs-19-', roster=b_xs, roster_hhid='case_id', roster_pid='PID',
))

# --- Panel half (y4_hhid; pid: id_code) ---
e_pn = get_dataframe('../Data/Panel/hh_mod_e_19.dta', convert_categoricals=False)
b_pn = get_dataframe('../Data/Panel/hh_mod_b_19.dta', convert_categoricals=False)
pieces.append(_people_last7days_block(
    e_pn, t=WAVE, hhid='y4_hhid', pid='id_code', layout='modern',
    roster=b_pn, roster_hhid='y4_hhid', roster_pid='id_code',
))

df = assemble_people_last7days(WAVE, pieces)

assert df.index.is_unique, f"Non-unique (t,i,pid) in people_last7days {WAVE}"
assert len(df) > 0, f"people_last7days {WAVE} produced no rows"

to_parquet(df, 'people_last7days.parquet')
