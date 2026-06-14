"""Build people_last7days for Malawi IHS4 / IHPS 2016-17 (GAP 3, individual).

Item-level (t, i, pid) 7-day individual time-use feature.  IHS4 ships a
Cross_Sectional half (cs-17-prefixed case_id, pid: pid) and a Panel half
(y3_hhid, pid: id_code), concatenated into the single 2016-17 wave --
exactly like household_roster / anthropometry.  Source: hh_mod_e (labor
module) per half, joined to hh_mod_b (roster) for working_age.

Module E layout is the IHS4/IHS5 (modern) generation: farm = hh_e07a (+
livestock hh_e07b, fishing hh_e07c), SOB = hh_e08 (+ hh_e09), wage = hh_e11
(+ ganyu hh_e10); industry hh_e20b.  IHS4/IHS5 drop the within-module
working-age marker, so working_age = (roster age hh_b05a >= 5), reproducing
the WB MWI_IHPS3.do:1315 definition, joined from the roster half.

This is the SAME Module E the WB code (MWI_IHPS3.do:1294-1343) reads; we
keep the per-individual reported record (industry as one harmonize_industry
label instead of six dummies).  See
lsms_library/countries/Malawi/_/malawi.py:_people_last7days_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _people_last7days_block, assemble_people_last7days


WAVE = '2016-17'

pieces = []

# --- Cross_Sectional half (cs-17 prefix; pid: pid) ---
e_xs = get_dataframe('../Data/Cross_Sectional/hh_mod_e.dta',
                     convert_categoricals=False)
b_xs = get_dataframe('../Data/Cross_Sectional/hh_mod_b.dta',
                     convert_categoricals=False)
pieces.append(_people_last7days_block(
    e_xs, t=WAVE, hhid='case_id', pid='pid', layout='modern',
    i_prefix='cs-17-', roster=b_xs, roster_hhid='case_id', roster_pid='pid',
))

# --- Panel half (bare y3_hhid; pid: id_code) ---
e_pn = get_dataframe('../Data/Panel/hh_mod_e_16.dta', convert_categoricals=False)
b_pn = get_dataframe('../Data/Panel/hh_mod_b_16.dta', convert_categoricals=False)
pieces.append(_people_last7days_block(
    e_pn, t=WAVE, hhid='y3_hhid', pid='id_code', layout='modern',
    roster=b_pn, roster_hhid='y3_hhid', roster_pid='id_code',
))

df = assemble_people_last7days(WAVE, pieces)

assert df.index.is_unique, f"Non-unique (t,i,pid) in people_last7days {WAVE}"
assert len(df) > 0, f"people_last7days {WAVE} produced no rows"

to_parquet(df, 'people_last7days.parquet')
