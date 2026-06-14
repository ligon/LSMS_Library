"""Build people_last7days for Malawi IHPS 2013-14 (GAP 3, individual grain).

Item-level (t, i, pid) 7-day individual time-use feature.  Source (flat
Data/):
  * HH_MOD_E_13 -- the household labor module.  IHS3/IHPS-2013 (legacy)
    layout: farm = hh_e07, SOB = hh_e08, wage = hh_e11 (+ ganyu hh_e10);
    working_age = (hh_e02 != "X"); industry hh_e20b.

ID NOTE: household_roster for 2013-14 uses pid: hh_b01 (the within-HH line
number 1,2,3...).  The labor module carries the same line number as hh_e01
(verified 2026-06-14: all 20219 (y2_hhid, hh_e01) keys are in the roster
and unique), so we key on hh_e01 -> pid to align with household_roster.
i = format_id(y2_hhid).  The framework applies panel id_walk + joins v at
API time.  This is the SAME Module E the WB code (MWI_IHPS2.do labor block)
reads.  See lsms_library/countries/Malawi/_/malawi.py:_people_last7days_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _people_last7days_block, assemble_people_last7days


WAVE = '2013-14'

labor = get_dataframe('../Data/HH_MOD_E_13.dta', convert_categoricals=False)

piece = _people_last7days_block(
    labor, t=WAVE, hhid='y2_hhid', pid='hh_e01', layout='legacy',
)

df = assemble_people_last7days(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in people_last7days {WAVE}"
assert len(df) > 0, f"people_last7days {WAVE} produced no rows"

to_parquet(df, 'people_last7days.parquet')
