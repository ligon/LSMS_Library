"""Build people_last7days for Malawi IHS3 2010-11 (GAP 3, individual grain).

Item-level (t, i, pid) 7-day individual time-use feature, mirroring
Uganda's people_last7days at the per-individual grain GAP_RANKING.org
specifies.  Source (Full_Sample/Household):
  * hh_mod_e -- the household labor module.  IHS3 (legacy) layout: farm =
    hh_e07, SOB = hh_e08, wage = hh_e11 (+ ganyu hh_e10); working_age =
    (hh_e02 != "X"); industry hh_e20b.

i = format_id(case_id), pid = format_id(id_code) -- the SAME (i, pid)
household_roster emits for this wave.  This is the SAME Module E the World
Bank cleaning code (MWI_IHPS1.do:1234-1269) reads to build the farm_work /
SOB_work / wage_work dummies, hours, and ind_* industry one-hots; we keep
the per-individual reported record (industry as one harmonize_industry
label instead of six dummies).  See
lsms_library/countries/Malawi/_/malawi.py:_people_last7days_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _people_last7days_block, assemble_people_last7days


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Household/'

labor = get_dataframe(BASE + 'hh_mod_e.dta', convert_categoricals=False)

piece = _people_last7days_block(
    labor, t=WAVE, hhid='case_id', pid='id_code', layout='legacy',
)

df = assemble_people_last7days(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in people_last7days {WAVE}"
assert len(df) > 0, f"people_last7days {WAVE} produced no rows"

to_parquet(df, 'people_last7days.parquet')
