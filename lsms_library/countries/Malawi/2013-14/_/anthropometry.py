"""Build anthropometry for Malawi IHPS 2013-14 (GAP 5).

Item-level (t, i, pid) body-measurement feature.  Source:
  * HH_MOD_V_13 -- Module V anthropometry, one row per (HH, person).
    Weight = hh_v08 (kg), Height = hh_v09 (cm); no MUAC in this module.
    Keyed on (y2_hhid, PID), where PID is the long string id ('000101').
  * HH_MOD_B_13 -- Module B roster, for Age_months (cage logic) and Sex.

ID NOTE: household_roster for 2013-14 uses pid: hh_b01 (the within-HH
person line number 1,2,3...), NOT the long-string PID.  So pid =
format_id(hh_b01) = '1','2',...  Module V carries only PID, so we first
remap (y2_hhid, PID) -> hh_b01 via the roster (a unique 1:1 map, verified
2026-06-14: all 2534 measured rows match), then key anthropometry on
hh_b01 so its (i, pid) aligns with household_roster.  i = format_id
(y2_hhid).  The framework applies panel id_walk + joins v at API time.

This is the SAME Module V the World Bank cleaning code reads then collapses
to WHO-2006 z-scores (MWI_IHPS2.do anthropometry block); we keep the RAW
measures.  See lsms_library/countries/Malawi/_/malawi.py:_anthropometry_block.
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _anthropometry_block, assemble_anthropometry


WAVE = '2013-14'
BASE = '../Data/'

health = get_dataframe(BASE + 'HH_MOD_V_13.dta')
roster = get_dataframe(BASE + 'HH_MOD_B_13.dta')

# Remap the health module's long-string PID to the roster's hh_b01 line
# number, so the emitted pid matches household_roster.  (y2_hhid, PID) is a
# unique key in the roster.
key = roster[['y2_hhid', 'PID', 'hh_b01']].drop_duplicates(['y2_hhid', 'PID'])
health = health.merge(key, on=['y2_hhid', 'PID'], how='left')

piece = _anthropometry_block(
    health, roster, t=WAVE,
    hh_id_health='y2_hhid', pid_health='hh_b01',
    hh_id_roster='y2_hhid', pid_roster='hh_b01',
)

df = assemble_anthropometry(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in anthropometry {WAVE}"
assert len(df) > 0, f"anthropometry {WAVE} produced no rows"

to_parquet(df, 'anthropometry.parquet')
