"""Build anthropometry for Malawi IHS3 2010-11 (GAP 5).

Item-level (t, i, pid) body-measurement feature.  Source
(Full_Sample/Household):
  * hh_mod_v -- Module V anthropometry, one row per (HH, person).
    Weight = hh_v08 (kg), Height = hh_v09 (cm); no MUAC in this module.
  * hh_mod_b -- Module B roster, for the child Age_months (cage logic:
    hh_b05a years x 12, else infant months hh_b05b) and Sex (hh_b03).

i = format_id(case_id), pid = format_id(id_code) -- the SAME (i, pid)
household_roster emits for this wave (its YAML uses i: case_id,
pid: id_code on the Full_Sample module-b file).  The framework applies
panel id_walk + joins v from sample() at API time.

This is the SAME Module V the World Bank cleaning code reads then collapses
to WHO-2006 z-scores (MWI_IHPS1.do:1213-1231); we keep the RAW measures.
See lsms_library/countries/Malawi/_/malawi.py:_anthropometry_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _anthropometry_block, assemble_anthropometry


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Household/'

health = get_dataframe(BASE + 'hh_mod_v.dta')
roster = get_dataframe(BASE + 'hh_mod_b.dta')

piece = _anthropometry_block(
    health, roster, t=WAVE,
    hh_id_health='case_id', pid_health='id_code',
    hh_id_roster='case_id', pid_roster='id_code',
)

df = assemble_anthropometry(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in anthropometry {WAVE}"
assert len(df) > 0, f"anthropometry {WAVE} produced no rows"

to_parquet(df, 'anthropometry.parquet')
