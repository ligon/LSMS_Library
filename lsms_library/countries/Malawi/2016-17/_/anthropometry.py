"""Build anthropometry for Malawi IHS4 / IHPS 2016-17 (GAP 5).

Item-level (t, i, pid) body-measurement feature.  Sources mirror this
wave's household_roster, which concatenates a Cross_Sectional and a Panel
half:
  * Cross_Sectional/hh_mod_v + hh_mod_b -- keyed i: case_id (with the
    roster's cs_i mapping -> 'cs-17-'+case_id), pid: pid.
  * Panel/hh_mod_v_16 + hh_mod_b_16 -- keyed i: y3_hhid, pid: id_code.
Weight = hh_v08 (kg), Height = hh_v09 (cm); no MUAC in Module V.
Age_months (cage logic) and Sex (hh_b03) come from the matching roster
half via _anthropometry_block's join.

The 'cs-17-' prefix reproduces the roster feature's cs_i so the
Cross_Sectional i aligns with household_roster (verified 2026-06-14: the
roster keeps 53885 cs-17- rows + 12266 panel rows).  The framework applies
panel id_walk + joins v at API time.

This is the SAME Module V the World Bank cleaning code reads then collapses
to WHO-2006 z-scores (MWI_IHPS3.do anthropometry block); we keep the RAW
measures.  See lsms_library/countries/Malawi/_/malawi.py:_anthropometry_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _anthropometry_block, assemble_anthropometry


WAVE = '2016-17'

# Cross_Sectional half (cs_i -> 'cs-17-' prefix).
cs_health = get_dataframe('../Data/Cross_Sectional/hh_mod_v.dta')
cs_roster = get_dataframe('../Data/Cross_Sectional/hh_mod_b.dta')
cs_piece = _anthropometry_block(
    cs_health, cs_roster, t=WAVE,
    hh_id_health='case_id', pid_health='pid',
    hh_id_roster='case_id', pid_roster='pid',
    i_prefix='cs-17-',
)

# Panel half (raw y3_hhid).
pn_health = get_dataframe('../Data/Panel/hh_mod_v_16.dta')
pn_roster = get_dataframe('../Data/Panel/hh_mod_b_16.dta')
pn_piece = _anthropometry_block(
    pn_health, pn_roster, t=WAVE,
    hh_id_health='y3_hhid', pid_health='id_code',
    hh_id_roster='y3_hhid', pid_roster='id_code',
)

df = assemble_anthropometry(WAVE, [cs_piece, pn_piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in anthropometry {WAVE}"
assert len(df) > 0, f"anthropometry {WAVE} produced no rows"

to_parquet(df, 'anthropometry.parquet')
