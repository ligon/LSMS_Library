"""Build anthropometry for Malawi IHS5 / IHPS 2019-20 (GAP 5).

Item-level (t, i, pid) body-measurement feature.  Sources mirror this
wave's household_roster, which concatenates a Cross_Sectional and a Panel
half:
  * Cross_Sectional/HH_MOD_V + HH_MOD_B -- keyed i: case_id, pid: PID.
  * Panel/hh_mod_v_19 + hh_mod_b_19 -- keyed i: y4_hhid, pid: id_code.
Weight = hh_v08 (kg), Height = hh_v09 (cm); no MUAC in Module V.
Age_months (cage logic) and Sex (hh_b03) come from the matching roster
half via _anthropometry_block's join.

ID NOTE: unlike 2016-17 (which keeps a 'cs-17-' prefix), the 2019-20
household_roster's final Cross_Sectional i is the RAW 12-digit case_id
(NO 'cs-19-' prefix), with the Panel y4_hhid walked to that same id space
by updated_ids (verified 2026-06-14: the roster has 0 cs-19- rows; all i
are plain 12-digit, some with id_walk's _N split suffix).  So we emit the
raw case_id for the CS half (no i_prefix) and let the framework's id_walk
remap the Panel half + join v at API time.

This is the SAME Module V the World Bank cleaning code reads then collapses
to WHO-2006 z-scores (MWI_IHPS4.do anthropometry block); we keep the RAW
measures.  See lsms_library/countries/Malawi/_/malawi.py:_anthropometry_block.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import _anthropometry_block, assemble_anthropometry


WAVE = '2019-20'

# Cross_Sectional half (raw case_id, no prefix -- see ID NOTE).
cs_health = get_dataframe('../Data/Cross_Sectional/HH_MOD_V.dta')
cs_roster = get_dataframe('../Data/Cross_Sectional/HH_MOD_B.dta')
cs_piece = _anthropometry_block(
    cs_health, cs_roster, t=WAVE,
    hh_id_health='case_id', pid_health='PID',
    hh_id_roster='case_id', pid_roster='PID',
)

# Panel half (raw y4_hhid; framework id_walk remaps to the case_id space).
pn_health = get_dataframe('../Data/Panel/hh_mod_v_19.dta')
pn_roster = get_dataframe('../Data/Panel/hh_mod_b_19.dta')
pn_piece = _anthropometry_block(
    pn_health, pn_roster, t=WAVE,
    hh_id_health='y4_hhid', pid_health='id_code',
    hh_id_roster='y4_hhid', pid_roster='id_code',
)

df = assemble_anthropometry(WAVE, [cs_piece, pn_piece])

assert df.index.is_unique, f"Non-unique (t,i,pid) in anthropometry {WAVE}"
assert len(df) > 0, f"anthropometry {WAVE} produced no rows"

to_parquet(df, 'anthropometry.parquet')
