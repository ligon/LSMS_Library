"""Build livestock for Malawi IHS5 2019-20 (GAP 4).

Item-level (t, i, animal) livestock-roster feature.  IHS5 ships a
Cross_Sectional half (bare case_id, NOT cs-prefixed) and a Panel half
(y4_hhid), concatenated into the single 2019-20 wave -- exactly like
crop_production / plot_features.  Source per half: ag_mod_r1 (Module R
livestock roster), one row per (HH, species).
    animal = ag_r0a (LIVESTOCK CODE, coarse scheme),
    HeadCount = ag_r02, HeadAcquired = ag_r10 (bought to raise),
    HeadSold = ag_r16, Value = ag_r04 (per-head current value).

Same roster the World Bank code collapses to a binary
(MWI_IHPS4.do:1043-1051); we keep the pre-collapse rows.  See
lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _livestock_block, assemble_livestock


WAVE = '2019-20'

pieces = []

# --- Cross-sectional half (bare case_id) ---
r_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_r1.dta', convert_categoricals=False)
r_xs['hhid'] = r_xs['case_id'].apply(format_id)
pieces.append(_livestock_block(r_xs, hhid='hhid', animalcode='ag_r0a', t=WAVE))

# --- Panel half (y4_hhid) ---
r_pn = get_dataframe('../Data/Panel/ag_mod_r1_19.dta', convert_categoricals=False)
r_pn['hhid'] = r_pn['y4_hhid'].apply(format_id)
pieces.append(_livestock_block(r_pn, hhid='hhid', animalcode='ag_r0a', t=WAVE))

df = assemble_livestock(WAVE, pieces)

assert df.index.is_unique, f"Non-unique (t,i,animal) in livestock {WAVE}"
assert len(df) > 0, f"livestock {WAVE} produced no rows"

to_parquet(df, 'livestock.parquet')
