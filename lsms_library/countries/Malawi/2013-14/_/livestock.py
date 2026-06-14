"""Build livestock for Malawi IHS3-IHPS 2013-14 (GAP 4).

Item-level (t, i, animal) livestock-roster feature.  Source:
  * AG_MOD_R1_13 -- Module R livestock roster, one row per (HH, species).
    animal = ag_r0a (LIVESTOCK CODE, coarse scheme with Ox=3304 etc.),
    HeadCount = ag_r02, HeadAcquired = ag_r10 (bought to raise),
    HeadSold = ag_r16, Value = ag_r04 (per-head current value).

i = format_id(y2_hhid), aligning with plot_features / crop_production
2013-14.  Same roster the World Bank code collapses to a binary
(MWI_IHPS2.do:1032-1040); we keep the pre-collapse rows.  See
lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _livestock_block, assemble_livestock


WAVE = '2013-14'

r = get_dataframe('../Data/AG_MOD_R1_13.dta', convert_categoricals=False)
r['hhid'] = r['y2_hhid'].apply(format_id)

piece = _livestock_block(r, hhid='hhid', animalcode='ag_r0a', t=WAVE)

df = assemble_livestock(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,animal) in livestock {WAVE}"
assert len(df) > 0, f"livestock {WAVE} produced no rows"

to_parquet(df, 'livestock.parquet')
