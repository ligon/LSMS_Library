"""Build livestock for Malawi IHS3 2010-11 (GAP 4).

Item-level (t, i, animal) livestock-roster feature.  Source
(Full_Sample/Agriculture):
  * ag_mod_r1 -- Module R livestock roster, one row per (HH, species).
    animal = ag_r0a (LIVESTOCK CODE, 2010-11 fine 301-318 scheme),
    HeadCount = ag_r02, HeadAcquired = ag_r10 (bought to raise),
    HeadSold = ag_r16, Value = ag_r04 (per-head current value).

i = format_id(case_id), aligning with plot_features / crop_production
2010-11.  This is the SAME roster the World Bank code collapses to a
single HH "engaged-in-livestock" binary (MWI_IHPS1.do:978-986); we keep
the pre-collapse rows.  See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import _livestock_block, assemble_livestock


WAVE = '2010-11'
BASE = '../Data/Full_Sample/Agriculture/'

r = get_dataframe(BASE + 'ag_mod_r1.dta', convert_categoricals=False)
r['hhid'] = r['case_id'].apply(format_id)

piece = _livestock_block(r, hhid='hhid', animalcode='ag_r0a', t=WAVE)

df = assemble_livestock(WAVE, [piece])

assert df.index.is_unique, f"Non-unique (t,i,animal) in livestock {WAVE}"
assert len(df) > 0, f"livestock {WAVE} produced no rows"

to_parquet(df, 'livestock.parquet')
