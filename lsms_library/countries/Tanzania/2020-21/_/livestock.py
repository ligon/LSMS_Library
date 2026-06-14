"""livestock for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
parity-loop GAP 4).

Item-level herd at grain (t, i, animal) from the livestock roster lf_sec_02
(one row per (household, lvstckid)).  We keep only OWNED rows (lf02_01 == 1),
resolve lvstckid -> canonical species via harmonize_species, and carry the
reported head counts:
  HeadCount    = lf02_04_1 (indigenous) + lf02_04_2 (improved/exotic)
  HeadAcquired = lf02_07  (bought alive in past 12 months)
  HeadSold     = lf02_25  (sold alive in past 12 months)
Emits raw y5_hhid as ``i``; the country-level concatenator applies id_walk.
'livestock' is in the framework _no_v_join set, so NO v level is joined.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import livestock_for_wave


lf = get_dataframe('../Data/lf_sec_02.dta', convert_categoricals=False)

colmap = dict(
    hhid='y5_hhid', animal='lvstckid', own='lf02_01',
    owned=['lf02_04_1', 'lf02_04_2'], bought='lf02_07', sold='lf02_25',
    species_col='2020-21')

df = livestock_for_wave('2020-21', lf, colmap)
assert df.index.is_unique, "livestock 2020-21: (t,i,animal) not unique"
assert len(df) > 0
to_parquet(df, 'livestock.parquet')
