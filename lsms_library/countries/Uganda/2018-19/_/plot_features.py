"""plot_features for Uganda UNPS 2018-19 (GH #167 Phase 1).

This wave switches to a new HHID scheme (`hhid` = 32-char hash) and
q-prefix `s2aq*` for the AGSEC2A columns.  AGSEC2B uses `s2aq0*`
(leading zero on the 2-digit codes — different from AGSEC2A's `s2aq*`).
Water-source column kept its legacy `a2aq18` name (one column the
schema didn't rename).  ~98.3% of HHs map cleanly to sample.
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_features_for_wave


df_2a = get_dataframe('../Data/AGSEC2A.dta', convert_categoricals=False)
df_2b = get_dataframe('../Data/AGSEC2B.dta', convert_categoricals=False)

colmap_2a = dict(
    hhid          = 'hhid',
    parcel_id     = 'parcelID',
    area_gps      = 's2aq4',
    area_est      = 's2aq5',
    tenure_system = 's2aq7',
    acquire       = 's2aq8',
    soil_type     = 's2aq16',
    water_source  = 'a2aq18',
)
colmap_2b = dict(
    hhid          = 'hhid',
    parcel_id     = 'parcelID',
    area_gps      = 's2aq04',
    area_est      = 's2aq05',
    tenure_system = 's2aq07',
    acquire       = 's2aq08',
    soil_type     = 's2aq16',
    water_source  = 'a2aq18',
)

df_a = plot_features_for_wave('2018-19', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2018-19', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
