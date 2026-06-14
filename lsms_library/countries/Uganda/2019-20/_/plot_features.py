"""plot_features for Uganda UNPS 2019-20 (GH #167 Phase 1).

Same shape as 2018-19 but the Data directory is nested in `Agric/`
(lowercase) per the 2019-20 file layout.  `hhid` (32-char hash) is
the canonical HH id; sample 2019-20 has it 100% matched.
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_features_for_wave


df_2a = get_dataframe('../Data/Agric/agsec2a.dta', convert_categoricals=False)
df_2b = get_dataframe('../Data/Agric/agsec2b.dta', convert_categoricals=False)

colmap_2a = dict(
    hhid          = 'hhid',
    parcel_id     = 'parcelID',
    area_gps      = 's2aq4',
    area_est      = 's2aq5',
    tenure_system = 's2aq7',
    acquire       = 's2aq8',
    # soil_type column appears absent from the 2019-20 AGSEC2A keyword
    # grep — to verify on a follow-up.  Omitting for v1.
    water_source  = 'a2aq18',
    certificate   = 's2aq23',   # formal certificate of title (1-3=Yes, 4=No)
    erosion       = 's2aq22a',  # erosion-control facility (method code; 8=None)
)
colmap_2b = dict(
    hhid          = 'hhid',
    parcel_id     = 'parcelID',
    area_gps      = 's2aq04',
    area_est      = 's2aq05',
    tenure_system = 's2aq07',
    acquire       = 's2aq08',
    water_source  = 'a2aq18',
    # No certificate question for use-rights (AGSEC2B) parcels.
    erosion       = 's2aq22a',  # erosion-control facility (method code; 8=None)
)

df_a = plot_features_for_wave('2019-20', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2019-20', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
