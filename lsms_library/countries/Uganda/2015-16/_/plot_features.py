"""plot_features for Uganda UNPS 2015-16 (GH #167 Phase 1).

Mostly identical to 2013-14 but the HHID column is in the canonical
form already (`HHID='H0081001'` short panel form, vs. 2013-14's
`HHID=int + hh='H...'` split).  ~97.8% of HHs map cleanly via
id_walk to sample's canonical i.
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_features_for_wave


df_2a = get_dataframe('../Data/AGSEC2A.dta', convert_categoricals=False)
df_2b = get_dataframe('../Data/AGSEC2B.dta', convert_categoricals=False)

colmap_2a = dict(
    hhid          = 'HHID',
    parcel_id     = 'parcelID',
    area_gps      = 'a2aq4',
    area_est      = 'a2aq5',
    tenure_system = 'a2aq7',
    acquire       = 'a2aq8',
    soil_type     = 'a2aq16',
    water_source  = 'a2aq18',
)
colmap_2b = dict(
    hhid          = 'HHID',
    parcel_id     = 'parcelID',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
    soil_type     = 'a2bq14',
    water_source  = 'a2bq16',
)

df_a = plot_features_for_wave('2015-16', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2015-16', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
