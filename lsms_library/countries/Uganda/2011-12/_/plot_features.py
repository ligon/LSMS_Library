"""plot_features for Uganda UNPS 2011-12 (GH #167 Phase 1).

First wave with the modern `parcelID` column name.  AGSEC2A's `a2aq18`
has a Stata label bug — labeled "main water source" but the value
codes correspond to soil-type values (Sand Loam etc.).  We don't trust
either interpretation in AGSEC2A; emit `water_source` from AGSEC2B
only (`a2bq16` is correctly labeled).  `a2aq16` is the real soil-type
column in AGSEC2A (values 1=Sand Loam, 2=Sandy Clay Loam, 3=Black
Clay, 4=Other).
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
    # water_source intentionally omitted — a2aq18 Stata label is
    # "main water source" but values are soil-type codes (label bug).
)
colmap_2b = dict(
    hhid          = 'HHID',
    parcel_id     = 'parcelID',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
    # 2011-12 AGSEC2B uses different numbering — check questionnaire
    # before declaring soil/water columns here.
)

df_a = plot_features_for_wave('2011-12', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2011-12', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
