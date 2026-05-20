"""plot_features for Uganda UNPS 2009-10 (GH #167 Phase 1).

AGSEC2A in this wave has Stata label bugs: `a2aq18` is labeled
"soil type" but its values are Good/Fair/Poor (so it's actually
soil-quality).  We skip `soil_type` for AGSEC2A 2009-10 rather than
emit a wrong-labeled column.  AGSEC2B uses `a2bq17` (correctly
labeled Sand Loam / Sandy Clay Loam / Black Clay codes).

The water-source column (a2aq20 in AGSEC2A, a2bq19 in AGSEC2B) is
correctly labeled.
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
    parcel_id     = 'a2aq2',
    area_gps      = 'a2aq4',
    area_est      = 'a2aq5',
    tenure_system = 'a2aq7',
    acquire       = 'a2aq8',
    # soil_type intentionally omitted — Stata label bug.
    water_source  = 'a2aq20',
)
colmap_2b = dict(
    hhid          = 'HHID',
    parcel_id     = 'a2bq2',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
    soil_type     = 'a2bq17',
    water_source  = 'a2bq19',
)

df_a = plot_features_for_wave('2009-10', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2009-10', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
