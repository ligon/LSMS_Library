"""plot_features for Uganda UNPS 2010-11 (GH #167 Phase 1).

Same Stata label-bug pattern as 2009-10 in AGSEC2A: `a2aq18` is
labeled "soil type" but the values are soil-quality codes.  AGSEC2B
uses `a2bq17` (correct soil-type labels).  Water source is in
a2aq20 / a2bq19.

First wave to use `prcid` (the modern parcelID convention; 2011-12+
uses `parcelID`).
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
    parcel_id     = 'prcid',
    area_gps      = 'a2aq4',
    area_est      = 'a2aq5',
    tenure_system = 'a2aq7',
    acquire       = 'a2aq8',
    water_source  = 'a2aq20',
    certificate   = 'a2aq25',   # formal certificate of title (1-3=Yes, 4=No)
    erosion       = 'a2aq24a',  # erosion-control facility (free-text method string)
)
colmap_2b = dict(
    hhid          = 'HHID',
    parcel_id     = 'prcid',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
    soil_type     = 'a2bq17',
    water_source  = 'a2bq19',
    # No certificate question for use-rights (AGSEC2B) parcels.
    erosion       = 'a2bq23a',  # erosion-control facility (free-text method string)
)

df_a = plot_features_for_wave('2010-11', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2010-11', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])
assert df.index.is_unique
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
