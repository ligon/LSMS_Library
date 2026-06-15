"""Build plot_features for Uganda UNPS 2013-14 (GH #167 Phase 1).

AGSEC2A = owned parcels (4,142 rows); AGSEC2B = use-rights / rented
parcels (1,294 rows).  The plot_id is the within-HH parcelID suffixed
'_A' or '_B' to disambiguate the source file.

See lsms_library/countries/Uganda/_/uganda.py:plot_features_for_wave
for the harmonization logic shared across all 8 UNPS waves.
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_features_for_wave


# convert_categoricals=False keeps the integer codes that the
# categorical_mapping.org harmonize_* tables key on.
df_2a = get_dataframe('../Data/AGSEC2A.dta', convert_categoricals=False)
df_2b = get_dataframe('../Data/AGSEC2B.dta', convert_categoricals=False)

# Per-wave column-name map.  All keys are documented in
# uganda.plot_features_for_wave.
# `hh` is the panel-form household id (H-prefix string) that updated_ids
# can id_walk to the canonical 10-digit form to match sample().  The
# AGSEC2A `HHID` column is an int (internal section index) that does
# NOT match sample's i.
colmap = dict(
    hhid          = 'hh',
    parcel_id     = 'parcelID',
    area_gps      = 'a2aq4',
    area_est      = 'a2aq5',
    tenure_system = 'a2aq7',
    acquire       = 'a2aq8',
    soil_type     = 'a2aq16',
    water_source  = 'a2aq18',
    certificate   = 'a2aq23',   # formal certificate of title (1-3=Yes, 4=No)
    erosion       = 'a2aq22a',  # erosion-control facility (method code; 8=None)
)

# AGSEC2B uses a2bq* numbering (vs AGSEC2A's a2aq*), so we call the
# helper with a separate colmap per source and concat.
colmap_2b = dict(
    hhid          = 'hh',
    parcel_id     = 'parcelID',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
    soil_type     = 'a2bq14',
    water_source  = 'a2bq16',
    # No certificate question for use-rights (AGSEC2B) parcels.
    erosion       = 'a2bq20a',  # erosion-control facility (method code; 8=None)
)

# Call helper twice (once per source) so each source uses its own
# column map; helper internally picks letter via the source position.
df_a = plot_features_for_wave('2013-14', df_2a, None, colmap)
df_b = plot_features_for_wave('2013-14', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2013-14"
assert len(df) > 0, "plot_features 2013-14 produced no rows"

to_parquet(df, 'plot_features.parquet')
