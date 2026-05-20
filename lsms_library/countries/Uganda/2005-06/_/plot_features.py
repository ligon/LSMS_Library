"""plot_features for Uganda UNHS 2005-06 (GH #167 Phase 1).

Earliest wave; AGSEC2A and AGSEC2B carry only area + tenure_system +
acquire — no soil or water-source questions in 2005-06.  See
slurm_logs/uganda_plot_labels_all_2026-05-20.txt for the column dump.
"""
import sys
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_features_for_wave


df_2a = get_dataframe('../Data/AGSEC2A.dta', convert_categoricals=False)
df_2b = get_dataframe('../Data/AGSEC2B.dta', convert_categoricals=False)

# 2005-06 HHID is already in 10-digit canonical form (no `hh` column);
# id_walk treats unmapped IDs as identity (the 1900 HHIDs in this
# wave are all in sample directly).
colmap_2a = dict(
    hhid          = 'HHID',
    parcel_id     = 'a2aq2',     # parcelID column not yet introduced
    area_gps      = 'a2aq4',
    area_est      = 'a2aq5',
    tenure_system = 'a2aq7',
    acquire       = 'a2aq8',
    # soil_type / water_source not collected in 2005-06.
)
colmap_2b = dict(
    hhid          = 'HHID',
    parcel_id     = 'a2bq2',
    area_gps      = 'a2bq4',
    area_est      = 'a2bq5',
    tenure_system = 'a2bq7',
    acquire       = 'a2bq8',
)

df_a = plot_features_for_wave('2005-06', df_2a, None, colmap_2a)
df_b = plot_features_for_wave('2005-06', None, df_2b, colmap_2b)
df = pd.concat([df_a, df_b])

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2005-06"
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
