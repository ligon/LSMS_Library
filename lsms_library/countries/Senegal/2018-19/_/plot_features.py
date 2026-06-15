"""Build plot_features for Senegal EHCVM 2018-19 (GH #167; EHCVM cluster).

Single source file: s16a_me_sen2018.dta (agriculture-parcel module).
plot_id = "{field_no}_{parcel_no}" (s16aq02 _ s16aq03); unique within
each (grappe, menage).  See
lsms_library/countries/Senegal/_/senegal.py:plot_features_for_wave for
the harmonization shared across all EHCVM waves and the sibling
countries (Mali is the reference implementation, PR #284).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from senegal import plot_features_for_wave


# convert_categoricals=False keeps the integer s16a codes that the
# categorical_mapping.org harmonize_* tables key on.
src = get_dataframe('../Data/s16a_me_sen2018.dta', convert_categoricals=False)

colmap = dict(
    grappe        = 'grappe',
    menage        = 'menage',
    field_no      = 's16aq02',
    parcel_no     = 's16aq03',
    area_gps      = 's16aq47',
    gps_measured  = 's16aq45',
    area_est      = 's16aq09a',
    area_est_unit = 's16aq09b',
    tenure        = 's16aq10',
    tenure_system = 's16aq13',
    soil_type     = 's16aq18',
    water_source  = 's16aq17',
)

df = plot_features_for_wave('2018-19', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2018-19"
assert len(df) > 0, "plot_features 2018-19 produced no rows"

to_parquet(df, 'plot_features.parquet')
