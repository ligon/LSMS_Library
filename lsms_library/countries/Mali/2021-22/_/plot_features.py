"""Build plot_features for Mali EHCVM 2021-22 (GH #167; EHCVM cluster).

Single source file: s16a_me_mli2021.dta (agriculture-parcel module,
9,924 rows).  The 2021-22 instrument uses the SAME column names and
integer code scheme as 2018-19 (the value labels merely add a "N. "
numeric prefix), so the colmap is identical.  See
lsms_library/countries/Mali/_/mali.py:plot_features_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from mali import plot_features_for_wave


src = get_dataframe('../Data/s16a_me_mli2021.dta', convert_categoricals=False)

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

df = plot_features_for_wave('2021-22', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2021-22"
assert len(df) > 0, "plot_features 2021-22 produced no rows"

to_parquet(df, 'plot_features.parquet')
