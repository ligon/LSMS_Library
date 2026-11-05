"""Build plot_features for Benin EHCVM 2021-22 (GH #552).

One source file, s16a_me_ben2021.dta, with the column names and code lists of 2018-19, checked
label by label (s16aq10 code 3 and s16aq17 code 1 reword the same answers).
benin.plot_features_for_wave builds it and spells 2018-19's bare household id, so the wave's
prefix goes on here.  s16aq47 is filled only where s16aq45 = Oui, so the area rule is 2018-19's.
"""
import sys

sys.path.append('../../_/')
from benin import plot_features_for_wave
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import ID_PREFIX

src = get_dataframe('../Data/s16a_me_ben2021.dta', convert_categoricals=False)

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
df = df.rename(index=lambda value: ID_PREFIX + value, level='i')

assert df.index.is_unique, 'Non-unique (t, i, plot_id) index in plot_features 2021-22'
assert len(df) == len(src), 'plot_features 2021-22 lost a parcel'
to_parquet(df, 'plot_features.parquet')
