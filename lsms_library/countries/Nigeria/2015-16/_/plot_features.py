"""Build plot_features for Nigeria GHS-Panel wave 3 (2015-16; GH #167).

Post-planting only -> single t = 2015Q3.  Area (sect11a1) joined onto
plot detail (sect11b1) on (hhid, plotid).  W3 acquire scheme has 6 codes
(adds family inheritance = 5, sharecropped = 6).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import plot_features_for_wave, PP_QUARTER

t = PP_QUARTER['2015-16']

area = get_dataframe('../Data/sect11a1_plantingw3.dta',
                     convert_categoricals=False)
detail = get_dataframe('../Data/sect11b1_plantingw3.dta',
                       convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq4a', area_unit='s11aq4b', area_gps='s11aq4c',
    acquire='s11b1q4',          # tenure
    soil_type='s11b1q44',
    irrigated='s11b1q39',
)

df = plot_features_for_wave(t, area, detail, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2015-16"
assert len(df) > 0, "plot_features 2015-16 produced no rows"

to_parquet(df, 'plot_features.parquet')
