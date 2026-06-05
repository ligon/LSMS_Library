"""Build plot_features for Nigeria GHS-Panel wave 1 (2010-11; GH #167).

Post-planting only -> single t = 2010Q3.  Area (sect11a1) joined onto
plot detail (sect11b) on (hhid, plotid).  Wave 1's sect11b stops at q28,
so it carries NO soil type, irrigation, or separate tenure-system
question; SoilType / Irrigated / TenureSystem are NaN this wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import plot_features_for_wave, PP_QUARTER

t = PP_QUARTER['2010-11']

area = get_dataframe('../Data/Post Planting Wave 1/Agriculture/'
                     'sect11a1_plantingw1.dta', convert_categoricals=False)
detail = get_dataframe('../Data/Post Planting Wave 1/Agriculture/'
                       'sect11b_plantingw1.dta', convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq4a', area_unit='s11aq4b', area_gps='s11aq4d',
    acquire='s11bq4',          # tenure
    # no soil / irrigation / tenure-system questions in W1
)

df = plot_features_for_wave(t, area, detail, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2010-11"
assert len(df) > 0, "plot_features 2010-11 produced no rows"

to_parquet(df, 'plot_features.parquet')
