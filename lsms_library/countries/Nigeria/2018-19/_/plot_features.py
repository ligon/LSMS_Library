"""Build plot_features for Nigeria GHS-Panel wave 4 (2018-19; GH #167).

Post-planting only -> single t = 2018Q3.  Area (sect11a1) joined onto
plot detail (sect11b1) on (hhid, plotid).

TRAP (recon): in W4 the AREA NUMBER is s11aq4aa.  s11aq4a is a GPS
yes/no FLAG (1/2), NOT the area -- do not use it.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import plot_features_for_wave, PP_QUARTER

t = PP_QUARTER['2018-19']

area = get_dataframe('../Data/sect11a1_plantingw4.dta',
                     convert_categoricals=False)
detail = get_dataframe('../Data/sect11b1_plantingw4.dta',
                       convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq4aa',        # NOT s11aq4a (that is a GPS yes/no flag)
    area_unit='s11aq4b', area_gps='s11aq4c',
    acquire='s11b1q4',          # tenure (7 codes)
    soil_type='s11b1q44',
    irrigated='s11b1q39',
)

df = plot_features_for_wave(t, area, detail, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2018-19"
assert len(df) > 0, "plot_features 2018-19 produced no rows"

to_parquet(df, 'plot_features.parquet')
