"""Build plot_features for Nigeria GHS-Panel wave 2 (2012-13; GH #167).

Post-planting only -> single t = 2012Q3.  Area (sect11a1) joined onto
plot detail (sect11b1) on (hhid, plotid).  Erosion protection is absent
this wave (ErosionProtection NaN).  PlotSlope from the plot-geovariables
file (srtmslp_nga, degrees), joined on (hhid, plotid).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import plot_features_for_wave, PP_QUARTER

t = PP_QUARTER['2012-13']

area = get_dataframe('../Data/Post Planting Wave 2/Agriculture/'
                     'sect11a1_plantingw2.dta', convert_categoricals=False)
detail = get_dataframe('../Data/Post Planting Wave 2/Agriculture/'
                       'sect11b1_plantingw2.dta', convert_categoricals=False)
geovar = get_dataframe('../Data/nga_plotgeovariables_y2.csv',
                       convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq4a', area_unit='s11aq4b', area_gps='s11aq4c',
    acquire='s11b1q4',          # tenure
    soil_type='s11b1q44',
    irrigated='s11b1q39',
    certificate='s11b1q7',      # land-ownership certificate
    fallow='s11b1q28',          # main-use code 1 = fallow
    slope='srtmslp_nga',
    # no separate tenure-system question pre-W5; erosion absent in W2
)

df = plot_features_for_wave(t, area, detail, colmap, geovar=geovar)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2012-13"
assert len(df) > 0, "plot_features 2012-13 produced no rows"

to_parquet(df, 'plot_features.parquet')
