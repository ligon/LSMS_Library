"""Build plot_features for Nigeria GHS-Panel wave 1 (2010-11; GH #167).

Post-planting only -> single t = 2010Q3.  Area (sect11a1) joined onto
plot detail (sect11b) on (hhid, plotid).  Wave 1's sect11b stops at q28,
so it carries NO soil type, irrigation, separate tenure-system, land
certificate, or erosion-protection question; SoilType / Irrigated /
TenureSystem / PlotCertificate / ErosionProtection are NaN this wave.
Fallow comes from s11bq17 ("left fallow"), with s11bq16 ("cultivated?")
forcing not-fallow.  PlotSlope from the plot-geovariables file
(srtmslp_nga, degrees), joined on (hhid, plotid).
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
geovar = get_dataframe('../Data/nga_plotgeovariables_y1.csv',
                       convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq4a', area_unit='s11aq4b', area_gps='s11aq4d',
    acquire='s11bq4',          # tenure
    fallow='s11bq17',          # 1 = "left fallow"
    fallow_cultivated='s11bq16',   # 1 = cultivated -> not fallow
    slope='srtmslp_nga',
    # no soil / irrigation / tenure-system / certificate / erosion in W1
)

df = plot_features_for_wave(t, area, detail, colmap, geovar=geovar)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2010-11"
assert len(df) > 0, "plot_features 2010-11 produced no rows"

to_parquet(df, 'plot_features.parquet')
