"""Build plot_features for Nigeria GHS-Panel wave 5 (2023-24; GH #167).

Post-planting only -> single t = 2023Q3.  Area (sect11a1) joined onto
plot detail (sect11b1) on (hhid, plotid).

TRAPS (recon): W5 fully RENUMBERS sect11b1.  Area number/unit are
s11aq3_number / s11aq3_unit (units add square-foot 8/9 and football
field 10).  GPS area is s11mq3.  Tenure acquire is s11b1q4 (9 codes);
the separate tenure-SYSTEM question s11b1q4b appears ONLY in W5.
Soil = s11b1q61, irrigation = s11b1q56.  Beware: s11b1q39 here is
"right to bequeath" and s11b1q44 is "main use" -- NOT soil/irrigation.
Land certificate is s11b1q8 (was s11b1q7 in W2-W4); erosion protection
is s11b1q66 (was s11b1q49); Fallow reads the main-use question s11b1q44
(code 1 = fallow).  No plot-geovariables file was released for W5, so
PlotSlope is NaN this wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import plot_features_for_wave, PP_QUARTER

t = PP_QUARTER['2023-24']

area = get_dataframe('../Data/Post Planting Wave 5/Agriculture/'
                     'sect11a1_plantingw5.dta', convert_categoricals=False)
detail = get_dataframe('../Data/Post Planting Wave 5/Agriculture/'
                       'sect11b1_plantingw5.dta', convert_categoricals=False)

colmap = dict(
    hhid='hhid', plot_id='plotid',
    area_est='s11aq3_number', area_unit='s11aq3_unit', area_gps='s11mq3',
    acquire='s11b1q4',          # tenure (9 codes)
    tenure_system='s11b1q4b',   # W5 only
    soil_type='s11b1q61',       # NOT s11b1q44 (= main use in W5)
    irrigated='s11b1q56',       # NOT s11b1q39 (= right to bequeath in W5)
    certificate='s11b1q8',      # land-ownership certificate (renumbered)
    erosion='s11b1q66',         # erosion-protection measure (renumbered)
    fallow='s11b1q44',          # main-use code 1 = fallow
    # no plot-geovariables file for W5 -> PlotSlope NaN
)

df = plot_features_for_wave(t, area, detail, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2023-24"
assert len(df) > 0, "plot_features 2023-24 produced no rows"

to_parquet(df, 'plot_features.parquet')
