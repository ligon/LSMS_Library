"""plot_features for Tanzania NPS 2020-21 (NPS Y5, Refresh Panel; GH #167).

Merges plot area (ag_sec_02) onto plot detail (ag_sec_3a) on
(y5_hhid, plot_id).  Note: unlike 2019-20, BOTH 2020-21 modules key on
``plot_id`` (there is no ``plotnum`` column).  Emits raw y5_hhid as
``i``; the country-level concatenator applies id_walk and the framework
joins ``v`` from sample().  GPS coordinates in ag_sec_02 are
confidential / redacted and are not emitted.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_features_for_wave


sec02 = get_dataframe('../Data/ag_sec_02.dta', convert_categoricals=False)
sec3a = get_dataframe('../Data/ag_sec_3a.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'y5_hhid',
    plot       = 'plot_id',
    area_est   = 'ag2a_04',
    area_gps   = 'ag2a_09',
    use        = 'ag3a_03',
    soil_type  = 'ag3a_10',
    irrigated  = 'ag3a_18',
    erosion    = 'ag3a_15',
    acquire    = 'ag3a_25',
    legal_cert = 'ag3a_28a',
    cert_other = 'ag3a_28d',
)

df = plot_features_for_wave('2020-21', sec02, sec3a, colmap)
assert df.index.is_unique, "plot_features 2020-21: (t,i,plot_id) not unique"
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
