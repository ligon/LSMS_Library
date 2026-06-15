"""plot_features for Tanzania NPS 2019-20 (NPS-SDD, Extended Panel; GH #167).

Merges plot area (AG_SEC_02) onto plot detail (AG_SEC_3A) on
(sdd_hhid, plotnum).  Emits raw sdd_hhid as ``i``; the country-level
concatenator applies id_walk and the framework joins ``v`` from
sample().  GPS coordinates in AG_SEC_02 are confidential / redacted and
are not emitted.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import plot_features_for_wave


sec02 = get_dataframe('../Data/AG_SEC_02.dta', convert_categoricals=False)
sec3a = get_dataframe('../Data/AG_SEC_3A.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'sdd_hhid',
    plot       = 'plotnum',
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

df = plot_features_for_wave('2019-20', sec02, sec3a, colmap)
assert df.index.is_unique, "plot_features 2019-20: (t,i,plot_id) not unique"
assert len(df) > 0
to_parquet(df, 'plot_features.parquet')
