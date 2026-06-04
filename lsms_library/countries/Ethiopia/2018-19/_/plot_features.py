#!/usr/bin/env python
"""Build plot_features for Ethiopia ESS 2018-19 (Wave 4; GH #167).

One row per FIELD (sect3_pp_w4), parcel-level Tenure / SoilType
broadcast from sect2_pp_w4 via (holder_id, household_id, parcel_id).
W4 uses the s{N}q* variable naming (vs pp_s{N}q* in W1-W3) and the
full 1-8 acquire / 1-11 area-unit code schemes.  i = household_id
(matches sample().i for W4).  GPS 100% '**CONFIDENTIAL**' -> not
emitted.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_features_for_wave


sect2 = get_dataframe('../Data/sect2_pp_w4.dta', convert_categoricals=False)
sect3 = get_dataframe('../Data/sect3_pp_w4.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'household_id',
    join_hhid  = 'household_id',
    holder_id  = 'holder_id',
    parcel_id  = 'parcel_id',
    field_id   = 'field_id',
    area_gps   = 's3q08',    # GPS-measured field area, square metres
    area_unit  = 's3q02b',   # farmer-estimate area unit code
    acquire    = 's2q05',    # how acquired -> Tenure
    soil_type  = 's2q16',
    irrigated  = 's3q17',    # 1 Yes / 2 No
)

df = plot_features_for_wave('2018-19', sect2, sect3, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2018-19"
assert len(df) > 0, "plot_features 2018-19 produced no rows"

to_parquet(df, 'plot_features.parquet')
