#!/usr/bin/env python
"""Build plot_features for Ethiopia ESS 2011-12 (Wave 1; GH #167).

One row per FIELD (sect3_pp_w1), with parcel-level Tenure broadcast
from sect2_pp_w1 via the holder-aware join key
(holder_id, household_id, parcel_id).  See
lsms_library/countries/Ethiopia/_/ethiopia.py:plot_features_for_wave.

W1 specifics: soil type is NOT asked (SoilType -> NaN); the acquire
question uses the narrow 1-6 scheme plus alias codes 10/11/12; the
field area unit code 7 means 'Other' (not 'Tilm' as in W2+).
GPS coordinates are 100% '**CONFIDENTIAL**' -> Latitude/Longitude
not emitted.  i = household_id (matches sample().i for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_features_for_wave


sect2 = get_dataframe('../Data/sect2_pp_w1.dta', convert_categoricals=False)
sect3 = get_dataframe('../Data/sect3_pp_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'household_id',
    join_hhid  = 'household_id',
    holder_id  = 'holder_id',
    parcel_id  = 'parcel_id',
    field_id   = 'field_id',
    area_gps   = 'pp_s3q05_a',   # GPS-measured field area, square metres
    area_unit  = 'pp_s3q02_c',   # farmer-estimate area unit code
    acquire    = 'pp_s2q03',     # how the parcel was acquired -> Tenure
    irrigated  = 'pp_s3q12',     # 1 Yes / 2 No
    # soil_type: not asked in W1 -> SoilType NaN
)

df = plot_features_for_wave('2011-12', sect2, sect3, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2011-12"
assert len(df) > 0, "plot_features 2011-12 produced no rows"

to_parquet(df, 'plot_features.parquet')
