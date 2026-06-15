#!/usr/bin/env python
"""Build plot_features for Ethiopia ESS 2021-22 (Wave 5; GH #167).

One row per FIELD (sect3_pp_w5), parcel-level Tenure / SoilType
broadcast from sect2_pp_w5 via (holder_id, household_id, parcel_id).
Same s{N}q* variable scheme as W4.  i = household_id (matches
sample().i for W5).  GPS 100% '**CONFIDENTIAL**' -> not emitted.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_features_for_wave


sect2 = get_dataframe('../Data/sect2_pp_w5.dta', convert_categoricals=False)
sect3 = get_dataframe('../Data/sect3_pp_w5.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'household_id',
    join_hhid  = 'household_id',
    holder_id  = 'holder_id',
    parcel_id  = 'parcel_id',
    field_id   = 'field_id',
    area_gps   = 's3q08',
    area_unit  = 's3q02b',
    acquire    = 's2q05',
    soil_type  = 's2q16',
    irrigated  = 's3q17',
    certificate= 's2q03',    # parcel has a certificate? 1 Yes / 2 No
    fallow     = 's3q03',    # field land-status (code 3 == Fallow)
    erosion    = 's3q38',    # erosion-control structure? 1 Yes / 2 No
    erosion_yes= 1, erosion_no = 2,   # W4-W5 code 1=Yes / 2=No
)

df = plot_features_for_wave('2021-22', sect2, sect3, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2021-22"
assert len(df) > 0, "plot_features 2021-22 produced no rows"

to_parquet(df, 'plot_features.parquet')
