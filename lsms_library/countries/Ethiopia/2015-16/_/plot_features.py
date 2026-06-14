#!/usr/bin/env python
"""Build plot_features for Ethiopia ESS 2015-16 (Wave 3; GH #167).

One row per FIELD (sect3_pp_w3), parcel-level Tenure / SoilType
broadcast from sect2_pp_w3 via (holder_id, household_id, parcel_id).

i = household_id2 (NOT household_id): the W3 sample()/household_roster
index on household_id2, and only household_id2 overlaps the sample
spine (99.3% vs 0% for household_id).  W3 adds acquire codes
6='Shared Crop in' (sharecropped_in) and 7='Purchased' (owned).
GPS coordinates 100% '**CONFIDENTIAL**' -> not emitted.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_features_for_wave


sect2 = get_dataframe('../Data/sect2_pp_w3.dta', convert_categoricals=False)
sect3 = get_dataframe('../Data/sect3_pp_w3.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'household_id2',  # EMITTED i; matches sample().i for W3
    join_hhid  = 'household_id',
    holder_id  = 'holder_id',
    parcel_id  = 'parcel_id',
    field_id   = 'field_id',
    area_gps   = 'pp_s3q05_a',
    area_unit  = 'pp_s3q02_c',
    acquire    = 'pp_s2q03',
    soil_type  = 'pp_s2q14',
    irrigated  = 'pp_s3q12',
    certificate= 'pp_s2q04',     # 1 Yes / 2 No
    fallow     = 'pp_s3q03',     # field land-status (code 3 == Fallow)
    erosion    = 'pp_s3q32',     # erosion-control structure?
    erosion_yes= 2, erosion_no = 1,   # W1-W3 code 2=Yes / 1=No
)

df = plot_features_for_wave('2015-16', sect2, sect3, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2015-16"
assert len(df) > 0, "plot_features 2015-16 produced no rows"

to_parquet(df, 'plot_features.parquet')
