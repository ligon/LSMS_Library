#!/usr/bin/env python
"""Build plot_features for Ethiopia ESS 2013-14 (Wave 2; GH #167).

One row per FIELD (sect3_pp_w2), with parcel-level Tenure / SoilType
broadcast from sect2_pp_w2 via the holder-aware join key
(holder_id, household_id, parcel_id).

i = household_id2 (NOT household_id): the W2 sample() and
household_roster index on household_id2, and only household_id2
overlaps the sample spine (99.3% vs 0% for household_id).  GPS
coordinates are 100% '**CONFIDENTIAL**' -> Latitude/Longitude not
emitted.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_features_for_wave


sect2 = get_dataframe('../Data/sect2_pp_w2.dta', convert_categoricals=False)
sect3 = get_dataframe('../Data/sect3_pp_w2.dta', convert_categoricals=False)

colmap = dict(
    hhid       = 'household_id2',  # EMITTED i; matches sample().i for W2
    join_hhid  = 'household_id',   # join key (present in both files)
    holder_id  = 'holder_id',
    parcel_id  = 'parcel_id',
    field_id   = 'field_id',
    area_gps   = 'pp_s3q05_a',
    area_unit  = 'pp_s3q02_c',
    acquire    = 'pp_s2q03',
    soil_type  = 'pp_s2q14',
    irrigated  = 'pp_s3q12',
)

df = plot_features_for_wave('2013-14', sect2, sect3, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2013-14"
assert len(df) > 0, "plot_features 2013-14 produced no rows"

to_parquet(df, 'plot_features.parquet')
