#!/usr/bin/env python
"""Build crop_production for Ethiopia ESS 2021-22 (Wave 5; GAP 1).

Item-level harvest at (t, i, plot_id, j, u) from §9 (sect9_ph_w5), with
planting month from §4 (sect4_pp_w5) and reported sale qty/value from §11
(sect11_ph_w5).

W5 specifics: crop code in s9q00b (§9), s4q01b (§4), s11q01 (§11).
Harvest qty s9q05a (native unit s9q05b).  intercropped from §9 s9q02.
§11 sale qty s11q11a (native unit s11q11b), value s11q12, sold-flag
s11q07.  i = household_id.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import crop_production_for_wave


harvest  = get_dataframe('../Data/sect9_ph_w5.dta',  convert_categoricals=False)
hunit    = get_dataframe('../Data/sect9_ph_w5.dta',  convert_categoricals=True)['s9q05b']
planting = get_dataframe('../Data/sect4_pp_w5.dta',  convert_categoricals=False)
sale     = get_dataframe('../Data/sect11_ph_w5.dta', convert_categoricals=False)
sunit    = get_dataframe('../Data/sect11_ph_w5.dta', convert_categoricals=True)['s11q11b']

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    crop_code='s9q00b',
    quantity='s9q05a', unit='s9q05b',
    harvest_month='s9q08b',
    intercrop='s9q02',
    pl_crop_code='s4q01b', pl_month='s4q13a',
    s_crop_code='s11q01b', s_sold_flag='s11q07',
    s_qty='s11q11a', s_value='s11q12',
)

df = crop_production_for_wave('2021-22', harvest, planting, sale, colmap,
                              unit_labels=hunit, sale_unit_labels=sunit)

assert len(df) > 0, "crop_production 2021-22 produced no rows"
to_parquet(df, 'crop_production.parquet')
