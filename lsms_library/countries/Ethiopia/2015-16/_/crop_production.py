#!/usr/bin/env python
"""Build crop_production for Ethiopia ESS 2015-16 (Wave 3; GAP 1).

Item-level harvest at (t, i, plot_id, j, u) from §9 (sect9_ph_w3), with
planting month from §4 (sect4_pp_w3) and reported sale qty/value from §11
(sect11_ph_w3).

W3 specifics: §9 harvest qty ph_s9q04_a is in a NATIVE unit (code
ph_s9q04_b).  intercropped from §9 ph_s9q01.  §11 sale qty ph_s11q03_a is
in a NATIVE unit (code ph_s11q03_b) — Quantity_sold attaches only where
the sale unit equals the harvest u; value is ph_s11q04 (Birr).
i = household_id2 (matches sample().i / plot_features for W3).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import crop_production_for_wave


harvest  = get_dataframe('../Data/sect9_ph_w3.dta',  convert_categoricals=False)
hunit    = get_dataframe('../Data/sect9_ph_w3.dta',  convert_categoricals=True)['ph_s9q04_b']
planting = get_dataframe('../Data/sect4_pp_w3.dta',  convert_categoricals=False)
sale     = get_dataframe('../Data/sect11_ph_w3.dta', convert_categoricals=False)
sunit    = get_dataframe('../Data/sect11_ph_w3.dta', convert_categoricals=True)['ph_s11q03_b']

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    crop_code='crop_code',
    quantity='ph_s9q04_a', unit='ph_s9q04_b',
    harvest_month='ph_s9q07_b',
    intercrop='ph_s9q01',
    pl_crop_code='crop_code', pl_month='pp_s4q12_a',
    s_crop_code='crop_code', s_sold_flag='ph_s11q01',
    s_qty='ph_s11q03_a', s_value='ph_s11q04',
)

df = crop_production_for_wave('2015-16', harvest, planting, sale, colmap,
                              unit_labels=hunit, sale_unit_labels=sunit)

assert len(df) > 0, "crop_production 2015-16 produced no rows"
to_parquet(df, 'crop_production.parquet')
