#!/usr/bin/env python
"""Build crop_production for Ethiopia ESS 2013-14 (Wave 2; GAP 1).

Item-level harvest at (t, i, plot_id, j, u) from §9 (sect9_ph_w2), with
planting month from §4 (sect4_pp_w2) and reported sale qty/value from §11
(sect11_ph_w2).

W2 specifics: §9 harvest qty ph_s9q04_a is in a NATIVE unit (code
ph_s9q04_b) — we carry the reported quantity and the native unit label
(no kg conversion).  intercropped from §9 ph_s9q01 (1=Pure->False,
2=Mixed->True).  §11 sale qty ph_s11q03_a is in KILOS (ph_s11q03_b is the
grams residual), so the sale unit is 'Kg'; sale value is ph_s11q04.
i = household_id2 (matches sample().i / plot_features for W2).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import crop_production_for_wave


harvest  = get_dataframe('../Data/sect9_ph_w2.dta',  convert_categoricals=False)
hunit    = get_dataframe('../Data/sect9_ph_w2.dta',  convert_categoricals=True)['ph_s9q04_b']
planting = get_dataframe('../Data/sect4_pp_w2.dta',  convert_categoricals=False)
sale     = get_dataframe('../Data/sect11_ph_w2.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    crop_code='crop_code',
    quantity='ph_s9q04_a', unit='ph_s9q04_b',
    harvest_month='ph_s9q07_b',
    intercrop='ph_s9q01',
    pl_crop_code='crop_code', pl_month='pp_s4q12_a',
    s_crop_code='crop_code', s_sold_flag='ph_s11q01',
    s_qty='ph_s11q03_a', s_value='ph_s11q04',     # sale qty kilos, value Birr
)

df = crop_production_for_wave('2013-14', harvest, planting, sale, colmap,
                              unit_labels=hunit)

assert len(df) > 0, "crop_production 2013-14 produced no rows"
to_parquet(df, 'crop_production.parquet')
