#!/usr/bin/env python
"""Build crop_production for Ethiopia ESS 2013-14 (Wave 2; GAP 1).

Item-level harvest at (t, i, plot_id, j, u) from §9 (sect9_ph_w2), with
planting month from §4 (sect4_pp_w2) and reported sale qty/value from §11
(sect11_ph_w2), plus the §12 tree/fruit/vegetable/root-crop sale module
(sect12_ph_w2; GH #820).

W2 specifics: §9 harvest qty ph_s9q04_a is in a NATIVE unit (code
ph_s9q04_b) — we carry the reported quantity and the native unit label
(no kg conversion).  intercropped from §9 ph_s9q01 (1=Pure->False,
2=Mixed->True).  §11 sale qty ph_s11q03_a is in KILOS (ph_s11q03_b is the
grams residual), so the sale unit is 'Kg'; sale value is ph_s11q04.
i = household_id2 (matches sample().i / plot_features for W2).

§12 (GH #820): Q6 sold flag ph_s12q06, Q7 quantity sold ph_s12q07 (KG, per
the W2 questionnaire header and variable label), Q8 value ph_s12q08 (Birr;
"IF SOLD IN-KIND, PLEASE RECORD THE CASH VALUE OF SALES").  §9 INCLUDES
tree/root crops in W2, so §12 only fills the sale columns §11 left empty --
see ethiopia._attach_sect12_sales.
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
sale12   = get_dataframe('../Data/sect12_ph_w2.dta', convert_categoricals=False)

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
    s12_crop_code='crop_code', s12_sold_flag='ph_s12q06',
    s12_qty='ph_s12q07', s12_value='ph_s12q08',     # §12 qty KG, value Birr
)

df = crop_production_for_wave('2013-14', harvest, planting, sale, colmap,
                              unit_labels=hunit, sale12=sale12)

assert len(df) > 0, "crop_production 2013-14 produced no rows"
to_parquet(df, 'crop_production.parquet')
