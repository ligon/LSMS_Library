#!/usr/bin/env python
"""Build crop_production for Ethiopia ESS 2011-12 (Wave 1; GAP 1).

Item-level harvest at (t, i, plot_id, j, u) from §9 post-harvest
(sect9_ph_w1), with planting month from §4 (sect4_pp_w1), the plot-level
mixed-stand flag from §3 (sect3_pp_w1), and reported sale qty/value from
§11 (sect11_ph_w1).

W1 specifics: the §9 harvest quantity is reported directly in KILOS
(ph_s9q12_a; grams residual ph_s9q12_b is dropped — median 0), so u='Kg'
and no unit code exists.  §9 has no pure/mixed-stand item variable, so
intercropped comes from the §3 plot roster (pp_s3q03: 1=No, 2=Yes).
i = household_id (matches sample().i for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import crop_production_for_wave


harvest  = get_dataframe('../Data/sect9_ph_w1.dta',  convert_categoricals=False)
planting = get_dataframe('../Data/sect4_pp_w1.dta',  convert_categoricals=False)
intercrop = get_dataframe('../Data/sect3_pp_w1.dta', convert_categoricals=False)
sale     = get_dataframe('../Data/sect11_ph_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    crop_code='crop_code',
    quantity='ph_s9q12_a',         # reported harvest, KILOS (u = 'Kg')
    harvest_month='ph_s9q13_b',    # harvest-end month
    # intercrop: §9 has none in W1 -> plot-roster fallback
    ic_holder_id='holder_id', ic_parcel_id='parcel_id',
    ic_field_id='field_id', ic_flag='pp_s3q03',
    pl_crop_code='crop_code', pl_month='pp_s4q12_a',
    s_crop_code='crop_code', s_sold_flag='ph_s11q01',
    s_qty='ph_s11q03_a', s_value='ph_s11q04_a',   # sale qty kilos, value Birr
)

df = crop_production_for_wave('2011-12', harvest, planting, sale, colmap,
                              intercrop=intercrop)

assert len(df) > 0, "crop_production 2011-12 produced no rows"
to_parquet(df, 'crop_production.parquet')
