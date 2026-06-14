#!/usr/bin/env python
"""Build plot_inputs for Ethiopia ESS 2013-14 (Wave 2; GAP 2).

Item-level ag inputs at (t, i, plot_id, input, j).

W2 sources / specifics:
  - Seed quantity (§4 sect4_pp_w2): pp_s4q11b (kg), per (field, crop);
    improved flag pp_s4q11 (1/3=No, 2=Yes).  §4 carries the plot id.
  - Seed purchase (§5 sect5_pp_w2): pp_s5q03 flag + pp_s5q05_a/b kg+gram
    at (HOLDER, crop) grain (no field_id) -> attached to the §4 plot-crop
    seed rows ONLY where (holder, crop) maps to exactly one plot.
  - Fertilizer (§3 sect3_pp_w2): UREA used pp_s3q15, kg pp_s3q16_a,
    purchased kg pp_s3q16c; DAP used pp_s3q18, kg pp_s3q19_a, purchased
    pp_s3q19c.  No NPS in W2.
  - Organic (§3): manure pp_s3q21 / compost pp_s3q23 / other pp_s3q25.
  - Pesticide (§4): pp_s4q05 used, gated by pp_s4q04==2.
i = household_id2 (matches sample().i for W2).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_inputs_for_wave


plot_roster = get_dataframe('../Data/sect3_pp_w2.dta', convert_categoricals=False)
planting    = get_dataframe('../Data/sect4_pp_w2.dta', convert_categoricals=False)
seeds       = get_dataframe('../Data/sect5_pp_w2.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',

    # seed quantity from §4 (field-crop grain); improved also from §4
    seed_src='planting', seed_crop_code='crop_code',
    seed_qty='pp_s4q11b',
    improved_code='pp_s4q11', improved_yes=(2,),

    # seed purchase from §5 (holder-crop grain): flag + kg + grams
    p_holder_id='holder_id', p_crop_code='crop_code',
    p_purch_flag='pp_s5q03', p_purch_a='pp_s5q05_a', p_purch_b='pp_s5q05_b',

    # inorganic fertilizer (§3): used + total kg + purchased kg
    urea_used='pp_s3q15', urea_kg='pp_s3q16_a', urea_purch_kg='pp_s3q16c',
    dap_used='pp_s3q18', dap_kg='pp_s3q19_a', dap_purch_kg='pp_s3q19c',
    # organic (§3)
    manure_used='pp_s3q21', compost_used='pp_s3q23', other_organic_used='pp_s3q25',

    # pesticide (§4)
    pest_crop_code='crop_code', pest_used='pp_s4q05', pest_gate='pp_s4q04',
)

df = plot_inputs_for_wave('2013-14', plot_roster, planting, seeds, colmap)

assert len(df) > 0, "plot_inputs 2013-14 produced no rows"
to_parquet(df, 'plot_inputs.parquet')
