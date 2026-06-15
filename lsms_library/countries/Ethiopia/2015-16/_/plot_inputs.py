#!/usr/bin/env python
"""Build plot_inputs for Ethiopia ESS 2015-16 (Wave 3; GAP 2).

Item-level ag inputs at (t, i, plot_id, input, j).

W3 sources / specifics:
  - Seed quantity (§4 sect4_pp_w3): kg = pp_s4q11b_a + pp_s4q11b_b*0.001
    (grams), per (field, crop); improved pp_s4q11 (1=No, 2=Yes).
  - Seed purchase (§5 sect5_pp_w3): pp_s5q03 flag + pp_s5q05_a/b kg+gram
    at (HOLDER, crop) grain -> attached unambiguously (one plot only).
  - Fertilizer (§3 sect3_pp_w3): UREA used pp_s3q15, kg pp_s3q16,
    purchased pp_s3q16c; DAP used pp_s3q18, kg pp_s3q19, purchased
    pp_s3q19c; NPS used pp_s3q20a_1, kg pp_s3q20a_2, purchased
    pp_s3q20a_4 (NPS introduced this wave).
  - Organic (§3): manure pp_s3q21 / compost pp_s3q23 / other pp_s3q25.
  - Pesticide (§4): pp_s4q05 used, gated by pp_s4q04==2.
i = household_id2 (matches sample().i for W3).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_inputs_for_wave


plot_roster = get_dataframe('../Data/sect3_pp_w3.dta', convert_categoricals=False)
planting    = get_dataframe('../Data/sect4_pp_w3.dta', convert_categoricals=False)
seeds       = get_dataframe('../Data/sect5_pp_w3.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',

    # seed quantity from §4 (kg + grams); improved from §4
    seed_src='planting', seed_crop_code='crop_code',
    seed_qty_a='pp_s4q11b_a', seed_qty_b='pp_s4q11b_b',
    improved_code='pp_s4q11', improved_yes=(2,),

    # seed purchase from §5 (holder-crop grain)
    p_holder_id='holder_id', p_crop_code='crop_code',
    p_purch_flag='pp_s5q03', p_purch_a='pp_s5q05_a', p_purch_b='pp_s5q05_b',

    # inorganic fertilizer (§3): Urea / DAP / NPS
    urea_used='pp_s3q15', urea_kg='pp_s3q16', urea_purch_kg='pp_s3q16c',
    dap_used='pp_s3q18', dap_kg='pp_s3q19', dap_purch_kg='pp_s3q19c',
    nps_used='pp_s3q20a_1', nps_kg='pp_s3q20a_2', nps_purch_kg='pp_s3q20a_4',
    # organic (§3)
    manure_used='pp_s3q21', compost_used='pp_s3q23', other_organic_used='pp_s3q25',

    # pesticide (§4)
    pest_crop_code='crop_code', pest_used='pp_s4q05', pest_gate='pp_s4q04',
)

df = plot_inputs_for_wave('2015-16', plot_roster, planting, seeds, colmap)

assert len(df) > 0, "plot_inputs 2015-16 produced no rows"
to_parquet(df, 'plot_inputs.parquet')
