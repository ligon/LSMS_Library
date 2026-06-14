#!/usr/bin/env python
"""Build plot_inputs for Ethiopia ESS 2011-12 (Wave 1; GAP 2).

Item-level ag inputs at (t, i, plot_id, input, j).  One row per input
applied to a plot, carrying only REPORTED fields.

W1 sources / specifics:
  - Seed (§5 sect5_pp_w1): per (field, crop); the seed quantity is
    reported in kg (pp_s5q19_a) + grams residual (pp_s5q19_b).  §5 ALSO
    carries field_id, so purchased detail (pp_s5q03 flag, pp_s5q05_a/b
    kg+gram) attaches directly at plot-crop grain.  The improved-seed
    flag is NOT in §5 -> joined from §4 (pp_s4q11: 1/3=No, 2=Yes).
  - Fertilizer / organic (§3 sect3_pp_w1): Urea kg = pp_s3q16_c, DAP kg
    = pp_s3q19_c (the .do reads the _c kg-form directly), used flags
    pp_s3q15 / pp_s3q18; organic dummies pp_s3q21 (manure) / pp_s3q23
    (compost) / pp_s3q25 (other).  No NPS in W1.
  - Pesticide (§4 sect4_pp_w1): pp_s4q05 used, gated by pp_s4q04==2.
i = household_id (matches sample().i for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_inputs_for_wave


plot_roster = get_dataframe('../Data/sect3_pp_w1.dta', convert_categoricals=False)
planting    = get_dataframe('../Data/sect4_pp_w1.dta', convert_categoricals=False)
seeds       = get_dataframe('../Data/sect5_pp_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',

    # seed quantity from §5 (field-crop grain), kg + grams residual
    seed_src='seeds', seed_crop_code='crop_code',
    seed_qty_a='pp_s5q19_a', seed_qty_b='pp_s5q19_b',
    # improved flag lives in §4; 1/3 = No, 2 = Yes
    improved_join=True, improved_code='pp_s4q11', pl_crop_code='crop_code',
    improved_yes=(2,),

    # seed purchase from §5 (field-crop grain): flag + kg + grams
    p_holder_id='holder_id', p_crop_code='crop_code',
    p_field_id='field_id', p_parcel_id='parcel_id',
    p_purch_flag='pp_s5q03', p_purch_a='pp_s5q05_a', p_purch_b='pp_s5q05_b',

    # inorganic fertilizer (§3): used flag + kg-form (_c)
    urea_used='pp_s3q15', urea_kg='pp_s3q16_c',
    dap_used='pp_s3q18', dap_kg='pp_s3q19_c',
    # no NPS in W1
    # organic (§3): use dummies
    manure_used='pp_s3q21', compost_used='pp_s3q23', other_organic_used='pp_s3q25',

    # pesticide (§4): used + gate
    pest_crop_code='crop_code', pest_used='pp_s4q05', pest_gate='pp_s4q04',
)

df = plot_inputs_for_wave('2011-12', plot_roster, planting, seeds, colmap)

assert len(df) > 0, "plot_inputs 2011-12 produced no rows"
to_parquet(df, 'plot_inputs.parquet')
