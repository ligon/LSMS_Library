#!/usr/bin/env python
"""Build plot_inputs for Ethiopia ESS 2018-19 (Wave 4; GAP 2).

Item-level ag inputs at (t, i, plot_id, input, j).

W4 renamed the pp variables (dropped the ``pp_`` prefix) and restructured
§3 fertilizer.  Sources / specifics:
  - Seed quantity (§4 sect4_pp_w4): kg = s4q11a, crop code s4q01b,
    improved s4q11 (1=No, 2/4=Yes); the .do drops s4q15==1 (immature /
    cut-green rows) from the seed block.
  - Seed purchase (§5 sect5_pp_w4): s5q02 flag + s5q04 kg, crop s5q0B,
    at (HOLDER, crop) grain -> attached unambiguously (one plot only).
  - Fertilizer (§3 sect3_pp_w4): UREA used s3q21, kg s3q21a, purchased
    s3q21c; DAP used s3q22, kg s3q22a, purchased s3q22c; NPS used s3q23,
    kg s3q23a, purchased s3q23c.
  - Organic (§3): manure s3q25 / compost s3q26 / other s3q27.
  - Pesticide (§4): s4q05 used, gated by s4q04==2.
i = household_id (matches sample().i for W4 -- entirely new sample).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_inputs_for_wave


plot_roster = get_dataframe('../Data/sect3_pp_w4.dta', convert_categoricals=False)
planting    = get_dataframe('../Data/sect4_pp_w4.dta', convert_categoricals=False)
seeds       = get_dataframe('../Data/sect5_pp_w4.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',

    # seed quantity from §4 (kg); improved from §4; drop s4q15==1
    seed_src='planting', seed_crop_code='s4q01b',
    seed_qty='s4q11a', seed_drop_col='s4q15', seed_drop_val=1,
    improved_code='s4q11', improved_yes=(2, 3, 4),

    # seed purchase from §5 (holder-crop grain)
    p_holder_id='holder_id', p_crop_code='s5q0B',
    p_purch_flag='s5q02', p_purch_qty='s5q04',

    # inorganic fertilizer (§3): Urea / DAP / NPS, total + purchased kg
    urea_used='s3q21', urea_kg='s3q21a', urea_purch_kg='s3q21c',
    dap_used='s3q22', dap_kg='s3q22a', dap_purch_kg='s3q22c',
    nps_used='s3q23', nps_kg='s3q23a', nps_purch_kg='s3q23c',
    # organic (§3)
    manure_used='s3q25', compost_used='s3q26', other_organic_used='s3q27',

    # pesticide (§4)
    pest_crop_code='s4q01b', pest_used='s4q05', pest_gate='s4q04',
)

df = plot_inputs_for_wave('2018-19', plot_roster, planting, seeds, colmap)

assert len(df) > 0, "plot_inputs 2018-19 produced no rows"
to_parquet(df, 'plot_inputs.parquet')
