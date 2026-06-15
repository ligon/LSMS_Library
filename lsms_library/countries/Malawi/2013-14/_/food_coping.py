"""Build food_coping (rCSI day counts) for Malawi IHPS 2013-14 (GH #332).

Module H (HH_MOD_H_13.dta) carries the 5 rCSI coping strategies in
hh_h02a..hh_h02e (days 0-7 in the past 7).  Items are explicitly labelled
in this wave (a=LessPreferred, b=LimitPortion, c=ReduceMeals,
d=RestrictAdults, e=BorrowFood).  Household id is the IHPS y2_hhid; the
framework's id_walk (panel_ids) converts it to the canonical i at API
time, exactly as for food_security / shocks in this wave.

See lsms_library/countries/Malawi/_/malawi.py:food_coping_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import food_coping_for_wave


WAVE = '2013-14'

df = get_dataframe('../Data/HH_MOD_H_13.dta', convert_categoricals=False)

out = food_coping_for_wave(WAVE, df, idcol='y2_hhid', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i,Strategy) in food_coping {WAVE}"
assert len(out) > 0, f"food_coping {WAVE} produced no rows"

to_parquet(out, 'food_coping.parquet')
