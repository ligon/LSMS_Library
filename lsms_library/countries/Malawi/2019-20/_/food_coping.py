"""Build food_coping (rCSI day counts) for Malawi IHS5 2019-20 (GH #332).

Cross-sectional Module H (Cross_Sectional/HH_MOD_H.dta) carries the 5
rCSI coping strategies in hh_h02a..hh_h02e (days 0-7 in the past 7),
explicitly labelled (a=LessPreferred, b=LimitPortion, c=ReduceMeals,
d=RestrictAdults, e=BorrowFood).  The cross-sectional case_id matches
this wave's household_roster i directly (no prefix).  A lone hh_h02a=20
data-entry value is dropped by food_coping_for_wave's 0-7 clamp.

See lsms_library/countries/Malawi/_/malawi.py:food_coping_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import food_coping_for_wave


WAVE = '2019-20'

df = get_dataframe('../Data/Cross_Sectional/HH_MOD_H.dta',
                   convert_categoricals=False)

out = food_coping_for_wave(WAVE, df, idcol='case_id', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i,Strategy) in food_coping {WAVE}"
assert len(out) > 0, f"food_coping {WAVE} produced no rows"

to_parquet(out, 'food_coping.parquet')
