"""Build food_coping (rCSI day counts) for Malawi IHS4 2016-17 (GH #332).

Cross-sectional Module H (Cross_Sectional/hh_mod_h.dta) carries the 5
rCSI coping strategies in hh_h02a..hh_h02e (days 0-7 in the past 7),
explicitly labelled (a=LessPreferred, b=LimitPortion, c=ReduceMeals,
d=RestrictAdults, e=BorrowFood).  The cross-sectional household id
(case_id) is prefixed 'cs-17-' to match this wave's household_roster i
(the roster prefixes cross-sectional cases via mapping.cs_i).

See lsms_library/countries/Malawi/_/malawi.py:food_coping_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import food_coping_for_wave


WAVE = '2016-17'

df = get_dataframe('../Data/Cross_Sectional/hh_mod_h.dta',
                   convert_categoricals=False)

out = food_coping_for_wave(WAVE, df, idcol='case_id', i_prefix='cs-17-')

assert out.index.is_unique, f"Non-unique (t,i,Strategy) in food_coping {WAVE}"
assert len(out) > 0, f"food_coping {WAVE} produced no rows"

to_parquet(out, 'food_coping.parquet')
