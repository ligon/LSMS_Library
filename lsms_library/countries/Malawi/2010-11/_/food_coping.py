"""Build food_coping (rCSI day counts) for Malawi IHS3 2010-11 (GH #332).

Module H of the IHS3 household questionnaire carries the 5 rCSI coping
strategies in hh_h02a..hh_h02e (days 0-7 in the past 7).  The 2010-11
.dta Stata labels are truncated to the question stem; the strategy order
(a=LessPreferred, b=LimitPortion, c=ReduceMeals, d=RestrictAdults,
e=BorrowFood) is taken from the IHS3 Household Questionnaire Module H,
Page 35, and matches the explicitly-labelled later waves.

See lsms_library/countries/Malawi/_/malawi.py:food_coping_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import food_coping_for_wave


WAVE = '2010-11'

# convert_categoricals=False keeps the integer day counts.
df = get_dataframe('../Data/Full_Sample/Household/hh_mod_h.dta',
                   convert_categoricals=False)

out = food_coping_for_wave(WAVE, df, idcol='case_id', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i,Strategy) in food_coping {WAVE}"
assert len(out) > 0, f"food_coping {WAVE} produced no rows"

to_parquet(out, 'food_coping.parquet')
