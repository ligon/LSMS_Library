"""months_food_inadequate for Malawi IHS3 2010-11 (GH #332).

Module H question H04 (hh_h04: 1=Yes / 2=No, faced a food-shortage
situation in the last 12 months) gates the wide H05 month calendar
(hh_h05a_01..hh_h05b_15, one cell per month, 'X' = month the HH lacked
food).  MonthsInadequate = count of 'X' for H04==Yes households (0 else);
AnyInadequate = (H04==Yes).

See lsms_library/countries/Malawi/_/malawi.py:months_food_inadequate_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import months_food_inadequate_for_wave


WAVE = '2010-11'

df = get_dataframe('../Data/Full_Sample/Household/hh_mod_h.dta',
                   convert_categoricals=False)

out = months_food_inadequate_for_wave(WAVE, df, idcol='case_id', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i) in months_food_inadequate {WAVE}"
assert len(out) > 0, f"months_food_inadequate {WAVE} produced no rows"

to_parquet(out, 'months_food_inadequate.parquet')
