"""months_food_inadequate for Malawi IHPS 2013-14 (GH #332).

Module H (HH_MOD_H_13.dta) H04 (hh_h04: 1=Yes / 2=No) gates the wide H05
month calendar (hh_h05a..hh_h05s, 'X' = month the HH lacked food).
MonthsInadequate = count of 'X' for H04==Yes households (0 else);
AnyInadequate = (H04==Yes).  Household id is the IHPS y2_hhid; the
framework's id_walk converts it at API time (as for food_security).

See lsms_library/countries/Malawi/_/malawi.py:months_food_inadequate_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import months_food_inadequate_for_wave


WAVE = '2013-14'

df = get_dataframe('../Data/HH_MOD_H_13.dta', convert_categoricals=False)

out = months_food_inadequate_for_wave(WAVE, df, idcol='y2_hhid', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i) in months_food_inadequate {WAVE}"
assert len(out) > 0, f"months_food_inadequate {WAVE} produced no rows"

to_parquet(out, 'months_food_inadequate.parquet')
