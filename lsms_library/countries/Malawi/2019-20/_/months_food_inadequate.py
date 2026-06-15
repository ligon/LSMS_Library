"""months_food_inadequate for Malawi IHS5 2019-20 (GH #332).

Cross-sectional Module H (Cross_Sectional/HH_MOD_H.dta) H04 (hh_h04:
1=Yes / 2=No) gates the wide H05 month calendar (hh_h05a..hh_h05y).
As in IHS4, every H04==No household has all 25 cells pre-filled with 'X';
gating the count on H04==Yes is essential.  case_id matches the wave's
roster i directly (no prefix).

See lsms_library/countries/Malawi/_/malawi.py:months_food_inadequate_for_wave.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from malawi import months_food_inadequate_for_wave


WAVE = '2019-20'

df = get_dataframe('../Data/Cross_Sectional/HH_MOD_H.dta',
                   convert_categoricals=False)

out = months_food_inadequate_for_wave(WAVE, df, idcol='case_id', i_prefix='')

assert out.index.is_unique, f"Non-unique (t,i) in months_food_inadequate {WAVE}"
assert len(out) > 0, f"months_food_inadequate {WAVE} produced no rows"

to_parquet(out, 'months_food_inadequate.parquet')
