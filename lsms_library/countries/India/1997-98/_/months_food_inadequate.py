import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet

# India 1997-98 §8 VULNERABILITY Part A 'Food security' (SECT08A.DTA).
#   v08a01      = "Got 2 square meals" (Yes/No), overall adequacy in last 12 months.
#   v08a02a..l  = January..December; marked 1 for each month in which the HH could
#                 NOT get 2 square meals.  NaN otherwise.
# MonthsInadequate = count of flagged months (0-12).
# AnyInadequate    = HH reported NOT getting 2 square meals (v08a01=='No') OR any
#                    month flagged (the two are mostly but not perfectly consistent:
#                    20 'No' HHs flagged 0 months; 5 'Yes' HHs flagged >=1 month).
# i=hhcode (matches sample() 100%; the roster's i:hh is a latent bug -> use hhcode,
#   consistent with housing/individual_education/interview_date).  v is NOT baked in;
#   it is joined from sample() at API time by _join_v_from_sample.

df = get_dataframe('../Data/SECT08A.DTA')

month_cols = [f'v08a02{c}' for c in 'abcdefghijkl']

out = pd.DataFrame(index=df.index)
out['i'] = df['hhcode'].astype(int).astype(str)
out['t'] = '1997-98'

months = df[month_cols].notna().sum(axis=1).astype('Int64')

# v08a01 is categorical Yes/No; treat 'No' as inadequate.
got_meals = df['v08a01'].astype(str).str.strip()
any_inadequate = (got_meals == 'No') | (months > 0)

out['MonthsInadequate'] = months
out['AnyInadequate'] = any_inadequate.astype(bool)

out = out.set_index(['t', 'i'])

to_parquet(out, 'months_food_inadequate.parquet')
