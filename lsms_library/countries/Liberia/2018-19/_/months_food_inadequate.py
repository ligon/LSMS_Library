import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet

# Liberia 2018-19 §16 'Food security' (sect16a_public.dta).
#   S16_1 = "in the last 12 months, have you been faced with a situation when
#           you did not have enough food to feed the household?" (yes/no).
#   S16_2 = "how many months in the past 12 months did you not have enough food
#           to feed the household?" (count 1-12; only asked when S16_1=='yes',
#           so NaN for the 'no' households).
#   S16_3/S16_4 are forest-product coping follow-ups (which months / how
#           important were wild products) -- NOT a which-months battery, so the
#           count item S16_2 is the source.  sect16b_public.dta is a long
#           forest-product roster, unrelated to provisioning adequacy.
# MonthsInadequate = S16_2, with the 'no' households filled to 0 (they were
#           skipped out of S16_2 precisely because they had no inadequate
#           months).  S16_1 NaN (11 HH, question not asked) -> MonthsInadequate
#           NaN.
# AnyInadequate    = S16_1=='yes'.
# i=hhid (matches household_roster / sample 100%); v is NOT baked in -- it is
#           joined from sample() at API time by _join_v_from_sample.

df = get_dataframe('../Data/Household/sect16a_public.dta')

s1 = df['S16_1'].astype(str).str.strip().str.lower()
any_inadequate = s1.map({'yes': True, 'no': False})  # NaN where not asked

months = pd.to_numeric(df['S16_2'], errors='coerce')
# 'no' households were skipped out of S16_2 -> 0 inadequate months.
months = months.where(any_inadequate != False, other=0)

out = pd.DataFrame(index=df.index)
out['i'] = df['hhid'].astype('int64').astype(str)
out['t'] = '2018-19'
out['MonthsInadequate'] = months.astype('Int64')
out['AnyInadequate'] = any_inadequate.astype('boolean')

out = out.set_index(['t', 'i'])

to_parquet(out, 'months_food_inadequate.parquet')
