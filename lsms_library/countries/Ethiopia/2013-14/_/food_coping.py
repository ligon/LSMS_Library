#!/usr/bin/env python
"""Build food_coping for Ethiopia ESS 2013-14 (Wave 2; GH #332, Family B).

Source: sect7_hh_w2.dta (Section 7, "Food Security"), question
hh_s7q02_{a..h} = "In the past 7 days, how many days have you or someone in
your HH had to: [strategy]?" (0-7, the 8-item Coping Strategies Index).

i = household_id2 (matches household_roster / sample for W2).  Long form
index (t, i, Strategy); column Days.  See ethiopia.food_coping_for_wave.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import food_coping_for_wave


df = get_dataframe('../Data/sect7_hh_w2.dta', convert_categoricals=False)

out = food_coping_for_wave('2013-14', df, 'household_id2')

assert out.index.is_unique, "Non-unique (t, i, Strategy) index in food_coping 2013-14"
assert len(out) > 0, "food_coping 2013-14 produced no rows"

to_parquet(out, 'food_coping.parquet')
