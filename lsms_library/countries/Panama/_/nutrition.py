#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
from lsms_library.local_tools import df_from_orgfile
"""
Create a nutrition DataFrame for households based on food consumption quantities

NOT REACHABLE as of 2026-09-06, for three reasons that all predate GH #783 and
are NOT fixed by it:

  1. `nutrition` is not registered in _/data_scheme.yml, so
     Country('Panama').nutrition() does not exist;
  2. the _/Makefile's `var` target is food_acquired.parquet only, so
     ../var/nutrition.parquet is never requested;
  3. line 14 reads ../var/food_quantities.parquet, which the Makefile's own
     comment forbids materializing (food_quantities is auto-derived at API
     time by _FOOD_DERIVED), and `eep153_tools` (imported below) is not an
     installed dependency -- so this module raises ModuleNotFoundError on
     import before reaching any of the above.

GH #783 repointed its food-label read off the retired food_items.org so the
reference is not dangling; it did not revive the script.
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets
 
#fct = read_sheets('https://docs.google.com/spreadsheets/d/1qljY2xrxbc37d9tLSyuFa9CnjEsh3Re2ufDQlBHzPEQ/')['FCT'].loc[3:]
fct = pd.read_csv('central_american_fct - FCT.csv').loc[3:]
q = get_dataframe('../var/food_quantities.parquet')

q['q_sum'] = q.sum(axis=1)
q = q[['q_sum']].droplevel('u').reset_index()
final_q = q.pivot_table(index = ['j','t'], columns = 'i', values = 'q_sum').fillna(0).drop(['Cigarrillo Y Tabaco', 'Otros'], axis = 1)
# missing fct information

# find FCT codes for foods
# GH #783: was a raw pd.read_csv(sep='|') over the standalone _/food_items.org,
# which is retired.  The vocabulary is now the `harmonize_food` table in
# _/categorical_mapping.org; df_from_orgfile nulls '---' cells for us, so the
# explicit '--- ' filter that the raw parse needed is gone.  Verified to yield
# the SAME (Preferred Label, FCT ID) pairs the raw parse produced.
food_items = df_from_orgfile('../_/categorical_mapping.org', name='harmonize_food')
food_items = food_items[['Preferred Label', 'FCT ID']].dropna()
food_items['FCT ID'] = food_items['FCT ID'].astype('int').astype('str')

fct = fct.rename(columns = {fct.columns[0]: 'FCT ID'})

final_fct = food_items.merge(fct, on='FCT ID')
final_fct = final_fct.drop(['Nutrient', 'FCT ID'], axis = 1).fillna(0).rename(columns={'Preferred Label': 'Food'})
final_fct.columns = final_fct.columns.str.replace('\\n%*', '', regex=True)
final_fct = final_fct.replace('', pd.NA).drop_duplicates(subset='Food', keep='first').set_index('Food').sort_index()

for column in final_fct.columns:
    final_fct[column] = final_fct[column].astype(float)

to_parquet(final_fct, '../var/fct.parquet')

print("Unmatched foods: ",final_q.columns.difference(final_fct.index))

final_q = final_q.mask(~np.isfinite(final_q),0)

n = final_q.reindex(columns=final_fct.index)@final_fct
to_parquet(n, '../var/nutrition.parquet')
