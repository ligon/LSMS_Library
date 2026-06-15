from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import sys
sys.path.append('../../_/')
import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile
from fct_tools import nutrient_df, harmonize_nutrient, fct_filter
import pandas as pd
import numpy as np


#retrieve org tables 
fct_origin = df_from_orgfile(orgfn= '../../Tanzania/_/demands.org', name = 'fct_origin')
# Unit #0 (2026-06-14): the food-item label table migrated from the
# standalone food_items.org into categorical_mapping.org as
# #+name: harmonize_food (carrying the FTC Code / FDC ID nutrient keys).
food = df_from_orgfile(orgfn= 'categorical_mapping.org', name='harmonize_food')
food = food.astype({'FTC Code': 'Int64', 'FDC ID' : 'Int64'})
n_labels = df_from_orgfile(orgfn= 'nutrient_labels.org')

##--Part 1: process foods that are existent in the given Tanzania fct 
fct = fct_filter(food, n_labels, fct_origin)



##--Part 2: process foods that are non-existent in the given Tanzania fct 
apikey = "hAkb5LsLAS1capOD60K6ILrZDkC29eK6ZmqCumXB"
#create and restructure fct for fdc food items; 
fct_add = food[["Preferred Label", "FDC ID"]]
fct_add = nutrient_df(fct_add, apikey)
fct_add = harmonize_nutrient(fct_add, n_labels)
#combine two fcts 
final_fct = pd.concat([fct, fct_add]).sort_index().T


##--Part 3: multiply consumption quantities to get the aggregate nutrition consumption
# food_quantities is now derived at runtime from food_acquired
# (_FOOD_DERIVED in country.py); mirror Uganda's nutrition.py and pull
# it via the Country API instead of the retired
# ../var/food_quantities.parquet (built by the removed pre-Phase-3
# food_prices_quantities_and_expenditures.py).  The derived table has a
# single Quantity column and canonical index (t, [v], i, j, u, s) where
# i is the household, j the food item; u='kg' tags kg-converted rows.
eth = ll.Country('Ethiopia')
q = eth.food_quantities()

#sum all quantities — keep only kg-converted rows (case-insensitive 'kg'
# to tolerate legacy u='Kg' and Phase-4 u='kg'), drop the unit level and
# collapse the acquisition-source axis.
mask = q.index.get_level_values('u').astype(str).str.lower() == 'kg'
q = q[mask].droplevel('u').sum(axis=1)

# Deal with any dupes (sum over s and any remaining levels).
q = q.groupby(['i','t','j']).sum()

# final_q: rows (i household, t), columns j (food item); final_fct.columns
# are food Preferred Labels, matching the renamed j level.
final_q = q.unstack('j')

#cross-filter two dfs to align matrices; replace NaN values with 0
list1 = final_q.columns.values.tolist()
list2 = final_fct.columns.values.tolist()
final_q = final_q.filter(items=list2).replace(np.nan,0)
final_fct = final_fct.filter(items=list1).replace(np.nan,0)

n = final_q@final_fct.T
to_parquet(n, '../var/nutrition.parquet')
