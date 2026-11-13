from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import sys
sys.path.append('../../_/')
import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile
from lsms_library.country import Country as _Country
from lsms_library.transformations import food_quantities_from_acquired
from fct_tools import fct_filter
import pandas as pd
import numpy as np

# GH #1509 (@ligon 2026-11-12, "Make the dependency visible").  Part 3 below
# reads ``eth.food_quantities()``, which the Country API derives at read time
# through ``food_quantities_from_acquired`` and its kilogram ladder
# (``food_kg_factors``).  A call through the Country API is invisible to
# ``framework_imports_fingerprint``, so a kg-factor edit used to leave a warm
# ``var/nutrition.parquet`` serving the old numbers.  The import above folds
# the transform's closure into every Ethiopia cache hash; this assertion keeps
# it honest: if the API stops deriving ``food_quantities`` through it, the
# build fails rather than the hash tracking the wrong code.
assert _Country._FOOD_DERIVED['food_quantities'] == \
    food_quantities_from_acquired.__name__, (
        'Ethiopia nutrition.py imports food_quantities_from_acquired to version '
        'the code behind food_quantities(); the API now derives it from '
        f"{_Country._FOOD_DERIVED['food_quantities']!r}: import that instead")


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
#create and restructure fct for fdc food items; 
# Stored densities are already per kg: do not harmonize/scale twice.
try:
    fct_add = pd.read_csv('fct_usda.csv', index_col=0, float_precision='round_trip')
except FileNotFoundError as exc:
    raise FileNotFoundError(
        'Missing versioned Ethiopia FCT snapshot; run '
        'python scripts/refresh_fct.py --country Ethiopia explicitly'
    ) from exc
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
