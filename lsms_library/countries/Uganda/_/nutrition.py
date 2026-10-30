#!/usr/bin/env python
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import numpy as np
from lsms_library.local_tools import df_from_orgfile

"""
Create a nutrition DataFrame for households based on food consumption quantities
"""


# GH #1062; see .coder/ledger/1062-uganda-sibling-writes.md sections 2/4.
# The framework stamps only the requested table, so never write its sibling.
target = sys.argv[1] if len(sys.argv) > 1 else 'nutrition'
if target not in {'fct', 'nutrition'}:
    raise ValueError("Expected target 'fct' or 'nutrition'")


#fct = df_from_orgfile('nutrition.org',name='fct',encoding='ISO-8859-1').set_index('FCT Code')
#fct.index.name = 'i'
fct = pd.read_csv('fct_uganda.csv').set_index('i')  # Build in nutrition.org

#create and restructure fct for fdc food items;
try:
    fct_usda = pd.read_csv('fct_usda.csv')
    if type(fct_usda.index)==pd.RangeIndex:
        fct_usda = fct_usda.set_index(fct_usda.columns[0])
        fct_usda.index.name = 'i'

except FileNotFoundError as exc:
    raise FileNotFoundError(
        'Missing versioned Uganda FCT snapshot; run '
        'python scripts/refresh_fct.py --country Uganda explicitly'
    ) from exc

#combine two fcts
uga = ll.Country('Uganda')
q = uga.food_quantities()

final_fct = pd.concat([fct, fct_usda]).sort_index().T

# But only keep columns common to both
fct_cols = fct.columns.intersection(fct_usda.columns)
final_fct = final_fct.loc[fct_cols]

#sum all quantities — case-insensitive 'kg' to handle both
# legacy parquets (u='Kg') and Phase-4 derived ones (u='kg').
mask = q.index.get_level_values('u').astype(str).str.lower() == 'kg'
q = q[mask].droplevel('u').sum(axis=1)

# Deal with any dupes
q = q.groupby(['i','t','j']).sum()

common_cols = q.index.unique(level='j').intersection(final_fct.columns)

final_q = q.unstack('j').loc[:,common_cols].replace(np.nan,0)
final_fct = final_fct.loc[:,common_cols].replace(np.nan,0)
final_fct.index.name = 'Nutrient'

if target == 'fct':
    to_parquet(final_fct, '../var/fct.parquet')

if target == 'nutrition':
    n = final_q@final_fct.T

    n.index.name = 'Nutrient'
    to_parquet(n, '../var/nutrition.parquet')
