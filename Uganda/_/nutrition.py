#!/usr/bin/env python
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import numpy as np
from fct_addition import nutrient_df, harmonize_nutrient
from lsms_library.local_tools import df_from_orgfile

"""
Create a nutrition DataFrame for households based on food consumption quantities
"""


uga = ll.Country('Uganda')
q = uga.food_quantities()

apikey = "hAkb5LsLAS1capOD60K6ILrZDkC29eK6ZmqCumXB"
fct_add = df_from_orgfile('nutrition.org',name='fct_addition',encoding='ISO-8859-1')
#fct = df_from_orgfile('nutrition.org',name='fct',encoding='ISO-8859-1').set_index('FCT Code')
#fct.index.name = 'i'
fct = pd.read_csv('fct_uganda.csv').set_index('i')  # Build in nutrition.org

#create and restructure fct for fdc food items;
try:
    fct_usda = pd.read_csv('fct_usda.csv')
except FileNotFoundError:
    n1 = nutrient_df(fct_add, apikey,verbose=True)
    fct_usda = harmonize_nutrient(n1)
    if type(fct_usda.index)==pd.RangeIndex:
        fct_usda = fct_usda.set_index(fct_usda.columns[0])
        fct_usda.index.name = 'i'
    fct_usda.to_csv('fct_usda.csv')

#combine two fcts
final_fct = pd.concat([fct, fct_usda]).sort_index().T

#sum all quantities
q = q.xs('Kg',level='u').sum(axis=1)

# Deal with any dupes
q = q.groupby(['i','t','m','j']).sum()

common_cols = q.index.unique(level='j').intersection(final_fct.columns)

final_q = q.unstack('j').loc[:,common_cols].replace(np.nan,0)
final_fct = final_fct.loc[:,common_cols].replace(np.nan,0)
final_fct.index.name = 'Nutrient'

to_parquet(final_fct, '../var/fct.parquet')

n = final_q@final_fct.T

n.index.name = 'Nutrient'
to_parquet(n, '../var/nutrition.parquet')
