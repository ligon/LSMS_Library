"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import sys
sys.path.append('../../_/')
from local_tools import df_from_orgfile
from fct_tools import nutrient_df, harmonize_nutrient, fct_filter
import pandas as pd
import numpy as np


#retrieve org tables 
fct_origin = df_from_orgfile(orgfn= '../../Tanzania/_/demands.org', name = 'fct_origin')
food = df_from_orgfile(orgfn= 'food_items.org')
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
#sum all quantities 
q = pd.read_parquet('../var/food_quantities.parquet')
q['q_sum'] = q.sum(axis=1)
#q = q[['q_sum']].droplevel('units').reset_index()
q = q[['q_sum']].reset_index()

final_q = q.pivot_table(index = ['j','t','m'], columns = 'i', values = 'q_sum')

#cross-filter two dfs to align matrices; replace NaN values with 0 
list1 = final_q.columns.values.tolist()
list2 = final_fct.columns.values.tolist()
final_q = final_q.filter(items=list2).replace(np.nan,0)
final_fct = final_fct.filter(items=list1).replace(np.nan,0)

n = final_q@final_fct.T
n.to_parquet('../var/nutrition.parquet')
