#!/usr/bin/env python
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets
import sys
sys.path.append('../../_/')
from local_tools import df_from_orgfile

fct = read_sheets('https://docs.google.com/spreadsheets/d/1qljY2xrxbc37d9tLSyuFa9CnjEsh3Re2ufDQlBHzPEQ/')['FCT'].loc[3:]
q = pd.read_parquet('../var/food_quantities.parquet')
b=q
q['q_sum'] = q.sum(axis=1)
q = q[['q_sum']].droplevel('u').reset_index()
final_q = q.pivot_table(index = ['j','t','m'], columns = 'i', values = 'q_sum').fillna(0)

#find FCT codes for foods
food_items = df_from_orgfile('./food_items.org')
food_items['FCT code'] = food_items['FCT code'].astype('Int64').astype(str)
food_labels = {}
food_labels = food_items[['Preferred Label', 'FCT code']].set_index('Preferred Label').to_dict('dict')

x = pd.Series(list(final_q.columns)).replace(food_labels['FCT code']).replace('<NA>', np.nan).dropna()

fct.columns.values[0] = 'Code'
final_fct = pd.DataFrame(columns=fct.columns)

counter = 0
for i in x:
    final_fct.loc[counter] = fct.loc[fct['Code'] == i].values[0]
    counter += 1

#drop items with no mataching FCT
food_drop = [k for k,v in food_labels['FCT code'].items() if v == '<NA>']
final_q = final_q.drop(columns = food_drop)

final_fct.columns = final_fct.columns.str.replace('\\n%*', '', regex=True)
final_fct['Food'] = list(final_q.columns)
final_fct = final_fct.drop(['Nutrient', 'Code'], axis = 1).fillna(0).set_index('Food')
final_fct = final_fct.replace('', 0)

for column in final_fct.columns:
    final_fct[column] = final_fct[column].astype(float)

final_fct.to_parquet('../var/fct.parquet')

n = final_q@final_fct
n.to_parquet('../var/nutrition.parquet')
