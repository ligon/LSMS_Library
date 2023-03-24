"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets

fct = read_sheets('https://docs.google.com/spreadsheets/d/1qljY2xrxbc37d9tLSyuFa9CnjEsh3Re2ufDQlBHzPEQ/')['FCT'].loc[3:]
q = pd.read_parquet('../var/food_quantities.parquet')
b=q
q['q_sum'] = q.sum(axis=1)
q = q[['q_sum']].droplevel('u').reset_index()
final_q = q.pivot_table(index = ['j','t','m'], columns = 'i', values = 'q_sum').fillna(0)

#find FCT codes for foods
food_items = pd.read_csv('../_/food_items.org', sep='|', skipinitialspace=True, converters={1:lambda s: s.strip()})
food_items.columns = [s.strip() for s in food_items.columns]
food_items = food_items[['Preferred Label', 'FCT code']].dropna()
food_items['FCT code'] = food_items['FCT code'].astype('int').astype('str')
food_items = dict(zip(food_items['Preferred Label'], food_items['FCT code']))

x = pd.Series(list(final_q.columns)).map(food_items)

fct.columns.values[0] = 'Code'
final_fct = pd.DataFrame(columns=fct.columns)

counter = 0
for i in x:
    final_fct.loc[counter] = fct.loc[fct['Code'] == i].values[0]
    counter += 1

final_fct = final_fct.replace(r'^\s*$', np.nan, regex=True)
final_fct['Food'] = list(final_q.columns)
final_fct = final_fct.drop(['Nutrient', 'Code'], axis = 1).fillna(0).set_index('Food')

for column in final_fct.columns:
    final_fct[column] = final_fct[column].astype(float)

final_fct.to_parquet('../var/fct.parquet')

n = final_q@final_fct
n.to_parquet('../var/nutrition.parquet')
