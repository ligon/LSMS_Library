#!/usr/bin/env python
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets

fct = read_sheets('https://docs.google.com/spreadsheets/d/1qljY2xrxbc37d9tLSyuFa9CnjEsh3Re2ufDQlBHzPEQ/')['FCT'].loc[3:]
q = pd.read_parquet('../var/food_quantities.parquet')

q['q_sum'] = q.sum(axis=1)
q = q[['q_sum']].droplevel('u').reset_index()
final_q = q.pivot_table(index = ['j','t','m'], columns = 'i', values = 'q_sum').fillna(0).drop(['Arroz(Todos)', 'Cigarrillo Y Tabaco', 'Otros', 'Queso (Balnco Y Amarillo)'], axis = 1)
# missing fct information

# find FCT codes for foods
food_items = pd.read_csv('../_/food_items.org', sep='|', skipinitialspace=True, converters={1:lambda s: s.strip()})
food_items.columns = [s.strip() for s in food_items.columns]
food_items = food_items[['Preferred Label', 'FCT ID']].dropna()
food_items = food_items[food_items['FCT ID'] != '--- ']
food_items['FCT ID'] = food_items['FCT ID'].astype('int').astype('str')

fct = fct.rename(columns = {'': 'FCT ID'})

final_fct = food_items.merge(fct, on='FCT ID')
final_fct = final_fct.drop(['Nutrient', 'FCT ID'], axis = 1).fillna(0).rename(columns={'Preferred Label': 'Food'})
final_fct.columns = final_fct.columns.str.replace('\\n%*', '', regex=True)
final_fct = final_fct.replace('', np.nan).drop_duplicates(subset='Food', keep='first').set_index('Food').sort_index()

for column in final_fct.columns:
    final_fct[column] = final_fct[column].astype(float)

final_fct.to_parquet('../var/fct.parquet')

n = final_q@final_fct
n.to_parquet('../var/nutrition.parquet')
