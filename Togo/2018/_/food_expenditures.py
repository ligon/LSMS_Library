#!/usr/bin/env python
import sys
sys.path.append('../../_')
from togo import food_expenditures
import numpy as np
import pandas as pd
import json

food = pd.read_stata('../Data/Togo_survey2018_fooditems_forEthan.dta')
food = food.rename(columns={'hhid':'j','food_item':'i'})

x=food[['j','i','item_value']]
x['j'] = x['j'].astype(int).astype(str)

x = x.set_index(['j','i']).squeeze()

x = x.unstack('i')

labels = json.load(open('food_items.json'))

x = x.rename(columns=labels)
x = x.groupby('i',axis=1).sum()

x = x.replace(0,np.nan)

x = x.iloc[:,2:]

x.to_parquet('food_expenditures2.parquet')


