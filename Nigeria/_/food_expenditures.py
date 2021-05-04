import pandas as pd
import json
import numpy as np

X = []
for t in ['2010-11','2012-13','2015-16','2018-19']:
    X.append(pd.read_parquet('../%s/_/food_expenditures.parquet' % t))

x = pd.concat(X,axis=0)

# Eliminate infinities
x = x.replace(np.inf,np.nan)

with open('aggregate_items.json') as f:
    lbl = json.load(f)

x = x.rename(columns=lbl['Aggregated Label'])

x.columns.name = 'i'
x = x.groupby('i',axis=1).sum()

x.to_parquet('./food_expenditures.parquet')
