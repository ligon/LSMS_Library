#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""
Read non-food expenditures; use harmonized non-food labels.
"""
import pandas as pd
import numpy as np
from uganda import Waves, harmonized_food_labels, id_walk
import json

x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = get_dataframe('../'+t+'/_/nonfood_expenditures.parquet')
    x[t] = x[t].stack('i').dropna()
    x[t] = x[t].reset_index().set_index(['j','i']).squeeze()
    x[t] = x[t].replace(0,np.nan).dropna()

df = pd.DataFrame(x)
df.columns.name = 't'

x = df.stack().unstack('i')

agg_labels = harmonized_food_labels(fn='./nonfood_items.org',
                                    key='Preferred Label',
                                    value='Aggregate Label')
#x = x.rename(columns=agg_labels)

x = x.groupby('i',axis=1).sum()

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x = x.fillna(0)

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

to_parquet(x, '../var/nonfood_expenditures.parquet')
