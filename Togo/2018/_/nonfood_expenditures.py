from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
import sys
sys.path.append('../../_')
sys.path.append('../../../_')
from togo import food_expenditures
import dvc.api
import numpy as np
import pandas as pd
import json
from cfe.df_utils import broadcast_binary_op

Dfs = []
for period in ['7days','30days','3months','6months','12months']:
    with dvc.api.open('../Data/Togo_survey2018_nonfooditems%s.csv' % period,mode='rb') as csv:
        Dfs.append(pd.read_csv(csv))

vars={'hhid': 'j',
      'nonfood_item' : 'i',
      'item_value': 'purchase value' # What was the value of [XX] bought the last time?
      }

df = pd.concat(Dfs).rename(columns=vars)
df['j'] = df['j'].astype(int).astype(str)
df['i'] = df['i'].astype(int).astype(str)

food = get_dataframe('food_expenditures.parquet')

# Get time from food df
t = food.groupby(['j','t']).count().reset_index('t')['t']
df = df.join(t,on='j').reset_index()
m = food.groupby(['j','m']).count().reset_index('m')['m']
df = df.join(m,on='j').reset_index()

df = df.rename(columns=vars).set_index(['j','t','m','i'])

x = df['purchase value']

x = x.unstack('i')

labels = json.load(open('food_items.json'))

x = x.rename(columns=labels)
x = x.groupby('i',axis=1).sum()

x = x.replace(0,np.nan)

x = x.reset_index().set_index(['j','t','m'])

x = x.drop_duplicates()

to_parquet(x, 'nonfood_expenditures.parquet')


