#!/usr/bin/env python
import sys
sys.path.append('../../_')
from lsms.tools import get_food_expenditures
import dvc.api
import pandas as pd
import json
import numpy as np


# There are two waves of data in each file, so we modify food expenditures to allow filtering.

def nonfood_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID',filter=None,itemlabels='nonfood_items.json'):

    with open(itemlabels) as f:
        items = {int(float(k)):v for k,v in json.load(f)['Label'].items()}

    # expenditures
    with dvc.api.open(fn,mode='rb') as f:
        df = pd.read_stata(f,convert_categoricals=False,preserve_dtypes=False)

    df['HHID'] = df.grappe*1000+df.menage

    if filter is not None:
        df = df.query(filter)

    expenditures,itemlabels = get_food_expenditures(df,purchased,away,produced,given,itmcd=item,HHID=HHID,itemlabels=items,fn_type=None)

    expenditures.columns.name = 'i'
    expenditures.index.name = 'j'
    expenditures.replace(0, np.nan, inplace=True)
    
    return expenditures

# Wave 1
t = '2018'

myvars = dict(fn='CotedIvoire/%s/Data/Menage/s09b_me_CIV2018.dta' % t,purchased='s09bq03',
              item='s09bq01',filter='vague==1')

x = nonfood_expenditures(**myvars)

x['t'] = t
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

X = [x.copy()]

# Wave 2

myvars['filter'] = 'vague==2'

x = nonfood_expenditures(**myvars)

x['t'] = '2019'

x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

X.append(x)

df = pd.concat(X)

df.to_parquet('nonfood_expenditures.parquet')

