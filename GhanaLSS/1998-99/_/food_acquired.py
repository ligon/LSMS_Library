#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd
sys.path.append('../../_')
from ghana import split_by_visit

t = '2012-13'

#food expenditure 
myvars = dict(fn='../Data/PARTB/sec9b.dta')
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    labels = pd.read_stata(dta, iterator=True).value_labels()
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    df = pd.read_stata(dta, convert_categoricals=False)
    df = df.replace(labels)

selector_pur = {'hid': 'j', 
              'freqcd': 'i'} 
#create purchased column labels for each visit -- 3-day recall starting from the 2nd to 7th visit
for i in range(1, 7):
    visit = i + 1
    selector_pur[f's9bq{i}'] = f'purchased_value_v{visit}'
x = df.rename(columns=selector_pur)[[*selector_pur.values()]]
x = x.replace({r'':np.nan, 0 : np.nan})
x = x.dropna(subset = x.columns.tolist()[2:], how ='all')
xf = split_by_visit(x, 2, 7, t)

#home produced amounts
myvars = dict(fn='../Data/PARTB/sec8h.dta')
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    labels2 = pd.read_stata(dta, iterator=True).value_labels()
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    prod = pd.read_stata(dta, convert_categoricals=True)

prod = prod[prod['s8hq1'] == 'yes'] #select only if hh consumed any own produced food in the past 12 months
#create produced column labels for each visit -- 3-day recall starting from the 2nd to 7th visit
selector_pro = {'hid': 'j', 
              'foodcd': 'i',
              's8hq9': 'u',
              's8hq10': 'produced_price'} 
for i in range(3, 9):
    visit = i - 1
    selector_pro[f's8hq{i}'] = f'produced_quantity_v{visit}'
y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
y = y.replace({r'':np.nan, 0 : np.nan})
y = y.dropna(subset = y.columns.tolist()[2:], how ='all')
yf = split_by_visit(y, 2, 7, t, ind = ['j','t','i', 'u', 'produced_price'])
yf = yf.reset_index().set_index(['j','t','i'])

#combine xf and yf
f = xf.reset_index().merge(yf.reset_index(), on = ['j','t','i'], how = 'outer').set_index(['j','t','i', 'u'])
f.to_parquet('food_acquired.parquet')
