#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd
sys.path.append('../../_')
from ghana import split_by_visit, load_large_dta
from lsms.tools import from_dta

t = '2016-17'

harmonized_label = pd.read_csv('food_label.csv', encoding='ISO-8859-1')

#food expenditure 
myvars = dict(fn='../Data/g7sec9b_small.dta')
#myvars = dict(fn='../Data/g7sec9b.dta')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
#with open(myvars['fn'],mode='rb') as dta:
    labels = pd.read_stata(dta, iterator=True).value_labels()

with dvc.api.open(myvars['fn'],mode='rb') as dta:
#with open(myvars['fn'],mode='rb') as dta:
    df = load_large_dta(dta, convert_categoricals=False)
    df = df.loc[df.filter(regex='^s9bq[1-6]a').sum(axis=1)>0]
    #df = from_dta(dta,convert_categoricals=False)
    #harmonize food labels and fix missing unit labels:
    labels['freqcd'] = harmonized_label[['Preferred Label', 'Code_9b']].dropna().set_index('Code_9b').to_dict('dict')['Preferred Label']
    #for i in range(1, 7):
    #    labels[f's9bq{i}c'] = labels['S9BQ1C']

df = df.replace(labels)

selector_pur = {'hid': 'j', 
              'freqcd': 'i'} 
#create purchased column labels for each visit -- 3-day recall starting from the 2nd to 7th visit
for i in range(1, 7):
    visit = i + 1
    selector_pur[f's9bq{i}a'] = f'purchased_value_v{visit}'
    selector_pur[f's9bq{i}b'] = f'purchased_quantity_v{visit}'
    selector_pur[f's9bq{i}c'] = f'purchased_unit_v{visit}'

x = df.rename(columns=selector_pur)[[*selector_pur.values()]]
#only select food expenditures,since section9b also recorded non-food expenditures
#non-food expenditures remained as numerical codes in previous harmonization steps 
x = x[~x['i'].apply(lambda x: type(x) == float)]
#unstack by visits 
x = x.replace({r'':np.nan, 0 : np.nan})
x = x.dropna(subset = x.columns.tolist()[2:], how ='all')
xf = split_by_visit(x, 2, 7, t, unit_col = 'purchased_unit', aggregate_amount = True)

#home produced amounts
myvars = dict(fn='../Data/g7sec8h.dta')
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    labels2 = pd.read_stata(dta, iterator=True).value_labels()
with dvc.api.open(myvars['fn'],mode='rb') as dta:
    prod = pd.read_stata(dta, convert_categoricals=True)

#harmonize food labels and unit labels:
food_l = harmonized_label[['Preferred Label', 'Label_8h']].dropna().set_index('Label_8h').to_dict('dict')['Preferred Label']
prod['foodcd'] = prod['foodcd'].replace(food_l)
unit_l = dict(zip(labels2['S8HU'].values(),pd.Series(labels2['S8HU'].values()).str.split(None, n=1).str[1]))
prod = prod.replace(unit_l)

prod = prod[prod['s8hq1'] == '1. Yes'] #select only if hh consumed any own produced food in the past 12 months
#create produced column labels for each visit -- 3-day recall starting from the 2nd to 7th visit
selector_pro = {'hid': 'j', 
              'foodcd': 'i'} 
for i in range(3, 9):
    visit = i - 1
    selector_pro[f's8hq{i}q'] = f'produced_quantity_v{visit}'
    selector_pro[f's8hq{i}u'] = f'produced_unit_v{visit}'
    selector_pro[f's8hq{i}p'] = f'produced_price_v{visit}'
y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
#unstack by visits 
y = y.replace({r'':np.nan, 0 : np.nan})
y = y.dropna(subset = y.columns.tolist()[2:], how ='all')
yf = split_by_visit(y, 2, 7, t, unit_col ='produced_unit')
yf = yf.loc[yf.produced_quantity>0]

#combine xf and yf
f = pd.concat([xf, yf], axis =0)
f = f.reset_index().groupby(['j','t', 'i', 'u']).agg({'purchased_value':"sum",
                                                      'purchased_quantity':"sum",
                                                      'produced_quantity':"sum",
                                                      'produced_price':"mean"})
f = f.rename(lambda x: str(x),level='u')
f.to_parquet('food_acquired.parquet')


#temporary code 
try:
    of = pd.read_parquet('other_features.parquet')
    f2 = f.reset_index().drop(columns = 't')
    f2['t'] = t
    f2 = f2.set_index(['j','t','i','u'])
    f2 = f2.join(of.reset_index('m')['m'],on=['j','t'])
    f2 = f2.reset_index().set_index(['j','t','m','i','u'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    f2['m'] = 'Ghana'
    f2 = f2.reset_index().set_index(['j','t','m','i','u'])
#expenditure
e = f2.groupby(['j', 'i'])['purchased_value'].agg(sum).to_frame()
e.to_parquet('food_expenditures.parquet')

#price
f2['purchased_price'] = f2['purchased_value'] / f2['purchased_quantity']
p = f2[['purchased_price', 'produced_price']].groupby(['t','m','i','u']).median()
p = p.reset_index().set_index(['t','m','i','u'])
p.unstack('t').to_parquet('food_prices.parquet')
