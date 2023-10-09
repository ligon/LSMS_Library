#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd
sys.path.append('../../_')
from ghana import split_by_visit

t = '2005-06'

harmonized_label = pd.read_csv('food_label.csv', encoding='ISO-8859-1')

#food expenditure 
with dvc.api.open('../Data/partb/sec9b.dta',mode='rb') as dta:
    labels = pd.read_stata(dta, iterator=True).value_labels()
with dvc.api.open('../Data/partb/sec9b.dta',mode='rb') as dta:
    df = pd.read_stata(dta, convert_categoricals=False)
     #harmonize food labels
    labels['freqcd'] = harmonized_label[['Preferred Label', 'Code_9b']].dropna().set_index('Code_9b').to_dict('dict')['Preferred Label']
    df = df.replace(labels)

selector_pur = {'hhid': 'j', 
              'freqcd': 'i'} 
#create purchased column labels for each visit -- from the 2nd to 11th visit
for i in range(1, 11):
    visit = i + 1
    selector_pur[f's9bq{i}'] = f'purchased_value_v{visit}'
x = df.rename(columns=selector_pur)[[*selector_pur.values()]]
#only select food expenditures,since section9b also recorded non-food expenditures
#non-food expenditures remained as numerical codes in previous harmonization steps 
x = x[~x['i'].apply(lambda x: type(x) == int)]
#unstack by visits 
x = x.replace({r'':np.nan, 0 : np.nan})
x = x.dropna(subset = x.columns.tolist()[2:], how ='all')
xf = split_by_visit(x, 3, 11, t)

#home produced amounts
with dvc.api.open('../Data/partb/sec8h.dta',mode='rb') as dta:
    labels2 = pd.read_stata(dta, iterator=True).value_labels()
with dvc.api.open('../Data/partb/sec8h.dta',mode='rb') as dta:
    prod = pd.read_stata(dta, convert_categoricals=True)

#harmonize food labels and unit labels:
food_l = harmonized_label[['Preferred Label', 'Label_8h']].dropna().set_index('Label_8h').to_dict('dict')['Preferred Label']
prod['foodcd'] = prod['foodcd'].replace(food_l)
prod = prod[prod['s8hq1'] == 'yes'] #select only if hh consumed any own produced food in the past 12 months
#create produced column labels for each visit -- 3-day recall starting from the 3nd to 11th visit
selector_pro = {'hhid': 'j', 
              'foodcd': 'i',
              's8hq13': 'u',
              's8hq14': 'produced_price'} 
for i in range(4, 13):
    visit = i - 1
    selector_pro[f's8hq{i}'] = f'produced_quantity_v{visit}'
y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
#unstack by visits 
y = y.replace({r'':np.nan, 0 : np.nan})
y = y.dropna(subset = y.columns.tolist()[2:], how ='all')
yf = split_by_visit(y, 3, 11, t, ind = ['j','t','i', 'u', 'produced_price'])
yf = yf.reset_index().set_index(['j','t','i'])

#combine xf and yf
f = xf.reset_index().merge(yf.reset_index(), on = ['j','t','i'], how = 'outer').set_index(['j','t','i', 'u'])
f.to_parquet('food_acquired.parquet')
