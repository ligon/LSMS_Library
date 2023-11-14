#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd
from ghana import split_by_visit
sys.path.append('../../../_/')
from local_tools import df_from_orgfile

t = '1998-99'

#categorical mapping
labels = df_from_orgfile('./categorical_mapping.org',name='harmonize_food',encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_9b', 'Code_8h']:
    labelsd[column] = labels[['Preferred Label', column]].set_index(column).to_dict('dict')
units = df_from_orgfile('./categorical_mapping.org',name='s8hq9',encoding='ISO-8859-1')
unitsd = units.set_index('Code').to_dict('dict')

#food expenditure 
with dvc.api.open('../Data/SEC9B.DTA',mode='rb') as dta:
    df = pd.read_stata(dta, convert_categoricals=True)
    #harmonize food labels
    df['fdexpcd'] = df['fdexpcd'].replace(labelsd['Code_9b']['Preferred Label'])

df['hhid'] = df['clust'].astype("string")+'-'+df['nh'].astype("string")
#df['hhid'] = df['clust'].astype('Int64').astype("string")+'-'+df['nh'].astype('Int64').astype("string")
selector_pur = {'hhid': 'j', 
              'fdexpcd': 'i'} 
#create purchased column labels for each visit -- from the 2nd to 7th visit
for i in range(1, 7):
    visit = i + 1
    selector_pur[f's9bq{i}'] = f'purchased_value_v{visit}'
x = df.rename(columns=selector_pur)[[*selector_pur.values()]]
#only select food expenditures,since section9b also recorded non-food expenditures
#non-food expenditures remained as numerical codes in previous harmonization steps 
x = x[~x['i'].apply(lambda x: type(x) == int or type(x) == float)]
#unstack by visits 
x = x.replace({r'':np.nan, 0 : np.nan})
x = x.dropna(subset = x.columns.tolist()[2:], how ='all')
xf = split_by_visit(x, 2, 7, t)


#home produced amounts
with dvc.api.open('../Data/SEC8H.DTA',mode='rb') as dta:
    prod = pd.read_stata(dta, convert_categoricals=True)
    #harmonize food labels and map unit labels:
    prod['homagrcd'] = prod['homagrcd'].replace(labelsd['Code_8h']['Preferred Label'])
    prod['s8hq9'] = prod['s8hq9'].replace(unitsd['Label'])

prod = prod[prod['s8hq1'] == 1] #select only if hh consumed any own produced food in the past 12 months
#create produced column labels for each visit -- 3-day recall starting from the 2nd to 7th visit
prod['hhid'] = prod['clust'].astype("string")+'-'+prod['nh'].astype("string")
#prod['hhid'] = prod['clust'].astype('Int64').astype("string")+'-'+prod['nh'].astype('Int64').astype("string")
selector_pro = {'hhid': 'j', 
              'homagrcd': 'i',
              's8hq9': 'u',
              's8hq10': 'produced_price'} 
for i in range(3, 9):
    visit = i - 1
    selector_pro[f's8hq{i}'] = f'produced_quantity_v{visit}'
y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
#unit code 1.7498005798264095e+100 has no categorical mapping  
y = y[~y['u'].apply(lambda x: type(x) == int or type(x) == float)]

#unstack by visits 
y = y.replace({r'':np.nan, 0 : np.nan})
y = y.dropna(subset = y.columns.tolist()[3:], how ='all')
yf = split_by_visit(y, 2, 7, t, ind = ['j','t','i', 'u', 'produced_price'])
yf = yf.reset_index().set_index(['j','t','i'])

#combine xf and yf
f = xf.reset_index().merge(yf.reset_index(), on = ['j','t','i'], how = 'outer').set_index(['j','t','i', 'u'])
f.to_parquet('food_acquired.parquet')
