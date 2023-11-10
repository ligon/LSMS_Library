#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana import yearly_expenditure
import numpy as np
import dvc.api
import pandas as pd
sys.path.append('../../../_/')
from local_tools import df_from_orgfile

t = '1988-89'
#categorical mapping
labels = df_from_orgfile('./categorical_mapping.org',name='harmonize_food',encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_12A', 'Code_12B']:
    labels[column] = labels[column].astype('Int64').astype('string')
    labelsd[column] = labels[['Preferred Label', column]].set_index(column).to_dict('dict')


# food expenditure 
with dvc.api.open('../Data/Y12A.DAT',mode='rb') as csv:
    df = pd.read_csv(csv)
    #map codes to categorical labels 
    df['FOODCD'] = df['FOODCD'].astype('string').replace(labelsd['Code_12A']['Preferred Label'])

df['purchased_value_yearly'] = df.apply(yearly_expenditure, axis=1)

selector_pur = {'HID': 'j', 
              'FOODCD': 'i', 
              'CFOODBLV': 'purchased_value_vlast', #amount spent since last visit 
              'purchased_value_yearly': 'purchased_value_yearly'}  
x = df.rename(columns=selector_pur)[[*selector_pur.values()]]
x = x.replace({'.':np.nan, 0: np.nan})
xf = x.dropna(subset = x.columns.tolist()[2:], how ='all')


#home produced amounts
with dvc.api.open('../Data/Y12B.DAT',mode='rb') as csv:
    prod = pd.read_csv(csv)
    #harmonize food labels and map unit labels:
    prod['FOODCD'] = prod['FOODCD'].astype('string').replace(labelsd['Code_12B']['Preferred Label'])

prod['UTFOODC'] = prod['UTFOODC'].astype(str)
prod['produced_value_yearly'] = prod.apply(yearly_expenditure, 
                                           cost = 'VFOODCPD', 
                                           freq = 'TFOODC', 
                                           freq_unit = 'UTFOODC', 
                                           months = 'MFOODCLY', 
                                           axis=1)

selector_pro = {'HID': 'j', 
              'FOODCD': 'i', 
              'VFOODCPD': 'produced_value_daily', #amount spent since last visit 
              'produced_value_yearly': 'produced_value_yearly'}  
y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
y = y.replace({'.':np.nan, 0: np.nan})
yf = y.dropna(subset = y.columns.tolist()[2:], how ='all')


#combine xf and yf
xf['j'] = xf['j'].astype(str)
yf['j'] = yf['j'].astype(str)
f = xf.merge(yf, on = ['j','i'], how = 'outer')
f['u'] = np.nan
f['t'] = t
f = f.set_index(['j','t','i', 'u'])
f.to_parquet('food_acquired.parquet')