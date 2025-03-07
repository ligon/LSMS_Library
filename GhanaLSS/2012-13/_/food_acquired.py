#!/usr/bin/env python
import sys
from lsms.tools import from_dta
import numpy as np
import dvc.api
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import df_from_orgfile, get_categorical_mapping, df_data_grabber, format_id, _to_numeric, to_parquet
w = '2012-13'

#categorical mapping
labelsd = {}
for column in ['Code_9b', 'Code_8h']:
    labelsd[column] = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':(column,format_id)},**{'Label':'Preferred Label'})
#units = df_from_orgfile('./categorical_mapping.org',name='s8hq9',encoding='ISO-8859-1')
#unitsd = units.set_index('Code').to_dict('dict')

# food expenditure
idxvars = dict(i='hid',
               v=('clust',format_id),
               j=('freqcd',lambda x: labelsd['Code_9b'][format_id(x)]))

myvars = dict()
# Iterate over visits
for i in range(1, 7):
    myvars[f"Expenditure_{i}"]= (f"s9bq{i}",_to_numeric)

x = df_data_grabber('../Data/PARTB/sec9b.dta',idxvars,convert_categoricals=False,**myvars)

dfs = {}
for i in range(1, 7):
    columns = [f"Expenditure_{i}"]
    dfs[i] = x.loc[:,columns].copy()
    dfs[i].columns = ['Expenditure']
    dfs[i]['visit'] = i
    dfs[i] = dfs[i].reset_index().replace({r'':np.nan, 0 : np.nan})

fe = pd.concat(dfs.values(),ignore_index=True)
fe = fe.loc[fe['j'].isin(labelsd['Code_9b'].values())]
fe= fe.groupby(['v', 'i', 'j', 'visit']).sum() # Deal with some cases with multiple records for purchases



####################
# Home produced
####################

with dvc.api.open('../Data/PARTB/sec8h.dta',mode='rb') as dta:
    prod = from_dta(dta, convert_categoricals=True)
    #harmonize food labels and map unit labels:
    prod['foodcd'] = prod['foodcd'].replace(labelsd['Code_8h'])

# Some bizarre garbage in dta masquerading as extremely large numbers.
#prod['s8hq14'] = prod.s8hq14.where(prod.s8hq14!=prod.s8hq14.max())

prod = prod[prod['s8hq1'] == 'yes'] #select only if hh consumed any own produced food in the past 12 months
#create produced column labels for each visit -- 3-day recall starting from the 2nd to 7th visit

selector_pro = {'clust': 'v',
                'hid': 'i',
                'foodcd': 'j',
                's8hq9': 'u',
                's8hq10': 'Price'}

selector_pro.update({f's8hq{i}':f'Produced_{i}' for i in range(3,9)})

y = prod.rename(columns=selector_pro)[[*selector_pro.values()]]
#unit code 1.7498005798264095e+100 has no categorical mapping
y.index = y.index.map(str)

#unstack by visits
y = y.replace({r'':np.nan, 0 : np.nan})
y = y.loc[y['j'].isin(labelsd['Code_8h'].values())]
y = y.groupby(['v', 'i','j','u']).sum() # Deal with some cases with multiple records for purchases
y = pd.wide_to_long(y.reset_index(), stubnames=['Produced'], i=['v', 'i', 'j', 'u'], j='visit', sep='_', suffix='\\d+')

y = y.join(fe,how='outer')

y['w'] = w
y = y.reset_index().set_index(['w','v','i','j','u','visit'])

# fa = y.groupby(['j','t','i','u']).sum()
fa = y.replace(0,np.nan).dropna(how='all')

# Deal with non-string values in units
fa.index = fa.index.set_levels(fa.index.levels[5].astype(str),level='u')
if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
