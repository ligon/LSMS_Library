#!/usr/bin/env python
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
sys.path.append('../../../_/')
from local_tools import df_from_orgfile

with dvc.api.open('../Data/SEC0A.DTA', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

of = df[['region','loc2']]
of['hhid'] = df['clust'].astype("string")+'-'+df['nh'].astype("string")
#of['hhid'] = df['clust'].astype('Int64').astype("string")+'-'+df['nh'].astype('Int64').astype("string")

of = of.rename(columns = {'hhid': 'j',
                          'region': 'm',
                          'loc2': 'Rural'})

#map numerical codes to categorical labels 
rural = df_from_orgfile('./categorical_mapping.org',name='rural',encoding='ISO-8859-1')
rurald = rural.set_index('Code').to_dict('dict')
region = df_from_orgfile('./categorical_mapping.org',name='region',encoding='ISO-8859-1')
regiond = region.set_index('Code').to_dict('dict')
of['Rural'] = of['Rural'].replace(rurald['Label'])
of['m'] = of['m'].replace(regiond['Label'])

of['j'] = of['j'].astype(str)
of['t'] = '1998-99'
of = of.set_index(['j','t','m'])
of['Rural'] = (of.Rural=='Rural') + 0.

of.to_parquet('other_features.parquet')
