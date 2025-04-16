from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/m5_1_diary.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

cols = ['opstina', 'popkrug', 'dom']
df['j'] = df[cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

quant = [col for col in df if col.startswith('kol')]
df['Quantity'] = df[quant].sum(axis=1)
spent = [col for col in df if col.startswith('din')]
df['Total Expenditure'] = df[spent].sum(axis=1)

dailyprice = pd.concat([df[s]/df[q] for q,s in zip(quant, spent)], axis=1)
median = dailyprice.median(axis=1)
df['Price'] = median

dict = {'nsifra': 'i', 'mera': 'units'}
df = df.rename(dict, axis = 1).reset_index()
final = df.loc[:, ['j', 'i', 'Quantity', 'units', 'Total Expenditure', 'Price']]

final = final.set_index(['j','i'])
to_parquet(final, 'food_acquired.parquet')
