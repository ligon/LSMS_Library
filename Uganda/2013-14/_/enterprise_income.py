from lsms_library.local_tools import to_parquet
#!/usr/bin/env python3
import numpy as np
import pandas as pd
import dvc.api
from lsms import from_dta

fn = '../Data/gsec12.dta'
hhid = 'hhid'
d = dict(revenue = 'h12q13',
         wagebill = 'h12q15',
         materials = 'h12q16',
         otherexpense = 'h12q17')

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

enterprise_income = df.groupby(hhid)[list(d.values())].sum() # Sum over enterprises
enterprise_income.index.name = 'j'

enterprise_income = enterprise_income.rename(columns={v:k for k,v in d.items()})

enterprise_income['profits'] = np.maximum(enterprise_income['revenue'] - enterprise_income[['wagebill','materials','otherexpense']].sum(axis=1),0)
enterprise_income['losses'] = -np.minimum(enterprise_income['revenue'] - enterprise_income[['wagebill','materials','otherexpense']].sum(axis=1),0)

to_parquet(enterprise_income, 'enterprise_income.parquet')
