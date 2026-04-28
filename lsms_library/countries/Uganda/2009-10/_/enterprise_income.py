from lsms_library.local_tools import to_parquet, get_dataframe
#!/usr/bin/env python3
import numpy as np
import pandas as pd

fn = '../Data/GSEC12.dta'
hhid = 'HHID'
d = dict(revenue = 'h12q13',
         wagebill = 'h12q15',
         materials = 'h12q16',
         otherexpense = 'h12q17')

df = get_dataframe(fn)

enterprise_income = df.groupby(hhid)[list(d.values())].sum() # Sum over enterprises
enterprise_income.index.name = 'j'

enterprise_income = enterprise_income.rename(columns={v:k for k,v in d.items()})

enterprise_income['profits'] = np.maximum(enterprise_income['revenue'] - enterprise_income[['wagebill','materials','otherexpense']].sum(axis=1),0)
enterprise_income['losses'] = -np.minimum(enterprise_income['revenue'] - enterprise_income[['wagebill','materials','otherexpense']].sum(axis=1),0)

to_parquet(enterprise_income, 'enterprise_income.parquet')
