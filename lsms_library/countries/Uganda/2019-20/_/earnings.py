from lsms_library.local_tools import to_parquet
#!/usr/bin/env python3

import pandas as pd
import dvc.api
from lsms import from_dta

fn = '../Data/HH/gsec8.dta'
earnings1 = 's8q78'  # Earnings from first job
earnings2 = 's8q80'  # Earnings from second job

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

earnings = df.groupby('hhid')[[earnings1,earnings2]].sum().sum(axis=1)

earnings.index.name = 'j'

to_parquet(pd.DataFrame({"Earnings":earnings}), 'earnings.parquet')
