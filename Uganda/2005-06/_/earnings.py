from lsms_library.local_tools import to_parquet
#!/usr/bin/env python3

import pandas as pd
import dvc.api
from lsms import from_dta

# NB: Earnings here are for last seven days.
fn = '../Data/GSEC8.dta'
earnings1 = ['h8q8a','h8q8b']  # Earnings from first job (cash, inkind)
earnings2 = []  # Not elicited separately in 2005-06

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

earnings = df.groupby('HHID')[earnings1+earnings2].sum().sum(axis=1)

earnings.index.name = 'j'

to_parquet(pd.DataFrame({"Earnings":earnings}), 'earnings.parquet')
