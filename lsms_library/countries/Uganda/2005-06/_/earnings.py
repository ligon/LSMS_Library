from lsms_library.local_tools import to_parquet, get_dataframe
#!/usr/bin/env python3

import pandas as pd

# NB: Earnings here are for last seven days.
fn = '../Data/GSEC8.dta'
earnings1 = ['h8q8a','h8q8b']  # Earnings from first job (cash, inkind)
earnings2 = []  # Not elicited separately in 2005-06

df = get_dataframe(fn)

earnings = df.groupby('HHID')[earnings1+earnings2].sum().sum(axis=1)

earnings.index.name = 'j'

to_parquet(pd.DataFrame({"Earnings":earnings}), 'earnings.parquet')
