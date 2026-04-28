from lsms_library.local_tools import to_parquet, get_dataframe
#!/usr/bin/env python3

import pandas as pd

# NB: Earnings here are for last seven days.
fn = '../Data/GSEC8.dta'
earnings1 = ['h8q31a','h8q31b']  # Earnings from first job (cash, inkind)
earnings2 = ['h8q45a','h8q45b']  # Earnings from second job (cash, inkind)

df = get_dataframe(fn)

earnings = df.groupby('HHID')[earnings1+earnings2].sum().sum(axis=1)

earnings.index.name = 'j'

to_parquet(pd.DataFrame({"Earnings":earnings}), 'earnings.parquet')
