from lsms_library.local_tools import to_parquet, get_dataframe
#!/usr/bin/env python3

import pandas as pd

fn = '../Data/GSEC8.dta'
earnings1 = 's8q78'  # Earnings from first job
earnings2 = 's8q80'  # Earnings from second job

df = get_dataframe(fn)

earnings = df.groupby('hhid')[[earnings1,earnings2]].sum().sum(axis=1)

earnings.index.name = 'j'

to_parquet(pd.DataFrame({"Earnings":earnings}), 'earnings.parquet')
