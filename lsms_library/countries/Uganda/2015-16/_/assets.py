from lsms_library.local_tools import to_parquet
#!/usr/bin/env python3
import numpy as np
import pandas as pd
import dvc.api
from lsms import from_dta

fn = '../Data/gsec14.dta'

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

assets = df.groupby('hhid')['h14q5'].sum().replace(0,np.nan)

assets.index.name = 'j'
assets.name = 'assets'

to_parquet(pd.DataFrame({"Assets":assets}), 'assets.parquet')
