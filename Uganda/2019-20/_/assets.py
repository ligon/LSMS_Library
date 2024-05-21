#!/usr/bin/env python
import numpy as np
import pandas as pd
import dvc.api
from lsms import from_dta

fn = '../Data/HH/gsec14.dta'

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

assets = df.groupby('hhid')['h14q05'].sum().replace(0,np.nan)

assets.index.name = 'j'
assets.name = 'assets'

pd.DataFrame({"Assets":assets}).to_parquet('assets.parquet')
