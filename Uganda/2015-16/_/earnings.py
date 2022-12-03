#!/usr/bin/env python3

import pandas as pd
import dvc.api
from lsms import from_dta

# NB: Earnings here are for last seven days.
fn = '../Data/gsec8.dta'
earnings1 = ['h8q31a','h8q31b']  # Earnings from first job (cash, inkind)
earnings2 = ['h8q45a','h8q45b']  # Earnings from second job (cash, inkind)

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

earnings = df.groupby('hhid')[earnings1+earnings2].sum().sum(axis=1)

earnings.index.name = 'j'

pd.DataFrame({"Earnings":earnings}).to_parquet('earnings.parquet')
