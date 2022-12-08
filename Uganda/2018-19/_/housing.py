#!/usr/bin/env python3
import numpy as np
import pandas as pd
import dvc.api
from lsms import from_dta

fn = '../Data/GSEC9.dta'
hhid = 'hhid'
d = {"Thatched roof" : ['h9q04',lambda x: 0 + ('Thatch' in x)],
     "Earthen floor" : ['h9q06',lambda x: 0 + ('earth' in x)]}

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta)

housing = df[[v[0] for v in d.values()]]
housing.index.name = 'j'
housing.columns.name = 'k'
housing = housing.fillna('0')

housing = housing.rename(columns={v[0]:k for k,v in d.items()})

for k,v in d.items():
    try:
        housing[k] = housing[k].apply(v[1])
    except IndexError:
        pass

housing.to_parquet('housing.parquet')
