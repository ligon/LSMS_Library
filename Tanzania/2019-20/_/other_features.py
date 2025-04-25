#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
import sys
sys.path.append('../../_/')
from tanzania import other_features

round_match = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}

myvars = dict(fn='../Data/HH_SEC_A.dta',
              HHID='sdd_hhid',
              urban='sdd_rural',
              region='domain',
              urban_converter = lambda x: True if x=='URBAN' else False)

df = other_features(**myvars)

df['Rural'] = 1 - df.urban.astype(int)

df['wave'] = '2019-20'

df = df.rename(columns={'region':'m','wave':'t'})

df = df.replace({'t':round_match})

df = df.reset_index().set_index(['j','t','m'])
df = df[['Rural']]

regions = set(df.index.get_level_values('m'))
df = df.rename(index={k:k.title() for k in regions})

assert df.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(df, 'other_features.parquet')
