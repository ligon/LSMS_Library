#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from ethiopia import other_features
from pathlib import Path

pwd = Path.cwd()
round = str(pwd.parent).split('/')[-1]


myvars = dict(fn='../Data/sect_cover_hh_w3.dta',
              HHID='household_id2',
              urban='rural',
              region='saq01',
              urban_converter = lambda x: x.lower() != 'rural')

df = other_features(**myvars)

df['Rural'] = 1 - df.urban.astype(int)

df = df.rename(columns={'region':'m'})

df['t'] = round

df = df.reset_index().set_index(['j','t','m'])[['Rural']]

to_parquet(df, 'other_features.parquet')
