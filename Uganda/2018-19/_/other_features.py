#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import other_features
from pathlib import Path

pwd = Path.cwd()
round = str(pwd.parent).split('/')[-1]


myvars = dict(fn='../Data/GSEC1.dta',
              HHID='hhid',
              urban='urban',
              region='region',
              urban_converter = lambda s: s.lower() == 'urban')

df = other_features(**myvars)

df['Rural'] = 1 - df.urban.astype(int)

df = df.rename(columns={'region':'m'})

df['t'] = round

df = df.reset_index().set_index(['j','t','m'])[['Rural']]

df.to_parquet('other_features.parquet')
