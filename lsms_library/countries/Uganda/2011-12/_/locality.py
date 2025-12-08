#!/usr/bin/env python
"""Extract locality (parish/village) identifiers."""
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
from uganda import other_features
from pathlib import Path

pwd = Path.cwd()
round = str(pwd.parent).split('/')[-1]

myvars = dict(fn='../Data/GSEC1.dta',
              HHID='HHID',
              urban='urban',
              region='region',
              v='comm',
              urban_converter=lambda s: s.lower() == 'urban')

df = other_features(**myvars)

df = df.replace({'region': {'0': 'Kampala'}})
df = df.rename(columns={'region': 'm'})
df['t'] = round
df = df.reset_index().set_index(['i', 't', 'm'])[['v']]

to_parquet(df, 'locality.parquet')
