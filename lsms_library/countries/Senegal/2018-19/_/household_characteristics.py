#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta
from senegal import age_sex_composition
from lsms.tools import get_household_roster

with dvc.api.open('../Data/s01_me_sen2018.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

def sexconverter(x):
    if x == 'Féminin':
        return 'f'
    if x == 'Masculin':
        return 'm'

df['s01q03c'] = df['s01q03c'].mask(df['s01q03c'] >= 9999)

waves = {1: 2018, 2: 2019}
df['t'] = df['vague'].map(waves)
df['age'] = df['t'] - df['s01q03c']

df['hhid'] = df['vague'].astype(str) + df['grappe'].astype(str) + df['menage'].astype(str)

final = age_sex_composition(df, sex='s01q01', sex_converter=sexconverter,
                           age='age', age_converter=None, hhid='hhid')

final = final.reset_index()
final['j'] = final.j.astype(str).apply(lambda s: s.split('.')[0])

final['t'] = final['j'].str.get(0).astype(int).map(waves)
final = final.set_index(['j','t'])
final.columns.name = 'k'

to_parquet(final, 'household_characteristics.parquet')
