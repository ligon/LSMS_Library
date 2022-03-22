#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import other_features
from pathlib import Path

pwd = Path.cwd()
round = str(pwd.parent).split('/')[-1]


myvars = dict(fn='Uganda/%s/Data/GSEC1.dta' % round,
              HHID='HHID',
              urban='urban',
              region='region',
              urban_converter = lambda s: s.lower() == 'urban')

df = other_features(**myvars)

# Some "Central" households have region coded as 0?  These seem to be households in one of the 34
# Enumeration Areas (comm) of Kampala.
# See https://microdata.worldbank.org/index.php/catalog/1001/data-dictionary/F41?file_name=2005_GSEC1
df = df.replace({'region':{'0':'Kampala'}})

df['Rural'] = 1 - df.urban

df = df.rename(columns={'region':'m'})

df['t'] = round

df = df.reset_index().set_index(['j','t','m'])[['Rural']]

df.to_parquet('other_features.parquet')
