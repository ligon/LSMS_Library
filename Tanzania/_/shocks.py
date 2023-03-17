#!/usr/bin/env python
"""
Concatenate data on shocks across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, id_match
import dvc.api
from lsms import from_dta

s={}
for t in Waves.keys():
    s[t] = pd.read_parquet('../'+t+'/_/shocks.parquet')
    s[t] = id_match(s[t],t,Waves)

s = pd.concat(s.values())
s['m'] = 'Tanzania'
s = s.reset_index().set_index(['j','t','m'])
s = s.drop(columns ='index')
s.to_parquet('../var/shocks.parquet')
