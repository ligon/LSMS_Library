#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, id_match
import dvc.api
from lsms import from_dta

z = {}
for t in Waves.keys():
    z[t] = pd.read_parquet('../'+t+'/_/other_features.parquet')
    #z[t] = id_match(y[t],t,Waves)

foo = z.copy()
z = pd.concat(z.values())

z = z.reset_index().set_index(['j','t','m'])
z.columns.name = 'k'

assert z.index.is_unique, "Non-unique index!  Fix me!"

z.to_parquet('../var/other_features.parquet')
