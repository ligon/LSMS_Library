#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import other_features

myvars = dict(fn='Uganda/2013-14/Data/GSEC1.dta',
              HHID='HHID',
              urban='urban',
              region='region')

df = other_features(**myvars)

df.to_parquet('other_features.parquet')
