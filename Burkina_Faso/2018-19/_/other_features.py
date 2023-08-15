#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np

df = pd.read_parquet('household_characteristics.parquet')
df = df.reset_index().set_index(['j', 't']).loc[:, 'm'].to_frame()

df.to_parquet('other_features.parquet')
