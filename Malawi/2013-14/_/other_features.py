#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, df_data_grabber

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import get_other_features


idxvars = {'j': 'y2_hhid',
           't': ('y2_hhid', lambda x: '2013-14')}
myvars = {'m': 'region', 'Rural': ('reside', {'rural': 1, 'urban': 0})}
df = df_data_grabber('../Data/HH_MOD_A_FILT_13.dta',
                           idxvars=idxvars,
                           **myvars,
                           convert_categoricals=True)
df.columns.name = 'k'

to_parquet(df, 'other_features.parquet')
