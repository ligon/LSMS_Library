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

with dvc.api.open('../Data/Cross_Sectional/hh_mod_a_filt.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

idxvars = {'j': 'y4_hhid',
           't': ('y4_hhid', lambda x: '2019-20')}
myvars = {'m': 'region', 'Rural': ('reside', {'Rural': 1, 'Urban': 0})}
panel_df = df_data_grabber('../Data/Panel/hh_mod_a_filt_19.dta',
                           idxvars=idxvars,
                           **myvars,
                           convert_categoricals=True)
panel_df.columns.name = 'k'
df = get_other_features(df, '2019-20', 'reside')

df['Rural'] = df.Rural - 1
df = pd.concat([df, panel_df], axis=0)

to_parquet(df, 'other_features.parquet')
