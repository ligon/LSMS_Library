#!/usr/bin/env python

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
from lsms import from_dta
from lsms_library.local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id

idxvars = dict(j=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               m=('region',lambda s: s.title())
               )

myvars = dict(Rural=("loc2",lambda s: s.title()))

df = df_data_grabber('../Data/aggregates/pov_gh5.dta',idxvars,**myvars,convert_categoricals=True)
df['t'] = '2005-06'
df = df.reset_index().set_index(['j','t','m'])

if __name__=='__main__':
    to_parquet(df,'other_features.parquet')
