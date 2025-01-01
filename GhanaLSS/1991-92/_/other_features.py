#!/usr/bin/env python
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from lsms import from_dta
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id

rural = df_from_orgfile('./categorical_mapping.org',name='rural',encoding='ISO-8859-1')
rurald = rural.set_index('Code').to_dict('dict')['Label']
region = df_from_orgfile('../../_/categorical_mapping.org',name='region',encoding='ISO-8859-1')
regiond = region.set_index('Code').to_dict('dict')['Label']

idxvars = dict(j=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               t=('nh', lambda x: "1991-92"),
               m=('region',lambda x: regiond[f"{x:3.0f}".strip()]),
               )

myvars = dict(Rural=('loc2',rurald))

of = df_data_grabber('../Data/POV_GH.DTA',idxvars,**myvars)

if __name__=='__main__':
    to_parquet(of,'other_features.parquet')
