#!/usr/bin/env python
import numpy as np
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id, get_categorical_mapping
from collections import defaultdict
import warnings

relationship_mapping = get_categorical_mapping(tablename='relationship')

region_mapping = get_categorical_mapping(tablename='region')
region_mapping = defaultdict(lambda:None,region_mapping)

idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh', lambda x: "1991-92"),
               v=('clust',format_id),
               indiv=('pid',lambda x: format_id(x)))


myvars = dict(Sex = ('sex', lambda s: 'MF'[int(s)-1]),
              Age = ('agey',lambda x: int(x)),
              Birthplace =('s1q10',lambda x: region_mapping[f"{x:3.0f}".strip()]),
              Relation = ('rel', relationship_mapping))

df = df_data_grabber('../Data/S1.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = df.select_dtypes(exclude='object').max().max()

if na==99 or na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    df = df.replace(na,np.nan)

if __name__=='__main__':
    to_parquet(df,'household_roster.parquet')
