#!/usr/bin/env python
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id, get_categorical_mapping
from collections import defaultdict
import warnings

relationship_mapping = get_categorical_mapping(tablename='relationship')

region_mapping = get_categorical_mapping(tablename='region')
region_mapping = defaultdict(lambda:None,region_mapping)

idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+'/'+format_id(x.nh,zeropadding=2)),
               w=('hid', lambda x: "2016-17"),
               v = ('clust',format_id),
               indiv=('pid',format_id)
               )

myvars = dict(Sex = ('s1q2', lambda s: s.upper()[0]),
              Age = ('s1q5y',lambda x: int(x)),
              Birthplace =('s1q11',lambda x: x.title() if isinstance(x,str) else np.nan),
              Relation = ('s1q3', lambda x: x.title()))

df = df_data_grabber('../Data/g7sec1.dta',idxvars,**myvars,convert_categoricals=True)

#assert len(set(df.Birthplace.value_counts().index.tolist()).difference(region_mapping.values()))==0
#assert len(set(df.Relation.value_counts().index.tolist()).difference(relationship_mapping.values()))==0

# Oddity with large number for missing code
na = df.select_dtypes(exclude='object').max().max()
if na>1e99:
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    df = df.replace(na,np.nan)

if __name__=='__main__':
    to_parquet(df,'household_roster.parquet')
