import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id
from collections import defaultdict

relationship_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='relationship',encoding='ISO-8859-1')
relationship_mapping = relationship_mapping.set_index('Code').to_dict('dict')['Label']

region_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='region',encoding='ISO-8859-1')
region_mapping = region_mapping.set_index('Code').to_dict('dict')['Label']
region_mapping = defaultdict(lambda:None,region_mapping)

idxvars = dict(j=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               t=('nh', lambda x: "1998-99"),
               indiv=('pid',lambda x: format_id(x))
               )

myvars = dict(Sex = ('sex', lambda s: 'MF'[int(s)-1]),
              Age = ('agey',lambda x: int(x)),
              Birthplace =('s1q10',lambda x: region_mapping[f"{x:3.0f}".strip()]),
              Relation = ('rel', relationship_mapping))

df = df_data_grabber('../Data/SEC1.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = df.select_dtypes(exclude='object').max().max()
df = df.replace(na,np.nan)

if __name__=='__main__':
    to_parquet(df,'household_roster.parquet')
