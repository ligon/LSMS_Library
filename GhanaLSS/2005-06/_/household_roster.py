import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id
from collections import defaultdict
import warnings

relationship_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='relationship',encoding='ISO-8859-1')
relationship_mapping = relationship_mapping.set_index('Code').to_dict('dict')['Label']

region_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='region',encoding='ISO-8859-1')
region_mapping = region_mapping.set_index('Code').to_dict('dict')['Label']
region_mapping = defaultdict(lambda:None,region_mapping)

idxvars = dict(j=('hhid',lambda x: format_id(x)),
               t=('nh', lambda x: "2005-06"),
               indiv=('pid',lambda x: format_id(x))
               )

myvars = dict(Sex = ('s1q2', lambda s: s.upper()[0]),
              Age = ('s1q5y',lambda x: int(x)),
              Birthplace =('region',lambda x: x.title()),
              Relation = ('s1q3', lambda x: x.title()))

df = df_data_grabber('../Data/parta/sec1.dta',idxvars,**myvars,convert_categoricals=True)

# Oddity with large number for missing code
na = df.select_dtypes(exclude='object').max().max()
if na>1e99:
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    df = df.replace(na,np.nan)

if __name__=='__main__':
    to_parquet(df,'household_roster.parquet')
