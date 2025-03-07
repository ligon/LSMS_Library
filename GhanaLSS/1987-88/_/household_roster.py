#!/usr/bin/env python
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id

region_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='region',encoding='ISO-8859-1')
region_mapping = region_mapping.set_index('Code').to_dict('dict')['Label']

relationship_mapping = df_from_orgfile('../../_/categorical_mapping.org',name='relationship',encoding='ISO-8859-1')
relationship_mapping = relationship_mapping.set_index('Code').to_dict('dict')['Label']

idxvars = dict(j=('HID',format_id),
               t=('HID', lambda x: "1987-88"),
               indiv=('PID',format_id)
               )

myvars = dict(Sex = ('SEX', lambda s: 'MF'[s-1]),
              Age = ('AGEY',lambda x: pd.to_numeric(x,errors='coerce')),
              Relation = ('REL', relationship_mapping),
              Birthplace =('REGION',lambda x: region_mapping[str(x)])
              )

df = df_data_grabber('../Data/Y01A.DAT',idxvars,**myvars)

if __name__=='__main__':
    to_parquet(df,'household_roster.parquet')
