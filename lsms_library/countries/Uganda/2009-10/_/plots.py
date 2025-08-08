import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet
    
def intercrop_area(row):
    if not np.isnan(row['area_percentage']):
        return row['acres'] * row['area_percentage']/100
    else:
        return row['acres']

idxvars = dict(j='HHID',
               t=('HHID', lambda x: "2009-10"),
               plt=(['a4aq2','a4aq4'],lambda x: "{a4aq2}-{a4aq4}".format_map(x)),
               crop="a4aq5"
               )

myvars = dict(acres = 'a4aq8',
              area_units = ('a4aq8', lambda x: 'ACRES'),
              intercrop='a4aq7',
              area_percentage = 'a4aq9')
df = df_data_grabber('../Data/AGSEC4A.dta',idxvars,**myvars)

df = df.assign(acres = df.apply(intercrop_area,axis=1)).drop(columns=['area_percentage'])
df = df.dropna(subset=['acres'])
to_parquet(df,'plots.parquet')
