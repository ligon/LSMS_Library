import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet
    
def intercrop_area(row):
    if not np.isnan(row['area_percentage']):
        return row['acres'] * row['area_percentage']/100
    else:
        return row['acres']

idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2019-20"),
               plt=(['parcelID','pltid'],lambda x: "{parcelID}-{pltid}".format_map(x)),
               crop="cropID"
               )

myvars = dict(acres='s4aq07',
              area_units = ('s4aq07', lambda x: 'ACRES'),
              intercrop = 's4aq08',
              area_percentage = 's4aq09')

df = df_data_grabber('../Data/Agric/agsec4a.dta',idxvars,**myvars)
df = df.assign(acres = df.apply(intercrop_area,axis=1)).drop(columns=['area_percentage'])
to_parquet(df,'plots.parquet')
