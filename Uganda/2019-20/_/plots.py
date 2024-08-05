import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2019-20"),
               plt=(['parcelID','pltid'],lambda x: "{parcelID}-{pltid}".format_map(x)),
               acres='s4aq07'
               )

myvars = dict(crop="cropID")

df = df_data_grabber('../Data/Agric/agsec4a.dta',idxvars,**myvars)

to_parquet(df,'plots.parquet')
