import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet


def title_string(x):
    try:
        return x.title()
    except AttributeError:
        return np.nan
    
idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2012-13"),
               plt='plotid',
               crop=("cropcode", title_string)
               )


myvars = dict(area='s11fq1a',
              area_units=('s11fq1b', title_string),
              intercrop=('s11fq2', title_string)
              )

df = df_data_grabber('../Data/Post Planting Wave 2/Agriculture/sect11f_plantingw2.dta',idxvars,**myvars)

to_parquet(df,'plots.parquet')