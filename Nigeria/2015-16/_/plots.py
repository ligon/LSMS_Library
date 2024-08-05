import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

def area_string_to_number(x):
    """
    Deal with formatting s11fq1.
    """
    try:
        return float(x.split('. ')[0])/100
    except AttributeError:
        return np.nan

def extract_string(x):
    try:
        return x.split('. ')[1].title().replace(' ','-')
    except AttributeError:
        return np.nan

idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2015-16"),
               plt='plotid',
               crop=("cropcode",extract_string)
               )


myvars = dict(area='s11fq1a',
              area_units=('s11fq1b', extract_string),
              intercrop=('s11fq2',extract_string))

df = df_data_grabber('../Data/sect11f_plantingw3.dta',idxvars,**myvars)

to_parquet(df,'plots.parquet')