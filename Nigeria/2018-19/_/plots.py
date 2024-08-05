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
               plt='plotid')
myvars = dict(area ='s11aq4aa',
              area_units=('s11aq4b', extract_string)
              )
df_plot = df_data_grabber('../Data/sect11a1_plantingw4.dta',idxvars,**myvars).reset_index()


idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2018-19"),
               plt='plotid',
               crop=("cropcode",extract_string)
               )


myvars = dict(pct_area=('s11fq1',area_string_to_number),
              intercrop=('s11fq2a',extract_string))

df_crop = df_data_grabber('../Data/sect11f_plantingw4.dta',idxvars,**myvars).reset_index()

df=pd.merge(df_crop,df_plot,on=['j', 'plt'],how='left')
df['area']=df['pct_area']*df['area']
df=df.drop(columns=['pct_area'])
df=df.set_index(['j', 't', 'plt', 'crop'])


to_parquet(df,'plots.parquet')
