import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

#Planting(2018Q3)
idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2018Q3"),
               )

myvars = dict(date=('InterviewStart', lambda x: pd.to_datetime(x).date))

df_planting = df_data_grabber('../Data/secta_plantingw4.dta',idxvars,**myvars)



#Harvest(2019Q1)
idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2019Q1"),
               )

myvars = dict(date=('InterviewStart', lambda x: pd.to_datetime(x).date))

df = df_data_grabber('../Data/secta_harvestw4.dta',idxvars,**myvars)


df=pd.concat([df_planting,df],axis=0)
to_parquet(df,'interview_date.parquet')