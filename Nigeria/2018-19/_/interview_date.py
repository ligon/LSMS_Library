import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2018-19"),
               )

myvars = dict(date='InterviewStart')

df = df_data_grabber('../Data/secta_plantingw4.dta',idxvars,**myvars)

to_parquet(df,'interview_date.parquet')
