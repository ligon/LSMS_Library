import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
sys.path.append('../../_/')
from lsms_library.local_tools import (assign_datetime, datetime_reports,
                                      df_data_grabber, to_parquet)
from nigeria import interview_result_label


##Harvest '2013Q1'
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2013Q1"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq1', interview_result_label))
df = df_data_grabber('../Data/Post Harvest Wave 2/Household/secta_harvestw2.dta',idxvars,**myvars)

#Planting(2012Q3)
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2012Q3"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq22a', interview_result_label))
df_1 = df_data_grabber('../Data/Post Planting Wave 2/Household/secta_plantingw2.dta',idxvars,**myvars)

df=pd.concat([df,df_1],axis=0)
# The NBS Q13 covers use the Gregorian civil day/month/year
# (Nigeria/_/CONTENTS.org, GH #1045). Keep the legacy round tags on t.
components = df[['year', 'month', 'day']]
df = assign_datetime(df.drop(columns=components.columns), 'int_t',
                     components.rename(columns={'year': 'y', 'month': 'm', 'day': 'd'}),
                     calendar='gregorian', resolution='day', country='Nigeria',
                     table='interview_date', wave_folder='2012-13')
# InterviewResult must not carry an undated visit. A parsed partial date
# has the registered key and is not a status-only row (GH #1096).
columns = ['int_t', 'InterviewResult']
if 'Derivation' in df:
    columns.append('Derivation')
df = df.dropna(subset=['int_t'])[columns]
to_parquet(df, 'interview_date.parquet',
           datetime_audit=datetime_reports('Nigeria', 'interview_date'))
