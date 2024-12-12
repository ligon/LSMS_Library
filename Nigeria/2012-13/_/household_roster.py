import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

def extract_number(x):
    """
    Deal with formatting field of the form "n. x" where x is the desired number.
    """
    try:
        return float(x.split('. ')[-1])
    except AttributeError:
        return np.nan

def extract_string(x):
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return ''

# Post planting:

idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2012Q3"),
               m=('zone', extract_string),
               indiv='indiv',
               )

myvars = dict(sex = ('s1q2', lambda s: extract_string(s).title()),
              age = 's1q6',
              relation = ('s1q3', lambda s: extract_string(s).title()))

pp = df_data_grabber('../Data/Post Planting Wave 2/Household/sect1_plantingw2.dta',idxvars,**myvars)

# Post harvest
#
idxvars = dict(j='hhid',
               t=('hhid', lambda x: "2013Q1"),
               m=('zone', extract_string),
               indiv='indiv',
               )

myvars = dict(sex = ('s1q2', lambda s: extract_string(s).title()),
              age = 's1q4',
              relation = ('s1q3', lambda s: extract_string(s).title()),)

ph = df_data_grabber('../Data/Post Harvest Wave 2/Household/sect1_harvestw2.dta',idxvars,**myvars)
df = pd.concat([pp,ph])

# Drop rows for individuals who are not in household any longer
# (e.g., who were in hh at planting, but left or died before harvest)
df = df.replace('',np.nan).sort_index().dropna(how='all')

to_parquet(df,'household_roster.parquet')