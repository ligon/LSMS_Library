import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

def extract_number(x):
    """
    Deal with formatting field of the form "n. x" where x is the desired number.
    """
    try:
        return float(x.split('. ')[-1])
    except AttributeError:
        return pd.NA

def extract_string(x):
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return ''

_clean_age = lambda a: a if pd.notna(a) and 0 <= a <= 120 else pd.NA

# Post planting:

idxvars = dict(i='hhid',
               t=('hhid', lambda x: "2012Q3"),
               v='ea',
               pid='indiv',
               )

myvars = dict(Sex = ('s1q2', lambda s: extract_string(s).title()),
              Age = ('s1q6', _clean_age),
              Relationship = ('s1q3', lambda s: extract_string(s).title()),
              in_housing = ('s1q4', lambda s: extract_string(s).title()))

pp = df_data_grabber('../Data/Post Planting Wave 2/Household/sect1_plantingw2.dta',idxvars,**myvars)

# Post harvest
#
idxvars = dict(i='hhid',
               t=('hhid', lambda x: "2013Q1"),
               v='ea',
               pid='indiv',
               )

myvars = dict(Sex = ('s1q2', lambda s: extract_string(s).title()),
              Age = 's1q4',
              Relationship = ('s1q3', lambda s: extract_string(s).title()),
              in_housing = ('s1q14', lambda s: extract_string(s).title())) 

ph = df_data_grabber('../Data/Post Harvest Wave 2/Household/sect1_harvestw2.dta',idxvars,**myvars)
df = pd.concat([pp,ph])

# Drop rows for individuals who are not in household any longer
# (e.g., who were in hh at planting, but left or died before harvest)
df = df.replace('',pd.NA).sort_index().dropna(how='all')

to_parquet(df,'household_roster.parquet')