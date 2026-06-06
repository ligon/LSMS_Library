"""Build Nigeria 2012-13 individual_education.

Education (GHS section 2) is post-harvest only -> t=2013Q1, matching the
post-harvest slice of household_roster (i=hhid, pid=indiv).

Source file:
  - Post Harvest Wave 2/Household/sect2a_harvestw2.dta (t=2013Q1)

Educational Attainment = highest qualification (s2aq9), raw labels
('SS3', 'P6', '1ST DEGREE', ...).  Low coverage (~310 rows): the W2
questionnaire only recorded the qualification for members who had left
school.
"""
import sys
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet


def extract_string(x):
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return ''


idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2013Q1'),
    pid='indiv',
)

myvars = dict(
    Attainment=('s2aq9', lambda s: extract_string(s)),
)

df = df_data_grabber(
    '../Data/Post Harvest Wave 2/Household/sect2a_harvestw2.dta',
    idxvars, **myvars,
)
df = df.rename(columns={'Attainment': 'Educational Attainment'})
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'individual_education.parquet')
