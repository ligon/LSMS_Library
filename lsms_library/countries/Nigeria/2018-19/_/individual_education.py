"""Build Nigeria 2018-19 individual_education.

Education (GHS section 2) is post-harvest only -> t=2019Q1, matching the
post-harvest slice of household_roster (i=hhid, pid=indiv).

Source file:
  - sect2_harvestw4.dta (t=2019Q1)

Educational Attainment = highest qualification (s2aq9), raw labels
('43. HIGHER DEGREE / POST-GRADUATE DEGREE', '16. PRIMARY 6', ...).
extract_string strips the 'n. ' numeric prefix.  Near-universal
coverage (~19.3k rows).
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
    t=('hhid', lambda x: '2019Q1'),
    pid='indiv',
)

myvars = dict(
    Attainment=('s2aq9', lambda s: extract_string(s)),
)

df = df_data_grabber('../Data/sect2_harvestw4.dta', idxvars, **myvars)
df = df.rename(columns={'Attainment': 'Educational Attainment'})
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'individual_education.parquet')
