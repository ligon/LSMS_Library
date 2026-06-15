"""Build Nigeria 2010-11 individual_education (GH individual_education).

Education (GHS section 2) is collected only in the post-harvest round,
so this single-file feature maps to the PH quarter t=2011Q1, matching
the post-harvest slice of household_roster (i=hhid, pid=indiv).

Source file:
  - Post Harvest Wave 1/Household/sect2a_harvestw1.dta (t=2011Q1)

Educational Attainment is the highest qualification (s2aq9), e.g.
'p6', 'ss3', 'nce', '1st degree'.  Raw per-wave codes are retained
(canonical individual_education allows raw labels).  Coverage in W1 is
low (~640 rows): the W1 questionnaire only asked the qualification of
members who had left school.
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
    t=('hhid', lambda x: '2011Q1'),
    pid='indiv',
)

myvars = dict(
    Attainment=('s2aq9', lambda s: extract_string(s)),
)

df = df_data_grabber(
    '../Data/Post Harvest Wave 1/Household/sect2a_harvestw1.dta',
    idxvars, **myvars,
)
df = df.rename(columns={'Attainment': 'Educational Attainment'})
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'individual_education.parquet')
