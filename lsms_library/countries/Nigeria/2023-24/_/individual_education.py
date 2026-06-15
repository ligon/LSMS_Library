"""Build Nigeria 2023-24 individual_education (GHS-Panel Wave 5).

Education (GHS section 2) is post-harvest only -> t=2024Q1, matching the
post-harvest slice of household_roster (i=hhid, pid=indiv).

Source file:
  - Post Harvest Wave 5/Household/sect2_harvestw5.dta (t=2024Q1)

Educational Attainment = s2q3 (highest grade/qualification completed),
recorded as a numeric code in W5 (1..22 plus 3-digit detail codes like
101, 201, 5102).  Raw per-wave codes are retained (canonical
individual_education permits raw labels); the float is rendered as an
integer string ('6', '101', ...) to keep the column typed as str.
~11.9k rows.
"""
import sys
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet


def code_to_str(x):
    if pd.isna(x):
        return pd.NA
    try:
        return str(int(x))
    except (TypeError, ValueError):
        return str(x)


idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2024Q1'),
    pid='indiv',
)

myvars = dict(
    Attainment=('s2q3', code_to_str),
)

df = df_data_grabber(
    '../Data/Post Harvest Wave 5/Household/sect2_harvestw5.dta',
    idxvars, **myvars,
)
df = df.rename(columns={'Attainment': 'Educational Attainment'})
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'individual_education.parquet')
