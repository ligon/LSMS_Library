#!/usr/bin/env python
"""months_food_inadequate (Family C) for Uganda 2009-10.

GSEC17 'Welfare and Food Security'.  The food-deprivation item is
h17q09 ('In the past 12 months, was there a time when you faced a
situation when you did not have enough food to feed the household?',
Yes/No).  The which-months follow-up h17q10 is a letter string
(A=Jan .. L=Dec); MonthsInadequate is the count of distinct month
letters.  (h17q11 records *reasons*, letters that run beyond L, so it
is not used here.)
"""
from lsms_library.local_tools import to_parquet, get_dataframe
import pandas as pd

t = '2009-10'

df = get_dataframe('../Data/GSEC17.dta')

MONTH_LETTERS = set('ABCDEFGHIJKL')

def count_months(s):
    if pd.isna(s):
        return 0
    return len({c.upper() for c in str(s)} & MONTH_LETTERS)

any_inadequate = df['h17q09'].map({'Yes': True, 'No': False})
months = df['h17q10'].apply(count_months).astype('Int64')

# If the household says No to the deprivation item, force 0 months.
months = months.where(any_inadequate != False, 0)

out = pd.DataFrame({
    'i': df['HHID'].astype(str).values,
    't': t,
    'MonthsInadequate': months.values,
    'AnyInadequate': any_inadequate.astype('boolean').values,
})

out = out.dropna(subset=['AnyInadequate']).set_index(['i', 't'])
out = out.astype({'MonthsInadequate': 'Int64', 'AnyInadequate': 'boolean'})

to_parquet(out, 'months_food_inadequate.parquet')
