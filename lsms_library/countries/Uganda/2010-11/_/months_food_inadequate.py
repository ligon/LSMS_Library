#!/usr/bin/env python
"""months_food_inadequate (Family C) for Uganda 2010-11.

GSEC17A 'Welfare and Food Security'.  The food-deprivation item is
h17q9 ('In the past 12 months, was there a time when you faced a
situation when you did not have enough food to feed the household?',
Yes/No).  This wave's only GSEC17 follow-up (GSEC17B) records *reasons*
(h17q11b), not which months, so MonthsInadequate is not observed and is
left <NA>; AnyInadequate carries the binary deprivation item.
"""
from lsms_library.local_tools import to_parquet, get_dataframe
import pandas as pd

t = '2010-11'

df = get_dataframe('../Data/GSEC17A.dta')

any_inadequate = df['h17q9'].map({'Yes': True, 'No': False})

out = pd.DataFrame({
    'i': df['HHID'].astype(str).values,
    't': t,
    'MonthsInadequate': pd.array([pd.NA] * len(df), dtype='Int64'),
    'AnyInadequate': any_inadequate.astype('boolean').values,
})

out = out.dropna(subset=['AnyInadequate']).set_index(['i', 't'])
out = out.astype({'MonthsInadequate': 'Int64', 'AnyInadequate': 'boolean'})

to_parquet(out, 'months_food_inadequate.parquet')
