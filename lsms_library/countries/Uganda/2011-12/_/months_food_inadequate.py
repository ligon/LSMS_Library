#!/usr/bin/env python
"""months_food_inadequate (Family C) for Uganda 2011-12.

GSEC17A 'Welfare and Food Security'.  Deprivation item h17q9
('In the past 12 months, was there a time when you faced a situation
when you did not have enough food to feed the household?', Yes/No).
The which-months follow-up GSEC17B is long-format: one row per month
(h17q10b = month name) with h17q10a = Yes/No.  MonthsInadequate is the
count of months flagged Yes.
"""
from lsms_library.local_tools import to_parquet, get_dataframe
import pandas as pd

t = '2011-12'

dep = get_dataframe('../Data/GSEC17A.dta')
any_inadequate = (
    dep.assign(AnyInadequate=dep['h17q9'].map({'Yes': True, 'No': False}))
       .set_index(dep['HHID'].astype(str))['AnyInadequate']
)
any_inadequate.index.name = 'i'

mon = get_dataframe('../Data/GSEC17B.dta')
mon['i'] = mon['HHID'].astype(str)
mon['flag'] = mon['h17q10a'].map({'Yes': True, 'No': False})
months = mon.groupby('i')['flag'].sum().astype('Int64')
months.name = 'MonthsInadequate'

out = pd.concat([any_inadequate, months], axis=1)
# No month rows but answered the binary -> 0 if not inadequate.
out['MonthsInadequate'] = out['MonthsInadequate'].fillna(0).astype('Int64')
out.loc[out['AnyInadequate'] == False, 'MonthsInadequate'] = 0

out = out.reset_index()
out['t'] = t
out = out.dropna(subset=['AnyInadequate']).set_index(['i', 't'])
out = out.astype({'MonthsInadequate': 'Int64', 'AnyInadequate': 'boolean'})

to_parquet(out, 'months_food_inadequate.parquet')
