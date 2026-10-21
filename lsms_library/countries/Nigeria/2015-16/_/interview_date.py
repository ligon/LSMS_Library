"""Interview date for Nigeria 2015-16 (GHS-Panel W3), both rounds.

The cover page records the date as a day / month / year triple
(``saq13d`` / ``saq13m`` / ``saq13y``), as in W1/W2.  W3 differs in one
respect: ``saq13m`` carries Stata value labels (``9 -> '9. SEPTEMBER'``), so
a label-decoding read hands ``pd.to_datetime`` the TEXT ``'9. SEPTEMBER'``,
which it cannot assemble -- every W3 row became NaT and ``dropna(how='all')``
deleted both rounds (GH #1037).  The file is therefore read with
``convert_categoricals=False``: the month is the stored integer code, read at
the point it is read rather than parsed back out of a label.  (The label's
leading integer equals the code on every non-blank row, 4,675/4,675 PP and
4,592/4,592 PH, so nothing but the representation changes.)

The PH file also ships ``saq13`` as ``ddmmyyyy`` text.  It is NOT used: the
triple is what W1/W2 serve, and the two agree on 4,587 of the 4,590 PH rows
where both parse (the three that disagree are recorded in CONTENTS.org).

The household id is the ``i`` level, as declared in ``data_scheme.yml``.
"""
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet


##Planting 2015Q3
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2015Q3"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d')
df_1 = df_data_grabber('../Data/secta_plantingw3.dta',idxvars,
                       convert_categoricals=False,**myvars)
df_1['int_t'] = pd.to_datetime(df_1[['year', 'month', 'day']], errors='coerce')
df_1=df_1.drop(columns=['year','month','day'])


##Harvest(2016Q1)
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2016Q1"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d')

df = df_data_grabber('../Data/secta_harvestw3.dta',idxvars,
                     convert_categoricals=False,**myvars)

df['int_t'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df=df.drop(columns=['year','month','day'])


df = pd.concat([df,df_1])
to_parquet(df,'interview_date.parquet')
