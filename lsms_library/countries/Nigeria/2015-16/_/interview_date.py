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

``InterviewResult`` is ``saq22a``, the response status of the questionnaire, read
as a code like the month (GH #1096).  ``saq22b`` is the status of data entry, and
the PH cover's ``saq22`` is a yes/no on observations.
"""
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
sys.path.append('../../_/')
from lsms_library.local_tools import (assign_datetime, datetime_reports,
                                      df_data_grabber, to_parquet)
from nigeria import response_status_label


##Planting 2015Q3
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2015Q3"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq22a', response_status_label))
df_1 = df_data_grabber('../Data/secta_plantingw3.dta',idxvars,
                       convert_categoricals=False,**myvars)


##Harvest(2016Q1)
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2016Q1"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq22a', response_status_label))

df = df_data_grabber('../Data/secta_harvestw3.dta',idxvars,
                     convert_categoricals=False,**myvars)

df = pd.concat([df,df_1])
# The NBS Q13 covers use the Gregorian civil day/month/year
# (Nigeria/_/CONTENTS.org, GH #1045). Keep the legacy round tags on t.
components = df[['year', 'month', 'day']]
df = assign_datetime(df.drop(columns=components.columns), 'int_t',
                     components.rename(columns={'year': 'y', 'month': 'm', 'day': 'd'}),
                     calendar='gregorian', resolution='day', country='Nigeria',
                     table='interview_date', wave_folder='2015-16')
# InterviewResult must not carry an undated visit. A parsed partial date
# has the registered key and is not a status-only row (GH #1096).
columns = ['int_t', 'InterviewResult']
if 'Derivation' in df:
    columns.append('Derivation')
df = df.dropna(subset=['int_t'])[columns]
to_parquet(df, 'interview_date.parquet',
           datetime_audit=datetime_reports('Nigeria', 'interview_date'))
