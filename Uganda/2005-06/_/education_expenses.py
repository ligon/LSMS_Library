from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
sys.path.append('../../_')


with dvc.api.open('../Data/GSEC4.dta',mode='rb') as dta:
    df = from_dta(dta)

labels = {'HHID': 'j',
          'PID': 'pid',
          'h4q2': 'Ever attended any formal school?',
          'h4q3': 'Why has not attended school?',
          'h4q4': 'Highest Grade/Class Completed',
          'h4q5': 'Main Reason for Leaving School', 
          'h4q6': 'Grade/Class Attending Currently',
          'h4q7': 'School Management Authority',
          'h4q8': 'Type of School',
          'h4q9': 'Distance to School in km',
          'h4q10a': 'School and Registration Fees',
          'h4q10b': 'Uniform Fee',
          'h4q10c': 'Book Fee',
          'h4q10d': 'Boarding Fee',
          'h4q10e': 'Other Fee',
          'h4q10f': 'Total Education Expenditure',
          'h4q11': 'Scholarship or Subsidy',
          'h4q12': 'Read and Write?'}

ed = df[labels.keys()].rename(columns = labels).dropna(how = 'all')

ed = ed.set_index(['j','pid'])

ed_expense = ed[['School and Registration Fees','Uniform Fee',
                 'Book Fee','Boarding Fee','Other Fee',
                 'Total Education Expenditure']]

ed_expense = ed_expense.replace({0: np.nan}).dropna(how='all')

#ed_expense = ed_expense.groupby('j').sum()

to_parquet(ed_expense, 'education_expenses.parquet')
