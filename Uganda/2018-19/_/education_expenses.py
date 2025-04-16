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

labels = {'hhid': 'j',
          'PID': 'pid',
          's4q04': 'Read and Write?',
          's4q05': 'Ever attended any formal school?',
          's4q06': 'Why has not attended school?',
          's4q07': 'Highest Grade/Class Completed',
          's4q08': 'Main Reason for Leaving School',
          's4q09': 'Grade/Class Attending Last Year',
          's4q10': 'Grade/Class Attending Currently',
          's4q11': 'School Management Authority',
          's4q12': 'Type of School',
          's4q7_1a': 'Duration of Course',
          's4q7_1b': 'Duration of Course (Unit)',
          's4q07a': 'Current Year Attending',
          's4q12': 'Boarding School',
          's4q13': 'Distance to School in km',
          's4q14': 'Time in min',
          's4q14b': 'Transportation',
          'h4q15h': 'School Fee',
          'h4q15i': 'Registration Fee',
          'h4q15j': 'Exam Fee',
          'h4q15e': 'Boarding Fee',
          'h4q15b': 'Uniform Fee',
          'h4q15c': 'Book Fee',
          'h4q15d': 'Transportation Fee',
          'h4q15k': 'Day Care Facility Fee',
          's4h15f': 'Other Fee',
          'h4q15g_1': 'Fees that Cannot be Broken Down',
          'h4q15g': 'Total Education Expenditure',
          's4q16': 'Scholarship or Subsidy',
          's4q17': 'Source of funding',
          's4q18': 'Meals at School'}

ed = df[labels.keys()].rename(columns = labels).dropna(how = 'all')

ed = ed.set_index(['j','pid'])

ed_expense = ed[['School Fee','Registration Fee','Exam Fee',
                 'Boarding Fee', 'Uniform Fee', 'Book Fee',
                 'Transportation Fee','Day Care Facility Fee',
                 'Other Fee', 'Fees that Cannot be Broken Down',
                 'Total Education Expenditure']]

ed_expense = ed_expense.replace({0: np.nan}).dropna(how='all')

#ed_expense = ed_expense.groupby('j').sum()

to_parquet(ed_expense, 'education_expenses.parquet')
