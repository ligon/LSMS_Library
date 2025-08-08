#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
sys.path.append('../../_')


with dvc.api.open('../Data/gsec4.dta',mode='rb') as dta:
    df = from_dta(dta)

labels = {'hhid': 'j',
          'pid': 'pid',
          'h4q4': 'Read and Write?',
          'h4q5': 'Ever attended any formal school?',
          'h4q6': 'Why has not attended school?',
          'h4q7': 'Highest Grade/Class Completed',
          'h4q8': 'Main Reason for Leaving School', 
          'h4q9': 'Grade/Class Attending Last Year',
          'h4q10': 'Grade/Class Attending Currently',
          'h4q10b': 'Name of School Attending Currently',
          'h4q11': 'School Management Authority',
          'h4q12': 'Type of School',
          'h4q12b': 'Boarding or Day Section of School',
          'h4q13': 'Distance to School in km',
          'h4q14': 'Time in min',
          'h4q14b': 'Transportation',
          'h4q15a': 'School and Registration Fees',
          'h4q15b': 'Uniform Fee',
          'h4q15c': 'Book Fee',
          'h4q15d': 'Transportation Fee',
          'h4q15e': 'Boarding Fee',
          'h4q15f': 'Other Fee',
          'h4q15g': 'Total Education Expenditure',
          'h4q16': 'Scholarship or Subsidy',
          'h4q17': 'Source of funding',
          'h4q18': 'Meals at School'}

ed = df[labels.keys()].rename(columns = labels).dropna(how = 'all')

ed = ed.set_index(['j','pid'])

ed_expense = ed[['School and Registration Fees','Uniform Fee',
                 'Book Fee','Transportation Fee', 
                 'Boarding Fee','Other Fee',
                 'Total Education Expenditure']]

ed_expense = ed_expense.replace({0: np.nan}).dropna(how='all')

#ed_expense = ed_expense.groupby('j').sum()

to_parquet(ed_expense, 'education_expenses.parquet')
