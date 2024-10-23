import sys
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
import sys
sys.path.append('../../_')

with dvc.api.open('../Data/hh_sec_e1.dta',mode='rb') as dta:
    df = from_dta(dta)

labels = {'interview__key': 'int_key',
          'y5_hhid': 'HHID',
          'indidy5': 'pid',
          'hh_e01_1': '5 years and older?',
          'hh_e01': 'Answering for themselves?',
          'hh_e02': 'Person reporting',
          'hh_e03': 'Work for wage?',
          'hh_e04': 'Hours worked for wage',
          'hh_e05': 'Work in own non-ag HH business?',
          'hh_e06': 'Hours worked for own non-ag HH business',
          'hh_e06a': 'Help in non-ag HH business?',
          'hh_e06b': 'Hours helped in non-ag HH business',
          'hh_e07': 'Work in HH ag?',
          'hh_e08': 'Hours worked in HH ag',
          'hh_e09': 'Product use',
          'hh_e11': 'Work unpaid?',
          'hh_e13': 'Have job that will return to',
          'hh_e14': 'Main reason not working',
          'hh_e22': 'Want to work?',
          'hh_e23': 'Main activity (not work)',
          'hh_e24': 'Main reason not looking for job',
          'hh_e29': 'Wage employer, main',
          'hh_e30b_1a': 'Type of occupation (1), main',
          'hh_e30b_2a': 'Type of occupation (2), main',
          'hh_e30b_3a': 'Type of occupation (3), main',
          'hh_e30b_4a': 'Type of occupation (4), main',
          'hh_e31b_1': 'Sector of business (1), main',
          'hh_e31b_2': 'Sector of business (2), main',
          'hh_e31b_3': 'Sector of business (3), main',
          'hh_e31b_4': 'Sector of business (4), main',
          'hh_e33': 'Receive wages, main?',
          'hh_e34': 'Main reason for no wage, main',
          'hh_e35a': 'Last payment (amount), main',
          'hh_e35b': 'Last payment (unit), main',
          'hh_e36': 'Any other payment, main?',
          'hh_e37a': 'Other payment (amount), main',
          'hh_e37b': 'Other payment (unit), main',
          'hh_e38': 'Months worked last year, main',
          'hh_e39': 'Usual weeks worked per month, main',
          'hh_e40': 'Usual hours worked per week, main',
          'hh_e41': 'Hours workd last week, main',
          'hh_e42': 'Job has contract, main?',
          'hh_e43': 'Type of contract, main',
          'hh_e44a': 'Paternity leave, main',
          'hh_e44b': 'Paid sick leave, main',
          'hh_e44c': 'Paid annual leave, main',
          'hh_e44d': 'Withhold taxes from wages, main',
          'hh_e44e': 'Health insurace, main',
          'hh_e45': 'Any other wage job?',
          'hh_e46': 'Wage employer, secondary',
          'hh_e47b_1': 'Type of occupation (1), secondary',
          'hh_e47b_2': 'Type of occupation (2), secondary',
          'hh_e47b_3': 'Type of occupation (3), secondary',
          'hh_e47b_4': 'Type of occupation (4), secondary',
          'hh_e48b_1': 'Sector of business (1), secondary',
          'hh_e48b_2': 'Sector of business (2), secondary',
          'hh_e48b_3': 'Sector of business (3), secondary',
          'hh_e48b_4': 'Sector of business (4), secondary',
          'hh_e49': 'Receive wages, secondary?',
          'hh_e50': 'Main reason for no wage, secondary',
          'hh_e51a': 'Last payment (amount), secondary',
          'hh_e51b': 'Last payment (unit), secondary',
          'hh_e52': 'Any other payment, secondary?',
          'hh_e53a': 'Other payment (amount), secondary',
          'hh_e53b': 'Other payment (unit), secondary',
          'hh_e54': 'Hours worked last week, secondary',
          'hh_e55': 'Job has contract, secondary?',
          'hh_e56': 'Type of contract, secondary',
          'hh_e57': 'Member of trade union?',
          'hh_e59': 'Able to work more hours?'}
        
labor = df[labels.keys()].rename(columns = labels).dropna(how = 'all')

labor = labor.set_index(['HHID','pid'])

labor.to_parquet('labor_recent.parquet')