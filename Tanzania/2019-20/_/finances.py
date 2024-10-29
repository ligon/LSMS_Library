import sys
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
import sys
sys.path.append('../../_')

with dvc.api.open('../Data/HH_SEC_Q1.dta',mode='rb') as dta:
    df = from_dta(dta)

labels = {'sdd_hhid': 'HHID',
          'hh_q01_1': 'Use M-PESA?',
          'hh_q01_2': 'Use EZY-PESA?',
          'hh_q01_3': 'Use AIRTEL?',
          'hh_q01_4': 'Use TIGO PESA?',
          'hh_q01_5': 'Use T PESA?',
          'hh_q01_6': 'Use HALLO PESA?',
          'hh_q02': 'How often use mobile money',
          'hh_q03_1': 'Buy airtime for yourself',
          'hh_q03_2': 'Buy airtime for someone else',
          'hh_q03_3': 'Send money',
          'hh_q03_4': 'Receive money',
          'hh_q03_5': 'Someone pay you for good or service',
          'hh_q03_6': 'Save for emergencies',
          'hh_q03_7': 'Save for everyday expenses',
          'hh_q03_8': 'Save for large purchases',
          'hh_q04': 'Most important use of MM',
          'hh_q05_1': 'Main source of cash (1)',
          'hh_q05_2': 'Main source of cash (2)',
          'hh_q06': 'Income from rental payments',
          'hh_q07': 'Income from pensions',
          'hh_q08': 'Income from other sources',
          'hh_q09_1': 'Other income (type 1)',
          'hh_q09_2': 'Other income (type 2)',
          'hh_q09_3': 'Other income (type 3)',
          'hh_q10': 'Bank account?',
          'hh_q11_1': 'Bank account (institution 1)',
          'hh_q11_2': 'Bank account (institution 2)',
          'hh_q11_3': 'Bank account (institution 3)',
          'hh_q12': 'Year opened bank account',
          'hh_q13_1': 'Reason for no bank (1)',
          'hh_q13_2': 'Reason for no bank (2)',
          'hh_q13_3': 'Reason for no bank (3)',
          'hh_q14': 'Received remittances?'}

finance = df[labels.keys()].rename(columns = labels).dropna(how = 'all')

finance = finance.set_index(['HHID'])

# replace "none" with 0's
columns_to_convert = ['Income from rental payments', 'Income from pensions', 'Income from other sources']
for column in columns_to_convert:
    finance[column] = finance[column].apply(lambda x: 0 if str(x).strip().lower() == 'none' else x)
    finance[column] = pd.to_numeric(finance[column], errors='coerce')

finance.to_parquet('finances.parquet')