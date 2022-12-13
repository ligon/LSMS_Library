#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired

fn='../Data/GSEC15B.dta'

myvars = dict(item='itmcd',
              HHID='HHID',
              market='h15bq12',
              farmgate='h15bq13',
              value_home='h15bq5',
              value_away='h15bq7',
              value_own='h15bq9',
              value_inkind='h15bq11',
              quantity_home='h15bq4',
              quantity_away='h15bq6',
              quantity_own='h15bq8',
              quantity_inkind='h15bq10',
              units='untcd')

df = food_acquired(fn,myvars)

df.to_parquet('food_acquired.parquet')
