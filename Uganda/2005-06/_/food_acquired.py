#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired

fn='../Data/GSEC14A.dta'

myvars = dict(item='h14aq2',
              HHID='HHID',
              market='h14aq12',
              farmgate='h14aq13',
              value_home='h14aq5',
              value_away='h14aq7',
              value_own='h14aq9',
              value_inkind='h14aq11',
              quantity_home='h14aq4',
              quantity_away='h14aq6',
              quantity_own='h14aq8',
              quantity_inkind='h14aq10',
              units='h14aq3')

df = food_acquired(fn,myvars)

df.to_parquet('food_acquired.parquet')
