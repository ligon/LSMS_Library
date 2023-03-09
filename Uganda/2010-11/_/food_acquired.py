#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired

fn='../Data/GSEC15b.dta'

myvars = dict(item='itmcd',                 # Code label uniquely identifying food
              HHID='hh',                    # Unique household id
              market='h15bq12',             # Market price
              farmgate='h15bq13',           # Farmgate price
              value_home='h15bq5',          # Total value of food purchased consumed at home
              value_away='h15bq7',          # Total value of food consumed away from home
              value_own='h15bq9',           # Value of food consumed out of own production
              value_inkind='h15bq11',       # Value of food received (and consumed) in kind
              quantity_home='h15bq4',       # Quantity of food consumed at home
              quantity_away='h15bq6',       # Quantity of food consumed away from home
              quantity_own='h15bq8',        # Quantity of food consumed out of own production
              quantity_inkind='h15bq10',    # Quantity of consumed food received in kind
              units='untcd')                # Units in which quantities are measured

df = food_acquired(fn,myvars)

df.to_parquet('food_acquired.parquet')
