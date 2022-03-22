#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import food_expenditures

# See https://microdata.worldbank.org/index.php/catalog/3795/data-dictionary/F93?file_name=GSEC15B.dta
# Note that notations on Q don't seem to match!
myvars = dict(fn='Uganda/2018-19/Data/GSEC15B.dta',item='CEB01',HHID='hhid',
              purchased='CEB07', 
              away='CEB09',
              produced='CEB11',
              given='CEB013')

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

