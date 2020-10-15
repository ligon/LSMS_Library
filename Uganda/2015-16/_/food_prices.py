import sys
sys.path.append('../../_')
from uganda import prices_and_units


myvars = {'fn':'Uganda/2015-16/Data/GSEC15B.dta',
          'units':'untcd',
          'item':'itmcd',
          'HHID':'hh',
          'market':'h15bq12',
          'farmgate':'h15bq13'}
        

prices_and_units(**myvars)
