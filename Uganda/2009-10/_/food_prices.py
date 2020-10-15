import sys
sys.path.append('../../_')
from uganda import prices_and_units

myvars = dict(fn='Uganda/2009-10/Data/GSEC15b.dta',item='h15bq2',HHID='hh',market='h15bq12',farmgate='h15bq13',units='untcd')

prices_and_units(**myvars)

