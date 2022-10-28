import pandas as pd
from pandas.io import stata
from cfe.df_utils import df_to_orgtbl
import dvc.api

#'2010':{'fn':'../2010-11/Data/GSEC15c.dta','itmcd':'H15CQ2'},
#'2011':{'fn':'../2011-12/Data/GSEC15C.dta','itmcd':'H15CQ2'},

Rounds = {'2005':{'fn':'../2005-06/Data/GSEC14B.dta','itmcd':'H14BQ2'},
          '2009':{'fn':'../2009-10/Data/GSEC15c.dta','itmcd':'H15CQ2'},
          '2013':{'fn':'../2013-14/Data/GSEC15C.dta','itmcd':'itmcd'},
          '2015':{'fn':'../2015-16/Data/gsec15c.dta','itmcd':'itmcd'},
          '2018':{'fn':'../2018-19/Data/GSEC15C.dta','itmcd':'CEC02'},
          '2019':{'fn':'../2019-20/Data/HH/gsec15c.dta','itmcd':'CEC02'}}

    
D = {}
for k,v in Rounds.items():
    with dvc.api.open(v['fn'],mode='rb') as dta:
        sr = stata.StataReader(dta)
    D[k] = sr.value_labels()[v['itmcd']]

D = pd.DataFrame(D).T.sort_index().T.sort_index()
D.index.name = "Code"

print(df_to_orgtbl(D))
