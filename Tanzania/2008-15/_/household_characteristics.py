#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster


with dvc.api.open('../Data/upd4_hh_b.dta',mode='rb') as dta:
    df = from_dta(dta)

year1 = df.loc[df['round'] == 1]
year2 = df.loc[df['round'] == 2]
year3 = df.loc[df['round'] == 3]
year4 = df.loc[df['round'] == 4]

round_match = {'2008-09':year1, '2010-11':year2, '2012-13':year3, '2014-15':year4}


def process_each_year(df):
    # Match Uganda FCT categories
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    
    df = get_household_roster(df, sex='hb_02', sex_converter=None,
                            age='hb_04',age_converter=None,
                            HHID='r_hhid',
                            convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    df.index.name = 'j'
    df.columns.name = 'k'
    df = df.filter(regex='ales ')    
    df['log HSize'] = np.log(df.sum(axis=1))

    # Drop any obs with infinities...
    df = df.loc[np.isfinite(df.min(axis=1)),:]
    return df

year_df = {k: v.pipe(process_each_year) for k, v in round_match.items()}

z={}
for t in ['2008-09','2010-11','2012-13','2014-15']:
    z[t] = year_df[t]
    z[t] = z[t].stack('k')
    z[t] = z[t].reset_index().set_index(['j','k']).squeeze()

z = pd.DataFrame(z)
z = z.stack().unstack('k')
z.index.names=['j','t']
z = z.reset_index().set_index(['j','t'])


z.to_parquet('household_characteristics.parquet')
