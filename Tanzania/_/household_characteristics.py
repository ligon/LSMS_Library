#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves
import dvc.api
from lsms import from_dta

def id_match(df, wave, waves_dict):
    df = df.reset_index()
    if len(waves_dict[wave]) == 3:
        if 'y4_hhid' and 'UPHI' not in df.columns:
            with dvc.api.open('../%s/Data/%s' % (wave,waves_dict[wave][0]),mode='rb') as dta:
                h = from_dta(dta)
            h = h[[waves_dict[wave][1], waves_dict[wave][2]]]
            m = df.merge(h, how = 'left', left_on ='j', right_on =waves_dict[wave][2])

            with dvc.api.open('../2008-15/Data/upd4_hh_a.dta',mode='rb') as dta:
                uphi = from_dta(dta)[['UPHI','r_hhid','round']]
            uphi['UPHI'] = uphi['UPHI'].astype(int).astype(str)
            y4 = uphi.loc[uphi['round']==4, 'r_hhid'].to_frame().rename(columns ={'r_hhid':'y4_hhid'})
            uphi = uphi.join(y4)    
            uphi = uphi[['UPHI', 'y4_hhid']].dropna()
            m = m.merge(uphi, how= 'left', on = 'y4_hhid')

            m['UPHI'].replace('', np.nan, inplace=True)
            m['UPHI'] = m['UPHI'].fillna(m.pop(waves_dict[wave][2]))
            m.j = m.UPHI
            m = m.drop(columns=['UPHI', 'y4_hhid'])
            if 't' not in m.columns:
                m.insert(1, 't', wave)

    if len(waves_dict[wave]) == 4:
        if 'UPHI'  in df.columns: 
            m = df.rename(columns={'UPHI': 'j'})
        else: 
            with dvc.api.open('../%s/Data/%s' % (wave,waves_dict[wave][0]),mode='rb') as dta:
                h = from_dta(dta)
            h = h[[waves_dict[wave][1], waves_dict[wave][2], waves_dict[wave][3]]]
            h[waves_dict[wave][1]] = h[waves_dict[wave][1]].astype(int).astype(str)
            dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
            h.replace({"round": dict},inplace=True)
            m = df.merge(h.drop_duplicates(), how = 'left', left_on =['j','t'], right_on =[waves_dict[wave][2], waves_dict[wave][3]])
            m['UPHI'] = m['UPHI'].fillna(m.pop('j'))
            m = m.rename(columns={'UPHI': 'j'})
            m = m.drop(columns=[waves_dict[wave][2], waves_dict[wave][3]])
    return m


z={}
for t in Waves.keys():
    z[t] = pd.read_parquet('../'+t+'/_/household_characteristics.parquet')
    z[t] = id_match(z[t],t,Waves)

z = pd.concat(z.values())
z['m'] = 'Tanzania'
z = z.reset_index().set_index(['j','t','m'])
z = z.drop(columns ='index')

z.to_parquet('../var/household_characteristics.parquet')
