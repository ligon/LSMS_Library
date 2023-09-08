#!/usr/bin/env python

import sys
sys.path.append('../../_/')
from burkina_faso import age_sex_composition
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
import pyreadstat

x = []

def sexconverter(x):
    if x == 'Masculin':
        return 'm'
    if x == 'Feminin' or x == 'Féminin':
        return 'f'

variables = [
    {'sex': 'B2', 'age': 'B4', 't': '2013_Q4'},
    {'sex': 'B2', 'age': 'B4', 't': '2014_Q1'},
    {'sex': 'sexe3', 'age': 'age3', 't': '2014_Q2'},
    {'sex': 'B2', 'age': 'B4B', 't': '2014_Q3'},
    ]

for i in np.arange(1,5):
    round = variables[i-1]

    filestring = 'emc2014_p'+str(i)+'_individu_27022015.dta'
    fs = dvc.api.DVCFileSystem('../../')
    fs.get_file('/Burkina_Faso/2014/Data/'+filestring, '/tmp/'+filestring)
    df, meta_r = pyreadstat.read_dta('/tmp/'+filestring, apply_value_formats = True, formats_as_category = True)

    df["hhid"]  = df["zd"].astype(str) + '-'  + df["menage"].astype(int).astype(str)
    regions  = df.groupby('hhid').agg({'region' : 'first'})
    regions['region'] = regions['region'].str.capitalize().str.replace(' ', '-')

    df = age_sex_composition(df, sex=round['sex'], sex_converter=sexconverter, age=round['age'], age_converter=None, hhid='hhid')
    df = pd.merge(left = df, right = regions, how = 'left', left_index = True, right_index = True)

    df = df.rename(columns = {'region' : 'm'})
    df['t'] = round['t']
    df = df.set_index(['t', 'm'], append = True)
    x.append(df)

concatenated = pd.concat(x)

concatenated.columns.name = 'k'

concatenated.to_parquet('household_characteristics.parquet')
