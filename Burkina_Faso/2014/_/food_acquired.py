from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
from burkina_faso import age_sex_composition
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
import pyreadstat

x = []

variables = [
    {'i' : 'product', 'quantity': 'qachat', 'units': 'uachat', 'total expenses': 'autocons',
     'quantity obtained': 'qcadeau', 'units obtained': 'ucadeau','t': '2013_Q4'}
    ]

for i in np.arange(1,2): #change to 5 to get the files for all 4 rounds
    round = variables[i-1]

    filestring = 'emc2014_p'+str(i)+'_conso7jours_16032015.dta'
    fs = dvc.api.DVCFileSystem('../../')
    fs.get_file('/Burkina_Faso/2014/Data/'+filestring, '/tmp/'+filestring)
    df, meta_r = pyreadstat.read_dta('/tmp/'+filestring, apply_value_formats = True, formats_as_category = True)

    df["j"] = df["zd"].astype(str) + df["menage"].astype(int).astype(str).str.rjust(3, '0')
    df = df.set_index('j')

    df = df.loc[:, list(round.values())[:-1]]
    invround = {v: k for k, v in round.items()}

    df = df.rename(invround, axis = 1)
    df["price per unit"] = df["total expenses"]/df["quantity"]
    df['t'] = round['t']
    df = df.set_index(['t', 'i'], append=True)
    df['units'], df['units obtained'] = df['units'].astype(str), df['units obtained'].astype(str)
    x.append(df)

concatenated = pd.concat(x)

concatenated.columns.name = 'k'
#inspect missing encoding for units
concatenated = concatenated.replace('nan', np.nan).dropna(how = 'all')

to_parquet(concatenated, 'food_acquired.parquet')
