#!/usr/bin/env python
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import df_from_orgfile

fct = read_sheets('https://docs.google.com/spreadsheets/d/1qljY2xrxbc37d9tLSyuFa9CnjEsh3Re2ufDQlBHzPEQ/',
                  sheet='FCT',nheaders=3)
# Drop useless "Name" level
fct.columns.names = fct.columns[0]
fct = fct.droplevel('Name',axis=1)

#find FCT codes for foods in expenditure survey
food_items = df_from_orgfile('./food_items.org')
food_items['FCT code'] = food_items['FCT code'].astype('Int64').astype(str).replace('<NA>',None)
food_items = food_items.rename(columns={'FCT code':'Code'}).set_index('Preferred Label')

useful_fct = food_items.join(fct.droplevel('Unit',axis=1),on='Code',how='inner')

# Clean up mess in column names
useful_fct.columns = useful_fct.columns.str.replace('\\n%*', '', regex=True)
useful_fct = useful_fct.loc[~useful_fct.index.duplicated()]

useful_fct.index.name = 'Food'
useful_fct = useful_fct.fillna(0)

useful_fct.to_parquet('../var/fct.parquet')
