#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from ligonlibrary.dataframes import from_dta
import numpy as np

# Shock dataset
with dvc.api.open('../Data/HH_SEC_R.dta', mode='rb') as dta:
    df = from_dta(dta)

# Filter for households that experienced the shock
df = df[df['hh_r01'] == 'YES']

# Map combined effect variable to separate EffectedIncome and EffectedAssets
effect_income_map = {
    'INCOME LOSS': True,
    'Income loss': True,
    'income loss': True,
    'ASSET LOSS': False,
    'Asset loss': False,
    'asset loss': False,
    'LOSS OF BOTH': True,
    'Loss of both': True,
    'loss of both': True,
    'NEITHER': False,
    'Neither': False,
    'neither': False,
}

effect_assets_map = {
    'INCOME LOSS': False,
    'Income loss': False,
    'income loss': False,
    'ASSET LOSS': True,
    'Asset loss': True,
    'asset loss': True,
    'LOSS OF BOTH': True,
    'Loss of both': True,
    'loss of both': True,
    'NEITHER': False,
    'Neither': False,
    'neither': False,
}

shocks = pd.DataFrame({
    'i': df.sdd_hhid.values.tolist(),
    'Shock': df.shock_id.values.tolist(),
    'EffectedIncome': df.hh_r03.map(effect_income_map).values.tolist(),
    'EffectedAssets': df.hh_r03.map(effect_assets_map).values.tolist(),
    'HowCoped0': df.hh_r04_1.values.tolist(),
    'HowCoped1': df.hh_r04_2.values.tolist(),
})

shocks.insert(1, 't', '2019-20')

shocks = shocks.set_index(['t', 'i', 'Shock'])

# Handle duplicates by keeping first occurrence
if not shocks.index.is_unique:
    shocks = shocks.groupby(level=shocks.index.names).first()

to_parquet(shocks, 'shocks.parquet')
