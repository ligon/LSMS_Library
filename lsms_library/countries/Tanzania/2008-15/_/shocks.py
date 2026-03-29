#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from ligonlibrary.dataframes import from_dta
import numpy as np

# Shock dataset (multi-round file covering rounds 1-4)
with dvc.api.open('../Data/upd4_hh_r.dta', mode='rb') as dta:
    df = from_dta(dta)

# Filter for households that experienced the shock
df = df[df['hr_01'] == 'YES']

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

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

shocks = pd.DataFrame({
    'i': df.UPHI.values.tolist(),
    't': df['round'].values.tolist(),
    'Shock': df.hr_00.values.tolist(),
    'EffectedIncome': df.hr_03.map(effect_income_map).values.tolist(),
    'EffectedAssets': df.hr_03.map(effect_assets_map).values.tolist(),
    'HowCoped0': df.hr_06_1.values.tolist(),
    'HowCoped1': df.hr_06_2.values.tolist(),
    'HowCoped2': df.hr_06_3.values.tolist(),
})

shocks = shocks.replace({'t': round_match})

# Convert household ID to string
shocks['i'] = shocks['i'].astype(int).astype(str)

shocks = shocks.set_index(['t', 'i', 'Shock'])

# Handle duplicates by keeping first occurrence
if not shocks.index.is_unique:
    shocks = shocks.groupby(level=shocks.index.names).first()

to_parquet(shocks, 'shocks.parquet')
