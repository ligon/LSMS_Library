#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np

# Shock dataset
df = get_dataframe('../Data/HH_SEC_R.dta')

# Filter for households that experienced the shock
df = df[df['hh_r01'] == 'YES']

# Map combined effect variable to separate AffectedIncome and AffectedAssets
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
    'AffectedIncome': df.hh_r03.map(effect_income_map).values.tolist(),
    'AffectedAssets': df.hh_r03.map(effect_assets_map).values.tolist(),
    'HowCoped0': df.hh_r04_1.values.tolist(),
    'HowCoped1': df.hh_r04_2.values.tolist(),
})

shocks.insert(1, 't', '2019-20')

shocks = shocks.set_index(['t', 'i', 'Shock'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# i is sdd_hhid, the wave's household id -- the SAME namespace as this wave's
# sample()/household_roster (502 of 502 distinct i overlap).  Contrast
# 2008-15/_/shocks.py, which keyed on the panel LINE (UPHI) and overlapped the
# household namespace by ZERO until GH #637.
# HH_SEC_R.dta after the hh_r01=='YES' filter is (sdd_hhid, shock_id)-unique:
# 864 rows, 864 groups, 0 duplicates.  .first() is never called.
if not shocks.index.is_unique:
    shocks = shocks.groupby(level=shocks.index.names).first()

to_parquet(shocks, 'shocks.parquet')
