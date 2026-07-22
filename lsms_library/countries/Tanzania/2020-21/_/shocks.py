#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np

# Shock dataset
df = get_dataframe('../Data/hh_sec_r.dta')

# Filter for households that experienced the shock
df = df[df['hh_r01'] == 'yes']

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
    'i': df.y5_hhid.values.tolist(),
    'Shock': df.shockid.values.tolist(),
    'AffectedIncome': df.hh_r03.map(effect_income_map).values.tolist(),
    'AffectedAssets': df.hh_r03.map(effect_assets_map).values.tolist(),
    'HowCoped0': df.hh_r04_1.values.tolist(),
    'HowCoped1': df.hh_r04_2.values.tolist(),
})

shocks.insert(1, 't', '2020-21')

shocks = shocks.set_index(['t', 'i', 'Shock'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# i is y5_hhid, the wave's household id -- the SAME namespace as this wave's
# sample()/household_roster (2,552 of 2,552 distinct i overlap).  Contrast
# 2008-15/_/shocks.py, which keyed on the panel LINE (UPHI) and overlapped the
# household namespace by ZERO until GH #637.
# hh_sec_r.dta after the hh_r01=='yes' filter is (y5_hhid, shockid)-unique:
# 5,067 rows, 5,067 groups, 0 duplicates.  .first() is never called.
if not shocks.index.is_unique:
    shocks = shocks.groupby(level=shocks.index.names).first()

to_parquet(shocks, 'shocks.parquet')
