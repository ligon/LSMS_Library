#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from ligonlibrary.dataframes import from_dta
import numpy as np

# Household roster (multi-round file covering rounds 1-4)
with dvc.api.open('../Data/upd4_hh_b.dta', mode='rb') as dta:
    df = from_dta(dta)

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

roster = pd.DataFrame({
    'i': df.r_hhid.values.tolist(),
    't': df['round'].values.tolist(),
    'pid': df.UPI.values.tolist(),
    'Sex': df.hb_02.values.tolist(),
    'Age': df.hb_04.values.tolist(),
    'Relationship': df.hb_05.values.tolist(),
})

roster = roster.replace({'t': round_match})

# Convert household ID and pid to string
roster['i'] = roster['i'].astype(str)
roster['pid'] = roster['pid'].astype(float).astype(int).astype(str)

roster = roster.set_index(['t', 'i', 'pid'])

# Handle duplicates by keeping first occurrence
if not roster.index.is_unique:
    roster = roster.groupby(level=roster.index.names).first()

to_parquet(roster, 'household_roster.parquet')
