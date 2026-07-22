#!/usr/bin/env python
"""Household roster for Tanzania 2008-15 (multi-round file covering rounds 1-4).

Produces (t, i, pid) indexed roster.  Cluster identity (v) is joined
from sample() at API time, not baked into this parquet.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
import pandas as pd

# Household roster (multi-round file covering rounds 1-4)
df = get_dataframe('../Data/upd4_hh_b.dta')

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

roster = pd.DataFrame({
    'i': df.r_hhid.values.tolist(),
    'round': df['round'].values.tolist(),
    'pid': df.UPI.values.tolist(),
    'Sex': df.hb_02.values.tolist(),
    'Age': df.hb_04.values.tolist(),
    'Relationship': df.hb_05.values.tolist(),
    'MonthsAway': df.hb_10.values.tolist(),
})

# Map round numbers to wave labels
roster['t'] = roster['round'].map(round_match)
roster = roster.drop(columns=['round'])

# Convert IDs to clean strings
roster['i'] = roster['i'].astype(str)
roster['pid'] = roster['pid'].astype(float).astype(int).astype(str)

roster = roster.set_index(['t', 'i', 'pid'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# upd4_hh_b.dta is one of the INDIVIDUAL-level upd4 modules, keyed
# (round, r_hhid, UPI) -- 83,706 rows, 83,706 groups, 0 duplicates, 0 null UPI.
# (It carries no UPHI column, so it escapes the panel-line replication that
# hits the household-level modules -- see housing.py.)  The pid formatting
# (float -> Int64 -> str) is injective: 45,396 distinct UPI -> 45,396 distinct
# pid.  Measured on a cold build, .first() is never reached.
if not roster.index.is_unique:
    roster = roster.groupby(level=roster.index.names).first()

to_parquet(roster, 'household_roster.parquet')
