#!/usr/bin/env python
"""Household roster for Tanzania 2008-15 (multi-round file covering rounds 1-4).

Joins cluster ID (v) from the cover page (upd4_hh_a.dta) onto the roster
so the final index is (t, v, i, pid).
"""
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd

# Household roster (multi-round file covering rounds 1-4)
df = get_dataframe('../Data/upd4_hh_b.dta')

# Cover page — contains cluster ID
cover = get_dataframe('../Data/upd4_hh_a.dta')

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

roster = pd.DataFrame({
    'i': df.r_hhid.values.tolist(),
    'round': df['round'].values.tolist(),
    'pid': df.UPI.values.tolist(),
    'Sex': df.hb_02.values.tolist(),
    'Age': df.hb_04.values.tolist(),
    'Relationship': df.hb_05.values.tolist(),
})

# Extract cluster mapping from cover page, keyed by (r_hhid, round)
cluster_map = cover[['r_hhid', 'round', 'clusterid']].drop_duplicates()
cluster_map = cluster_map.rename(columns={'r_hhid': 'i', 'clusterid': 'v'})

# Merge cluster ID onto roster using household ID and round
roster = roster.merge(cluster_map, left_on=['i', 'round'], right_on=['i', 'round'], how='left')

# Map round numbers to wave labels
roster['t'] = roster['round'].map(round_match)
roster = roster.drop(columns=['round'])

# Convert IDs to clean strings
roster['i'] = roster['i'].astype(str)
roster['pid'] = roster['pid'].astype(float).astype(int).astype(str)
roster['v'] = roster['v'].apply(format_id)

roster = roster.set_index(['t', 'v', 'i', 'pid'])

# Handle duplicates by keeping first occurrence
if not roster.index.is_unique:
    roster = roster.groupby(level=roster.index.names).first()

to_parquet(roster, 'household_roster.parquet')
