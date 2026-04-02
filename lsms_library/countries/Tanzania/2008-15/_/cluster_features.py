#!/usr/bin/env python
"""Extract cluster-level features (Region, District, Rural) for the 2008-15 multi-round data."""
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

idxvars = dict(j='r_hhid',
               t=('round', round_match),
               v='clusterid')

myvars = dict(Rural=('urb_rur', lambda x: 'Rural' if x.lower() != 'urban' else 'Urban'),
              Region='ha_01_1',
              District='ha_02_2')

df = df_data_grabber('../Data/upd4_hh_a.dta', idxvars, **myvars)

# Splitoffs in later rounds retroactively added to earlier rounds.
# Drop duplicates keeping first occurrence.
df = df.sort_index()
df = df.loc[~df.index.duplicated(keep='first')]

to_parquet(df, 'cluster_features.parquet')
