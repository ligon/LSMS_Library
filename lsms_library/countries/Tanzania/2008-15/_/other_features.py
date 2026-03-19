#!/usr/bin/env python
import numpy as np
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

round_match = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}

idxvars = dict(j='r_hhid',
               t=('round',round_match),
               m=('domain',lambda s:s.title()),
               v='clusterid',
               uphi='UPHI')

myvars = dict(Rural=('urb_rur',lambda x: 'Rural' if x.lower()!='urban' else 'Urban'),
              Region=('ha_01_1', lambda s: s.title()),
              District='ha_02_2')

df = df_data_grabber('../Data/upd4_hh_a.dta',idxvars,**myvars)

# Splitoffs in later rounds retroactively added to earlier rounds.
# This leads to double-counting if we're focused on households.
# Drop this retroactive additions.

df = df.sort_index().droplevel('uphi')
df = df.loc[~df.index.duplicated(keep='first')]

assert df.index.is_unique, "Non-unique index!  Fix me!"

# Save backward-compatible other_features with m index (domain as region)
to_parquet(df,'other_features.parquet')

# Save cluster_features version: drop m, keep Region/District/Rural columns
cf = df.droplevel('m')
cf = cf.loc[~cf.index.duplicated(keep='first')]
to_parquet(cf, 'cluster_features.parquet')
