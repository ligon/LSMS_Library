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

# other_features: only Rural (Region/District belong in cluster_features)
myvars = dict(Rural=('urb_rur',lambda x: 'Rural' if x.lower()!='urban' else 'Urban'))

df = df_data_grabber('../Data/upd4_hh_a.dta',idxvars,**myvars)

# Splitoffs in later rounds retroactively added to earlier rounds.
# This leads to double-counting if we're focused on households.
# Drop this retroactive additions.

df = df.sort_index().droplevel('uphi')
df = df.loc[~df.index.duplicated(keep='first')]

assert df.index.is_unique, "Non-unique index!  Fix me!"

# Save backward-compatible other_features with m index (domain as region)
to_parquet(df,'other_features.parquet')

# cluster_features: Rural + Region + District (Region/District normalized
# via categorical_mapping.org at API read time)
cf_myvars = dict(Rural=('urb_rur',lambda x: 'Rural' if x.lower()!='urban' else 'Urban'),
                 Region='ha_01_1',
                 District='ha_02_2')

cf = df_data_grabber('../Data/upd4_hh_a.dta',idxvars,**cf_myvars)
cf = cf.sort_index().droplevel('uphi').droplevel('m')
cf = cf.loc[~cf.index.duplicated(keep='first')]
to_parquet(cf, 'cluster_features.parquet')
