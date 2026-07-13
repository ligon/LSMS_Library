#!/usr/bin/env python
"""Extract cluster-level features (Region, District, Rural) for the 2008-15 multi-round data."""
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

# `i` (the HOUSEHOLD) -- not `j`, which means ITEM everywhere else in this
# library.  The household level is deliberately emitted even though the declared
# index is (t, v): Region/District/Rural are HOUSEHOLD attributes (where the
# household was interviewed), and cluster_features declares
# `aggregation: majority` in data_scheme.yml, so the framework needs the
# household-grain BALLOT in order to take the vote.  See GH #323.
idxvars = dict(i='r_hhid',
               t=('round', round_match),
               v='clusterid')

myvars = dict(Rural=('urb_rur', lambda x: 'Rural' if x.lower() != 'urban' else 'Urban'),
              Region='ha_01_1',
              District='ha_02_2')

df = df_data_grabber('../Data/upd4_hh_a.dta', idxvars, **myvars)

# The source is keyed (UPHI, round) -- the panel-tracking LINE -- so one
# household-round is replicated once per descendant line (29,250 source rows ->
# 16,599 distinct (i, t, v)).  Collapse the replication explicitly so each
# household casts exactly ONE vote in the majority reduction above.  Safe by
# construction, and asserted: geo is constant within (i, t, v).
df = df.sort_index()
_g = df.groupby(level=['i', 't', 'v'], observed=True)
assert (_g.nunique(dropna=False).max() <= 1).all(), \
    'cluster_features: Region/District/Rural vary within (i, t, v) -- the ' \
    'household ballot is no longer well defined (GH #323)'
df = df.loc[~df.index.duplicated(keep='first')]

to_parquet(df, 'cluster_features.parquet')
