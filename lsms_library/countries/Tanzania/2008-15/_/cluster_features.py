#!/usr/bin/env python
"""Extract cluster-level features (Region, District, Rural) for the 2008-15 multi-round data."""
import sys
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}


def blank_to_na(x):
    """An empty string is MISSING DATA, not a value (GH #323).

    Rounds 1 and 2 of the NPS never populate the district NAME: `ha_02_2` is ''
    for all 6,128 round-1 rows and all 8,163 round-2 rows (rounds 3-4 carry real
    names).  Stata writes an unasked string question as '', and carrying that
    through means shipping an empty string as a district NAME -- 818 cluster
    cells (every cluster in 2008-09 and 2010-11) were served exactly that.  It
    also poisons any reduction: '' sorts and votes like a real label.

    Coerce to pd.NA so the absence is honest and `pd.isna()` finds it.  Applied
    to Region as well as District -- Region happens to be clean in all four
    rounds today, but nothing guarantees a future wave will be.
    """
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    return pd.NA if s == '' else s

# `i` (the HOUSEHOLD) -- not `j`, which means ITEM everywhere else in this
# library.  r_hhid is a household id; declaring it as `j` mislabels the grain and
# routes the household->cluster reduction through the generic declared-index
# collapse instead of the household-to-cluster collapse in `Wave.cluster_features`
# that actually owns it.  The source grain IS the household: Region/District/Rural
# come from the household cover page (where the household was INTERVIEWED), while
# `v` is its ORIGINAL sampling EA, which the NPS panel carries forward unchanged
# when it TRACKS a mover or a split-off -- so the geo columns are household
# attributes and are NOT constant within a cluster (round 1, before anyone has
# moved: 0 conflicted clusters; rounds 2-4: 229 / 337 / 63).  Naming the level `i`
# puts the collapse where the framework can see and audit it.  See GH #323.
idxvars = dict(i='r_hhid',
               t=('round', round_match),
               v='clusterid')

myvars = dict(Rural=('urb_rur', lambda x: 'Rural' if x.lower() != 'urban' else 'Urban'),
              Region=('ha_01_1', blank_to_na),
              District=('ha_02_2', blank_to_na))

df = df_data_grabber('../Data/upd4_hh_a.dta', idxvars, **myvars)

# The source is keyed (UPHI, round) -- the panel-tracking LINE -- so one
# household-round is replicated once per descendant line (29,250 source rows ->
# 16,540 distinct (i, t); groups run from 1 to 11 lines).  Collapse that
# replication explicitly so the frame carries exactly one row per household-round.
# Value-preserving by construction, and ASSERTED rather than assumed: if a future
# wave ever makes the geo columns vary across a household's own panel lines, this
# fails LOUDLY instead of silently keeping an arbitrary one.  GH #323.
df = df.sort_index()
_g = df.groupby(level=['i', 't', 'v'], observed=True)
assert (_g.nunique(dropna=False).max() <= 1).all(), \
    'cluster_features: Region/District/Rural vary within (i, t, v) -- the ' \
    'dedup below would keep an arbitrary household record (GH #323)'
df = df.loc[~df.index.duplicated(keep='first')]

to_parquet(df, 'cluster_features.parquet')
