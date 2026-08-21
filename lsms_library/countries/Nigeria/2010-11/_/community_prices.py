#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2010-11 (W1) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
sectc8_harvestw1.csv).  CLUSTER-level (no household i); v = cluster_id(state, lga, ea) is the
COMPOSITE community EA id (GH #323), the SAME keyspace as sample().v.  W1 uses the
community questionnaire's OWN item-code scheme (mapped by name to canonical
harmonize_food Preferred Labels) and carries no unit column (the unit is the
per-item fixed questionnaire unit).  t = 2011Q1 (post-harvest quarter).

Registered as a wave-level script so the build flows through the framework's
wave path (getattr(wave, 'community_prices')()), which preserves the (t,v,j,u)
grain -- the country-level fallback path's map_index() would swap the j level
to i for a j-without-i index.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave

t = PH_QUARTER['2010-11']
f = '../Data/sectc8_harvestw1.csv'
raw = get_dataframe(f, convert_categoricals=False)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=raw, ea='ea', item='item_cd', price='sc8q2', unit=None)],
    mode='names')

assert df.index.is_unique, "community_prices W1: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
