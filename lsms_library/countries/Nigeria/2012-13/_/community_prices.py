#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2012-13 (W2) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
sectc8_harvestw2.csv).  CLUSTER-level (no household i); v = cluster_id(state, lga, ea).
W2 shares W1's community item-code scheme (mapped by name to canonical
harmonize_food labels) and carries no unit column (per-item fixed unit).
t = 2013Q1 (post-harvest quarter).  See the W1 wave script for why this is a
wave-level (not country-level) script.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave

t = PH_QUARTER['2012-13']
f = '../Data/sectc8_harvestw2.csv'
raw = get_dataframe(f, convert_categoricals=False)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=raw, ea='ea', item='item_cd', price='c8q2', unit=None)],
    mode='names')

assert df.index.is_unique, "community_prices W2: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
