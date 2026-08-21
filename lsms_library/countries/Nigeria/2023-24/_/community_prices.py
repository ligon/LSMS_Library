#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2023-24 (W5) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
Post Harvest Wave 5/Community/sectc8_harvestw5.dta).  CLUSTER-level (no
household i); v = cluster_id(state, lga, ea).  W5 item_cd is the consumption-module scheme
already in harmonize_food (resolved via Code); the per-row unit label is
c8aq2_b, the reported price c8aq3.  t = 2024Q1 (post-harvest quarter).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave, _crop_labels

crop_labels = _crop_labels()

t = PH_QUARTER['2023-24']
f = '../Data/Post Harvest Wave 5/Community/sectc8_harvestw5.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=dec, ea='ea', item='item_cd', price='c8aq3', unit='c8aq2_b')],
    mode='codes', crop_labels=crop_labels)

assert df.index.is_unique, "community_prices W5: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
