#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2018-19 (W4) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
sectc8_harvestw4.dta).  CLUSTER-level (no household i); v = format_id(ea).
W4 item_cd is the consumption-module scheme already in harmonize_food
(resolved via Code); c8q2 is the per-row unit label, c8q3 the reported price.
t = 2019Q1 (post-harvest quarter).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave, _crop_labels

crop_labels = _crop_labels()

t = PH_QUARTER['2018-19']
f = '../Data/sectc8_harvestw4.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=dec, ea='ea', item='item_cd', price='c8q3', unit='c8q2')],
    mode='codes', crop_labels=crop_labels)

assert df.index.is_unique, "community_prices W4: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
