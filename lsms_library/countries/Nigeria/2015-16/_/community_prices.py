#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2015-16 (W3) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8).  W3
splits the module across sectc8a_harvestw3.dta and sectc8b_harvestw3.dta
(different item ranges).  CLUSTER-level (no household i); v = format_id(ea).
W3 item_cd is the consumption-module scheme already in harmonize_food
(resolved via Code); c8q2 is the per-row unit label, c8q3 the reported price.
t = 2016Q1 (post-harvest quarter).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave, _crop_labels

crop_labels = _crop_labels()

t = PH_QUARTER['2015-16']
frames = []
for f in ('../Data/sectc8a_harvestw3.dta', '../Data/sectc8b_harvestw3.dta'):
    raw = get_dataframe(f, convert_categoricals=False)
    dec = get_dataframe(f, convert_categoricals=True)
    frames.append(dict(df=raw, dec=dec, ea='ea', item='item_cd',
                       price='c8q3', unit='c8q2'))

df = community_prices_for_wave(t, frames, mode='codes',
                              crop_labels=crop_labels)

assert df.index.is_unique, "community_prices W3: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
