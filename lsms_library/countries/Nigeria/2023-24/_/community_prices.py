#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2023-24 (W5) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
Post Harvest Wave 5/Community/sectc8_harvestw5.dta).  CLUSTER-level (no
household i); v = cluster_id(state, lga, ea).  W5 item_cd is the consumption-module scheme
already in harmonize_food (resolved via Code); the per-row unit label is
c8aq2_b, the reported price c8aq3, the coded SIZE c8aq2_c (with c8aq2_a the
quantity and c8aq2_cvn the kg/l factor agreeing with the code; GH #834:
metric sizes are divided through to per-g / per-cl, and a metric-unit row
with no recorded size -- the 133 rows on '2. GRAMS (G)' and 49 on
'4. CENTLITRES (CL)' (GH #587) -- is dropped, not
served mis-scaled).
t = 2024Q1 (post-harvest quarter).

Post-planting round (GH #587, rulings 2026-11-12): the PP COMMUNITY
questionnaire prices food too, in Section C2 (Post Planting Wave 5/Community/sectc2_plantingw5.dta), so
this script also serves t = 2023Q3 through the PP adapter
(../../_/pp_community_prices.py): j / u on Nigeria's shared labels, the native
size in `Size`, Price on the form's own basis -- quantity of one, ruled (c2q2b unit, c2q2c size, c2q3 Naira; c2q2a Quantity disclosed).
Distinct t per round (pp-ph skill).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave, _crop_labels
from pp_community_prices import combine_rounds, pp_community_prices_for_folder

crop_labels = _crop_labels()

t = PH_QUARTER['2023-24']
f = '../Data/Post Harvest Wave 5/Community/sectc8_harvestw5.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=dec, ea='ea', item='item_cd', price='c8aq3', unit='c8aq2_b',
    size='c8aq2_c')],
    mode='codes', crop_labels=crop_labels)

# The post-planting round's Section C2 (GH #587), its own t (2023Q3).
df = combine_rounds(df, pp_community_prices_for_folder(
    '2023-24', lambda f: get_dataframe(f, convert_categoricals=False)))

assert df.index.is_unique, "community_prices W5: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
