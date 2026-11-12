#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2012-13 (W2) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u, variety, obs) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8,
sectc8_harvestw2.csv).  CLUSTER-level (no household i); v = cluster_id(state, lga, ea).
W2 shares W1's community item-code scheme (mapped by name to canonical
harmonize_food labels) and carries no unit column (per-item fixed unit).
t = 2013Q1 (post-harvest quarter).  See the W1 wave script for why this is a
wave-level (not country-level) script.

Post-planting round (GH #587, rulings 2026-11-12): the PP COMMUNITY
questionnaire prices food too, in Section C2 (Post Planting Wave 2/Community/sectc2_plantingw2.dta), so
this script also serves t = 2012Q3 through the PP adapter
(../../_/pp_community_prices.py): j / u on Nigeria's shared labels, the native
size in `Size`, Price on the form's own basis -- quantity of one (c2q2 unit, c2q3 Naira).
Distinct t per round (pp-ph skill).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave
from pp_community_prices import combine_rounds, pp_community_prices_for_folder

t = PH_QUARTER['2012-13']
f = '../Data/sectc8_harvestw2.csv'
raw = get_dataframe(f, convert_categoricals=False)
df = community_prices_for_wave(t, [dict(
    df=raw, dec=raw, ea='ea', item='item_cd', price='c8q2', unit=None)],
    mode='names')

# Native variety is the original item/size code; obs is the single C8 slot.
# The post-planting round's Section C2 (GH #587), its own t (2012Q3).
df = combine_rounds(df, pp_community_prices_for_folder(
    '2012-13', lambda f: get_dataframe(f, convert_categoricals=False)))

assert df.index.is_unique, "community_prices W2: (t,v,j,u,variety,obs) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
