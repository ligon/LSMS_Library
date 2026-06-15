"""community_prices for Tanzania NPS 2019-20 (NPS-SDD Extended Panel;
parity-loop GAP C).

Item-level community/market prices at grain (t, v, j, u) from the COMMUNITY
price questionnaire item file CM_SEC_F_ID.dta.  Same construction as 2020-21
(see that wave's docstring + tanzania.community_prices_for_wave); the variable
names (interview__key / item_id / cm_f061 / cm_f062 / cm_f063) and the 1..52
community-price item code space are identical across waves, only the cased file
name differs.

  v     = interview__key (community cluster id; does NOT intersect sample().v --
          see the data-limitation note in community_prices_for_wave, issue #113).
  j     = canonical Preferred Label from harmonize_community_price (REUSES the
          harmonize_food / harmonize_crop labels so j joins food_acquired /
          crop_production).
  u     = native unit (cm_f061) -> canonical Preferred Label.
  Price = reported unit price = cm_f063 / cm_f062 (currency per one native u).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import community_prices_for_wave


idf = get_dataframe('../Data/CM_SEC_F_ID.dta', convert_categoricals=False)

colmap = dict(
    cluster='interview__key', item='item_id',
    unit='cm_f061', qty='cm_f062', price='cm_f063')

df = community_prices_for_wave('2019-20', idf, colmap)
assert df.index.is_unique, "community_prices 2019-20: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
