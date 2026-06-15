"""community_prices for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
parity-loop GAP C).

Item-level community/market prices at grain (t, v, j, u) from the COMMUNITY
price questionnaire item file cm_sec_f_id.dta.  One row per
(community cluster v, harmonized food item j, native unit u), carrying ONLY
the REPORTED village-market unit price.

  v     = interview__key (the community questionnaire's own cluster id; see the
          data-limitation note in tanzania.community_prices_for_wave -- it does
          NOT intersect sample().v, the NPS community and household instruments
          use incompatible cluster coding).
  j     = canonical Preferred Label from harmonize_community_price, which REUSES
          the harmonize_food / harmonize_crop labels so community_prices.j joins
          food_acquired.j and crop_production.j.
  u     = native unit (cm_f061) -> canonical Preferred Label.
  Price = reported unit price = cm_f063 / cm_f062 (currency per one native u).

The district-capital price triple (cm_f064/65/66) is a different geographic
level (the district town) and is NOT folded into the cluster grain.
NO median/mean across clusters, NO community->household imputation -- those are
transformations over these rows.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import community_prices_for_wave


idf = get_dataframe('../Data/cm_sec_f_id.dta', convert_categoricals=False)

colmap = dict(
    cluster='interview__key', item='item_id',
    unit='cm_f061', qty='cm_f062', price='cm_f063')

df = community_prices_for_wave('2020-21', idf, colmap)
assert df.index.is_unique, "community_prices 2020-21: (t,v,j,u) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
