"""Build community_prices for Niger ECVMA 2014-15 (GAP C, item-level).

Source: comprixcs07.dta — the community price questionnaire, section CS07
(food/market prices).  One row per (grappe, item) with up to THREE surveyed
(price, quantity) observations sharing ONE unit column:
  grappe     -> v (the EA/cluster, into the sample() v keyspace via format_id)
  cs07q01    -> j (item, via the shared harmonize_food Preferred Labels)
  cs07q03    -> u (the single unit for all three observations, via the u table)
  (cs07q04, cs07q05) (cs07q06, cs07q07) (cs07q08, cs07q09)
             -> the three (Price, Quantity) observation pairs

One REPORTED price is selected per (t, v, j, u) (this single-passage wave has
no post-harvest/post-planting split, so the questionnaire row order breaks
ties), mirroring the Mali EACI reference — NOT averaged (a mean is a
transformation).  Rows with no usable price, or a missing-marker unit
('produit absent'/'manquant' -> 'Manquant'), are dropped.

This is a CLUSTER-level feature (no household i): `v` is declared in the index
and is NOT framework-joined.  Index = (t, v, j, u).
"""
import sys

sys.path.append('../../_/')

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (_community_prices_maps, _community_price_triples,
                   _finish_community_prices)


item_map, unit_map = _community_prices_maps()

base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
src = get_dataframe(base + 'comprixcs07.dta', convert_categoricals=True)

# 2014-15: one shared unit column (cs07q03) across the three (price, qty) pairs.
triples = [
    ('cs07q04', 'cs07q05', 'cs07q03'),
    ('cs07q06', 'cs07q07', 'cs07q03'),
    ('cs07q08', 'cs07q09', 'cs07q03'),
]
df = _community_price_triples(src, item_map, unit_map, triples)
df = _finish_community_prices(df, '2014-15')

assert len(df) > 0, 'community_prices 2014-15 produced no rows'
to_parquet(df, 'community_prices.parquet')
