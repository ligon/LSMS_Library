#!/usr/bin/env python
"""Build community_prices for Ethiopia ESS 2011-12 (Wave 1; GAP C).

Item-level surveyed FOOD prices at (t, v, j, u) from the §10 community
price questionnaire.  W1 fields the price list at TWO retail outlets
(sect10b1 = primary 'Marketplace', sect10b2 = secondary 'Shops/Stalls');
we wire the primary outlet ONLY, to keep one market per cluster and the
strict (t, v, j, u) grain consistent with W2-W5 (each a single price file
per EA).  b2 adds only ~4 EAs and is intentionally left for a future
multi-outlet extension if wanted.

W1 specifics: item (cs10b1q07_b) and unit (cs10b1q07_d) are value LABELS;
price = Birr (cs10b1q07_f1) + cents (cs10b1q07_f2)/100; quantity basis =
whole (cs10b1q07_e1) + decimal (cs10b1q07_e2)/1000.  v = ea_id (matches
sample().v for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import community_prices_for_wave


prices = get_dataframe('../Data/sect10b1_com_w1.dta', convert_categoricals=True)

colmap = dict(
    v='ea_id',
    item='cs10b1q07_b',
    unit='cs10b1q07_d',
    price_whole='cs10b1q07_f1', price_cents='cs10b1q07_f2',
    qty_whole='cs10b1q07_e1',   qty_decimal='cs10b1q07_e2',
)

df = community_prices_for_wave('2011-12', prices, colmap)

assert len(df) > 0, "community_prices 2011-12 produced no rows"
to_parquet(df, 'community_prices.parquet')
