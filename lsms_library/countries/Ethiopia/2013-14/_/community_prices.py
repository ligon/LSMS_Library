#!/usr/bin/env python
"""Build community_prices for Ethiopia ESS 2013-14 (Wave 2; GAP C).

Item-level surveyed FOOD prices at (t, v, j, u) from the §10 community
price questionnaire.  W2 uses a single price file (sect10b2_com_w2;
sect10b1 carries the retail-outlet metadata).  Item (cs10b2q01) and unit
(cs10b2q03) are value LABELS; bare Weight (cs10b2q04) and Price
(cs10b2q05).  v = ea_id2 (the wave-2 EA id that matches sample().v;
the W1-era ea_id does NOT).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import community_prices_for_wave


prices = get_dataframe('../Data/sect10b2_com_w2.dta', convert_categoricals=True)

colmap = dict(
    v='ea_id2',
    item='cs10b2q01',
    unit='cs10b2q03',
    price='cs10b2q05',
    quantity='cs10b2q04',
)

df = community_prices_for_wave('2013-14', prices, colmap)

assert len(df) > 0, "community_prices 2013-14 produced no rows"
to_parquet(df, 'community_prices.parquet')
