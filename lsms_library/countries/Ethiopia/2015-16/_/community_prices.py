#!/usr/bin/env python
"""Build community_prices for Ethiopia ESS 2015-16 (Wave 3; GAP C).

Item-level surveyed FOOD prices at (t, v, j, u) from the §10 community
price questionnaire.  W3 uses a single price file (sect10a2_com_w3;
sect10a1 carries the retail-outlet metadata).  Item (cs10a2q01) and unit
(cs10a2q03) are raw LABEL strings (no Stata value labels); bare Weight
(cs10a2q04) and Price (cs10a2q05).  v = ea_id2 (the wave-3 EA id that
matches sample().v).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import community_prices_for_wave


prices = get_dataframe('../Data/sect10a2_com_w3.dta', convert_categoricals=True)

colmap = dict(
    v='ea_id2',
    item='cs10a2q01',
    unit='cs10a2q03',
    price='cs10a2q05',
    quantity='cs10a2q04',
)

df = community_prices_for_wave('2015-16', prices, colmap)

assert len(df) > 0, "community_prices 2015-16 produced no rows"
to_parquet(df, 'community_prices.parquet')
