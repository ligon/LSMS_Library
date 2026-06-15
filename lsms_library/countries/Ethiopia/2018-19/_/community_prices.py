#!/usr/bin/env python
"""Build community_prices for Ethiopia ESS 2018-19 (Wave 4; GAP C).

Item-level surveyed FOOD prices at (t, v, j, u) from the §10 community
price questionnaire (sect10b_com_w4).  Item (cs10bq02) and unit
(cs10bq03) value-LABELS carry a numeric code prefix ("1. Teff",
"1. Kilogram") that the shared helper strips before the crosswalk.  The
availability gate cs10bq00 ("is this item available in the market") is
honored -- rows not flagged "1. Yes" are dropped.  Bare Weight (cs10bq04)
and Price (cs10bq05).  v = ea_id (matches sample().v for W4).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import community_prices_for_wave


prices = get_dataframe('../Data/sect10b_com_w4.dta', convert_categoricals=True)

colmap = dict(
    v='ea_id',
    item='cs10bq02',
    unit='cs10bq03',
    price='cs10bq05',
    quantity='cs10bq04',
    avail='cs10bq00', avail_yes='Yes',
)

df = community_prices_for_wave('2018-19', prices, colmap)

assert len(df) > 0, "community_prices 2018-19 produced no rows"
to_parquet(df, 'community_prices.parquet')
