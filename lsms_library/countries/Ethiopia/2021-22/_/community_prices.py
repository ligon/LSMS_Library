#!/usr/bin/env python
"""Build community_prices for Ethiopia ESS 2021-22 (Wave 5; GAP C).

Item-level surveyed FOOD prices at (t, v, j, u) from the §10 community
price questionnaire (sect10b_com_w5).  Structure identical to W4: item
(cs10bq02) and unit (cs10bq03) value-LABELS carry a numeric code prefix
("1. Teff", "1. Kilogram") stripped by the shared helper; availability
gate cs10bq00 ("1. Yes" / "2. No") honored; bare Weight (cs10bq04) and
Price (cs10bq05).  v = ea_id (matches sample().v for W5).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import community_prices_for_wave


prices = get_dataframe('../Data/sect10b_com_w5.dta', convert_categoricals=True)

colmap = dict(
    v='ea_id',
    item='cs10bq02',
    unit='cs10bq03',
    price='cs10bq05',
    quantity='cs10bq04',
    avail='cs10bq00', avail_yes='Yes',
)

df = community_prices_for_wave('2021-22', prices, colmap)

assert len(df) > 0, "community_prices 2021-22 produced no rows"
to_parquet(df, 'community_prices.parquet')
