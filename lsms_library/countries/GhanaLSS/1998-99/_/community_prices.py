#!/usr/bin/env python
"""community_prices for GhanaLSS 1998-99 (GLSS4) -- GH #562 phase 3a.

Source: ``Data/Prices/G4PRICE.DTA`` -- one row per observation: ``clust``
(4002-4999), ``item`` (1-123, the form's codes), ``price`` (the per-unit
value; no KG column, no labels) and ``loc5``.  No survey-month column.
Typically three rows per (clust, item); 744 (clust, item) pairs carry six
and 108 nine rows -> ``obs`` 4..9.  Same reading as 1991-92 (the two waves
share the instrument): the form's basis per item supplies ``u`` and
``NumberOfUnits``.  251 of the 253 price clusters are household clusters.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from glss_prices import build_glss34

WAVE = '1998-99'

if __name__ == '__main__':
    df = build_glss34(WAVE, '../Data/Prices/G4PRICE.DTA', price_col='price', time_col=None)
    to_parquet(df, 'community_prices.parquet')
