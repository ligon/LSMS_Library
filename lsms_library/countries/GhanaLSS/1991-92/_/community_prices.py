#!/usr/bin/env python
"""community_prices for GhanaLSS 1991-92 (GLSS3) -- GH #562 phase 3a.

Source: ``Data/Prices/G3PRICE.DTA`` -- one row per observation already:
``clust`` (3016-3796), ``item`` (1-117), ``p`` (the per-unit value, = the
form's PRICE / KG, e.g. 500/7 -- the weighed KG itself is not distributed),
``time`` (the survey month, 1-12; 251 of 256 clusters have one value) and
``loc5`` (the 5-way locality type, identical to POV_GH.DTA's).  Typically
three rows per (clust, item) = the form's three observations; a few
(clust, item) carry six or nine rows (a second visit) -> ``obs`` 4..9.

* v  = clust, the EA the form is keyed on ("REGION / DISTRICT / NAME OF
       LOCALITY / EA"); 256 of the 365 household clusters.
* j  = item decoded through harmonize_price_item.  CAUTION: the price form
       this wave ships is the 123-item GLSS4 instrument; G3PRICE has 117
       codes, and the table is the GLSS4 list minus six items, reconstructed
       by price/count alignment (CONTENTS.org "Community price survey").
* u  = the form's basis per item (KG for most foods; beer bottle, 300 ml
       bottle, 720 ml, tablets, packets, yards ... as the form states).
* NumberOfUnits = the form's basis count (Basis column): 1 for a kg or a
       single container, 10 for "10 tablets", 6 for "6 yards", 0.170 for
       the evaporated-milk tin.
* Description = the form's item label.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from glss_prices import build_glss34

WAVE = '1991-92'

if __name__ == '__main__':
    df = build_glss34(WAVE, '../Data/Prices/G3PRICE.DTA', price_col='p', time_col='time')
    to_parquet(df, 'community_prices.parquet')
