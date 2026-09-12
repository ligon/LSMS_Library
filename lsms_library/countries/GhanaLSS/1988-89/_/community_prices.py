#!/usr/bin/env python
"""community_prices for GhanaLSS 1988-89 (GLSS2) -- GH #562 phase 3a.

Same instrument and file layout as 1987-88 (the shipped price questionnaire
is byte-identical to GLSS1's; ``PRICE.DAT`` has no ``TYPRES`` column and one
extra item code 48 that is on no form we hold).  ``v`` = CLUST (2006-2798).
Two clusters were priced twice -- 2305 in June and September 1989, 2310
twice in one month -- and those repeat records become ``obs`` 4-6 (ordered by
the interview month); cluster 2726's rows are dated March 1988, a first-year
price carried into the second-year set (BID p.17).  See
``../../1987-88/_/community_prices.py`` for the reading of the form.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from glss_prices import build_glss12

WAVE = '1988-89'

if __name__ == '__main__':
    df = build_glss12(WAVE, '../Data/PRICE.DAT')
    to_parquet(df, 'community_prices.parquet')
