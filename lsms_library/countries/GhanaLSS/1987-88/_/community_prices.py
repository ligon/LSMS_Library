#!/usr/bin/env python
"""community_prices for GhanaLSS 1987-88 (GLSS1) -- GH #562 phase 3a.

Source: ``Data/PRICE.DAT`` (comma-delimited with a header despite the
fixed-width ``PRICE.DCT``; the library reader detects that -- CONTENTS.org).
One record per (CLUST, ITEMNO) with three vendor observations
``QUANn``/``PRICEn`` (n = 1..3).  ``PRICEnU`` (= PRICEn/QUANn), ``DEFL`` and
``PRICE`` (the deflated mean) are CALCULATED fields (BID §6.2) and are not
stored.  ``TYPRES`` (BID's 7-way location type) and ``MOINT``/``YRINT`` (the
price interview's month/year) are not stored either; the month orders repeat
records.

* v   = CLUST (1001-1200), the cluster the price form is keyed on ("LOCALITY /
        CLUSTER"); sample().v keyspace.  BID §2.3: prices were to be
        collected at the market nearest each locality; 165 of 176 clusters.
* j   = ITEMNO decoded through harmonize_price_item (the 47-item form).
* u   = the form's basis per item: KG for foods (weighed, BID §2.3), each
        for eggs, TABLETS for the four drugs, the DESCRIPTION unit otherwise.
* NumberOfUnits = QUANn (x the form's lot size where the description is a
        multi-unit lot, e.g. 6 yards of cloth).
* Description   = the form's item label.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from glss_prices import build_glss12

WAVE = '1987-88'

if __name__ == '__main__':
    df = build_glss12(WAVE, '../Data/PRICE.DAT')
    to_parquet(df, 'community_prices.parquet')
