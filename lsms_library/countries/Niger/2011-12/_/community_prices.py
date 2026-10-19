"""Build community_prices for Niger ECVMA 2011-12 (GAP C, item-level).

Sources: ecvmacoms07_p1.dta (passage 1) + ecvmacoms07_p2.dta (passage 2) — the
community price questionnaire section CS07 (food/market prices), collected in
both field passages.  Layout differs from 2014-15: each of the THREE surveyed
observations is a (price, quantity, UNIT) triple with its OWN unit column:
  grappe                    -> v (EA/cluster, into the sample() v keyspace)
  cs07q01                   -> j (item, via the shared harmonize_food labels)
  (cs07q03, cs07q04, cs07q05)  observation 1 (Price, Quantity, unit)
  (cs07q06, cs07q07, cs07q08)  observation 2
  (cs07q09, cs07q10, cs07q11)  observation 3

Both passages are read, and every recorded price is served (GH #840) —
none selected, none averaged: `variety` is the cs07q02 variety code, `visit`
the passage (p1 = 1, p2 = 2), `obs` the observation slot 1 / 2 / 3.
Passage 2 re-ships 150 of its 232 grappes as a second block: 13,478 of those
rows are byte-identical and dropped (lossless); the 22 that differ are kept as
obs 4-6.  Rows with no usable price, or a missing-marker unit ('produit
absent' / 'manquant' -> 'Unknown' since GH #842), are dropped.  Measured: 795 price-bearing
observations in this wave (218 passage 1, 577 passage 2) carry no unit value
at all, and none of them carries a usable Quantity either -- neither a unit
nor a weight, so no basis to be a price OF anything.  See
niger.py::_finish_community_prices.

CLUSTER-level feature (no household i): `v` is declared in the index, NOT
framework-joined.  Index = (t, v, j, u, variety, visit, obs).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (_community_prices_maps, _community_price_triples,
                   _finish_community_prices)


item_map, unit_map = _community_prices_maps()

base = '../Data/NER_2011_ECVMA_v01_M_Stata8/'

# 2011-12: each observation carries its own unit column.
triples = [
    ('cs07q03', 'cs07q04', 'cs07q05'),
    ('cs07q06', 'cs07q07', 'cs07q08'),
    ('cs07q09', 'cs07q10', 'cs07q11'),
]

pieces = []
for fn, passage in [('ecvmacoms07_p1.dta', 1), ('ecvmacoms07_p2.dta', 2)]:
    src = get_dataframe(base + fn, convert_categoricals=True)
    # Same file, raw codes: the cs07q02 variety CODE (GH #840).
    codes = get_dataframe(base + fn, convert_categoricals=False)
    pieces.append(_community_price_triples(src, item_map, unit_map, triples,
                                           passage=passage, codes=codes))

df = pd.concat(pieces, ignore_index=True)
df = _finish_community_prices(df, '2011-12')

assert len(df) > 0, 'community_prices 2011-12 produced no rows'
to_parquet(df, 'community_prices.parquet')
