#!/usr/bin/env python
"""Concatenate wave-level community_prices for Nigeria GHS-Panel (GAP C).

Item-level reported community food prices at grain (t, v, j, u) from the
post-harvest COMMUNITY questionnaire's food-price module (Section C8).
CLUSTER-level (no household i); v = cluster_id(state, lga, ea) is the community EA id -- the COMPOSITE key
(GH #323: the bare `ea` serial is unique only within an LGA), the SAME
keyspace as sample().v, so prices join households via their cluster.
Stores the REPORTED surveyed Price ONLY -- no index, no cross-cluster
median/mean, no HH-median imputation (those are transformations).

Each year folder's ``<wave>/_/community_prices.py`` produces a wave-level
parquet at the post-harvest quarter (2011Q1 / 2013Q1 / 2016Q1 / 2019Q1 /
2024Q1); this script concatenates them.  The wave-level split (mirroring
food_acquired and Tanzania's community_prices) routes the build through the
framework's wave path, which preserves the (t, v, j, u) grain -- the
country-only fallback path applies map_index(), which swaps a j-without-i
index level to i and collapses the table.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

# Year folders, each contributing exactly its post-harvest quarter.
WAVE_FOLDERS = ['2010-11', '2012-13', '2015-16', '2018-19', '2023-24']

pieces = []
for w in WAVE_FOLDERS:
    fn = f'../{w}/_/community_prices.parquet'
    try:
        pieces.append(get_dataframe(fn))
    except Exception:
        # Wave-level parquet not built yet; skip (the framework wave path
        # builds it on demand, but a direct country build may run first).
        continue

assert pieces, "community_prices: no wave-level parquets found"

df = pd.concat(pieces, axis=0).sort_index()
# A food item may be priced under more than one base unit in a cluster, but
# (t, v, j, u) must be unique; the wave scripts already collapse same-cell
# duplicates.  Guard against any cross-wave overlap (there is none -- each
# wave owns a distinct PH quarter).
assert df.index.is_unique, "community_prices: (t,v,j,u) not unique after concat"

to_parquet(df, '../var/community_prices.parquet')
