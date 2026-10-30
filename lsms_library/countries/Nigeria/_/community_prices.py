#!/usr/bin/env python
"""Concatenate wave-level community_prices for Nigeria GHS-Panel (GAP C).

Item-level reported community food prices at grain (t, v, j, u, variety, obs) from
the post-harvest COMMUNITY questionnaire's food-price module (Section C8);
`variety` is W1/W2's native item/size code (unknown_variety elsewhere);
`obs` is the C8 reading slot (W3's 1st / 2nd location, 1 elsewhere; GH #637).
CLUSTER-level (no household i); v = cluster_id(state, lga, ea) is the community EA id -- the COMPOSITE key
(GH #323: the bare `ea` serial is unique only within an LGA), the SAME
keyspace as sample().v, so prices join households via their cluster.
Stores the REPORTED surveyed Price ONLY -- no index, no cross-cluster
median/mean, no HH-median imputation (those are transformations).

Each year folder's ``<wave>/_/community_prices.py`` produces a wave-level
parquet at the post-harvest quarter (2011Q1 / 2013Q1 / 2016Q1 / 2019Q1 /
2024Q1); this script concatenates them.  The wave-level split (mirroring
food_acquired and Tanzania's community_prices) routes the build through the
framework's wave path, which preserves the (t, v, j, u, variety, obs) grain -- the
country-only fallback path applies map_index(), which swaps a j-without-i
index level to i and collapses the table.
"""
import pandas as pd

from lsms_library import Country

from lsms_library.local_tools import get_dataframe, to_parquet

# Year folders, each contributing exactly its post-harvest quarter.
WAVE_FOLDERS = ['2010-11', '2012-13', '2015-16', '2018-19', '2023-24']

country = Country('Nigeria', preload_panel_ids=False)
declared_folders = set()
for quarter in country.waves:
    wave = country[quarter]
    if 'community_prices' in wave.data_scheme:
        declared_folders.add(wave.wave_folder)

pieces = []
for w in WAVE_FOLDERS:
    if w not in declared_folders:
        continue
    fn = f'../{w}/_/community_prices.parquet'
    pieces.append(get_dataframe(fn))

assert pieces, "community_prices: no wave-level parquets found"

df = pd.concat(pieces, axis=0).sort_index()
# A food item may be priced under more than one base unit in a cluster, but
# (t, v, j, u, variety, obs) must be unique; the wave builder guarantees it
# per wave.
# Guard against any cross-wave overlap (there is none -- each wave owns a
# distinct PH quarter).
assert df.index.is_unique, "community_prices: (t,v,j,u,variety,obs) not unique after concat"

to_parquet(df, '../var/community_prices.parquet')
