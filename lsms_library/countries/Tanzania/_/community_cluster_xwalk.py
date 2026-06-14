"""Concatenate wave-level community_cluster_xwalk for Tanzania NPS (issue #113).

Each buildable wave's ``Tanzania/<wave>/_/community_cluster_xwalk.py`` produces
a parquet at grain (t, v) -- one row per community price cluster
(``interview__key``) -- mapping it to the household survey cluster
(``sample().v``) where the (region, ward) + interview-date match resolves
uniquely, with a region-level fallback otherwise.  This script concatenates the
per-wave parquets.

There is NO id_walk here: the grain carries no household ``i``, and ``v`` is the
community questionnaire's OWN native cluster id (interview__key), not a
household-panel id.  Only 2019-20 and 2020-21 carry a community questionnaire
(see ``community_prices`` for the same wave-scope note).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2019-20', '2020-21']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/community_cluster_xwalk.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "community_cluster_xwalk: no wave-level parquets found"

p = pd.concat(pieces)
assert p.index.is_unique, "community_cluster_xwalk: (t, v) not unique after concat"

to_parquet(p, '../var/community_cluster_xwalk.parquet')
