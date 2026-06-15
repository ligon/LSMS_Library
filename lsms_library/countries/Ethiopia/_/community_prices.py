#!/usr/bin/env python
"""Concatenate wave-level community_prices data for Ethiopia (GAP C).

Each wave's ``Ethiopia/<wave>/_/community_prices.py`` produces a parquet
with index ``(t, v, j, u)`` and columns (Price, Quantity) -- the surveyed
cluster FOOD price from the §10 community price questionnaire.  This
script concatenates them across the five ESS waves.

Grain is ``(t, v, j, u)`` -- CLUSTER-level, with NO household ``i``.  So,
unlike crop_production / livestock / plot_* (which carry ``i`` and need
the cross-wave ``id_walk`` to align with the panel household scheme),
community_prices needs NO id conversion: ``v`` is the EA/cluster id and
already lives in the SAME keyspace as ``sample().v`` for the same wave
(ea_id for W1/W4/W5, ea_id2 for W2/W3 -- emitted by the wave scripts to
match cluster_features / sample()).  The framework's ``_join_v_from_sample``
does NOT apply (it gates on ``i`` being in the index), so ``v`` is native
and joins households via ``sample()`` at query time.

``j`` carries ``harmonize_food`` Preferred Labels and ``u`` the shared
``u`` table labels, so community_prices joins ``food_acquired`` /
``crop_production`` on the shared (j, u) axes.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/community_prices.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/community_prices.parquet')
