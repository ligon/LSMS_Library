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

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


country = Country('Ethiopia', preload_panel_ids=False)
pieces = []
seen_folders = set()
for t in country.waves:
    wave = country[t]
    folder = wave.wave_folder
    if folder in seen_folders:
        continue
    seen_folders.add(folder)
    # A wave can legitimately omit this table. Use its actual script/YAML
    # declarations, rather than treating a failed declared read as absence.
    if 'community_prices' not in wave.data_scheme:
        continue
    fn = f'../{folder}/_/community_prices.parquet'
    # Declared failures must stop before publishing an incomplete country table
    # (ledger ethiopia-1287-community-declarations.md §§2–5).
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "community_prices: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/community_prices.parquet')
