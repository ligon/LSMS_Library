"""Concatenate wave-level plot_features for Senegal (GH #167; EHCVM cluster).

Each EHCVM wave's ``Senegal/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical plot_features columns.
This script concatenates the EHCVM waves (2018-19, 2021-22).  ``v`` is
NOT baked in — the framework joins it from ``sample()`` at API time.

Following the Mali reference (PR #284), this does NOT apply ``id_walk``
here.  Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.  Applying id_walk at
cache-write time could collide panel households that remap to the same
baseline id onto identical ``(t, i, plot_id)`` tuples.  Leaving the index
pre-walk keeps it globally unique; the framework's idempotent id_walk
handles the panel remap consistently with every other Senegal table.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for plot_features (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(p, '../var/plot_features.parquet')
