"""Concatenate wave-level plot_features for Niger (GH #167; EHCVM cluster).

Each EHCVM wave's ``Niger/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical plot_features columns.
This script concatenates the waves that have one (2018-19, 2021-22; the
pre-EHCVM ECVMA waves 2011-12 / 2014-15 use a different agriculture
instrument and are deferred to a separate recipe).  ``v`` is NOT baked
in — the framework joins it from ``sample()`` at API time.

As in the Mali reference (PR #284), this does NOT apply ``id_walk``
here.  Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.  Baking id_walk at
cache-write time risks colliding "moved" panel households onto identical
``(t, i, plot_id)`` tuples; leaving the index pre-walk keeps it globally
unique and lets the framework's idempotent id_walk handle the panel
remap consistently with every other Niger table.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

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
