"""Concatenate wave-level plot_features for Togo (GH #167; EHCVM cluster).

Togo's single EHCVM wave (2018) writes a parquet indexed by
``(t, i, plot_id)`` with the canonical plot_features columns from
``Togo/2018/_/plot_features.py``.  This script concatenates the wired
waves.  ``v`` is NOT baked in — the framework joins it from
``sample()`` at API time.

Following the Mali reference (PR #284), this does NOT apply ``id_walk``
here.  Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.  (Togo has a single
wave with no panel remap, but the no-id_walk convention is kept for
consistency across the EHCVM cluster.)
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2018']

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
