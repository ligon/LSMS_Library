"""Concatenate wave-level plot_features for Benin (GH #167; EHCVM cluster).

Benin has a single EHCVM wave in this repo (2018-19; there is no
2021-22 EHCVM-II wave).  The wave's ``Benin/2018-19/_/plot_features.py``
writes a parquet indexed by ``(t, i, plot_id)`` with the canonical
plot_features columns; this script concatenates whatever waves have one.
``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.

Like the Mali concatenator, this does NOT apply ``id_walk`` here.
Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2018-19']

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
