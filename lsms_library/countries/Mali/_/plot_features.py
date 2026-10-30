"""Concatenate wave-level plot_features for Mali (GH #167; EHCVM cluster).

Each EHCVM wave's ``Mali/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical plot_features columns.
This script concatenates the waves that have one (2018-19, 2021-22; the
pre-EHCVM EACI waves 2014-15 / 2017-18 use a different instrument and
are deferred).  ``v`` is NOT baked in — the framework joins it from
``sample()`` at API time.

Unlike Uganda's concatenator, this does NOT apply ``id_walk`` here.
Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.  Mali's 2021-22
panel has a handful of "moved" households whose 2021-22 id maps to the
SAME 2018-19 baseline id as another household — applying id_walk at
cache-write time would collide those onto identical ``(t, i, plot_id)``
tuples (9 dups observed) and bake a non-unique index into the parquet.
Leaving the index pre-walk keeps it globally unique; the framework's
idempotent id_walk handles the panel remap (and the inherent panel
collisions) consistently with every other Mali table.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/plot_features: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(p, '../var/plot_features.parquet')
