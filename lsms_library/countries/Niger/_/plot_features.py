"""Concatenate wave-level plot_features for Niger (GH #167; EHCVM cluster).

Each EHCVM wave's ``Niger/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical plot_features columns.
This script concatenates the waves that have one (2018-19, 2021-22; the
pre-EHCVM ECVMA waves 2011-12 / 2014-15 use a different agriculture
instrument and are deferred to a separate recipe).  ``v`` is NOT baked
in — the framework joins it from ``sample()`` at API time.

As in the Mali reference (PR #284), this does NOT apply ``id_walk``
here. The framework owns the exactly-once panel remap; this compiler keeps
the wave outputs' native identifiers and attrs unchanged.

Country/Wave metadata determines declarations: native drivers and YAML keys
both declare a table. Only an undeclared wave is skipped. A declared read
failure must propagate before any country write (GH #1287).
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet


country = Country('Niger')
pieces = []
for t in country.waves:
    wave = country[t]
    # Reuse the actual script/YAML union and logical-to-physical folder map
    # (ledger 1287-niger-declaration-implementation §§2-4).
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_features.parquet'
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(p, '../var/plot_features.parquet')
