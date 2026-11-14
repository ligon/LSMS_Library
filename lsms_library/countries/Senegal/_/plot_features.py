"""Concatenate wave-level plot_features for Senegal (GH #167; EHCVM cluster).

Each EHCVM wave's ``Senegal/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical plot_features columns.
This script concatenates the waves whose own metadata declares the table
(2018-19, 2021-22).  ``v`` is NOT baked in — the framework joins it from
``sample()`` at API time.

Following the Mali reference (PR #284), this does NOT apply ``id_walk``
here.  Cached parquets store pre-transformation data; the framework runs
``id_walk`` in ``_finalize_result`` on every read.  Applying id_walk at
cache-write time could collide panel households that remap to the same
baseline id onto identical ``(t, i, plot_id)`` tuples.  Leaving the index
pre-walk keeps it globally unique; the framework's exactly-once id_walk
handles the panel remap consistently with every other Senegal table.

A declared wave is REFUSED, not skipped, when its parquet is absent (GH
#1287, the Burkina Faso form of the #1241 refusal).  Until 2026-11-13 this
script read a hardcoded wave list through ``try: ... except Exception:
continue``, so a declared wave whose script failed or never ran was dropped
and the other wave served alone; ``assert pieces`` fired only when every
wave was missing.  Only a wave whose metadata declares no plot_features (no
wave script and no YAML entry) is skipped, before any read.  This checks
absence, not the freshness of an existing output.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.paths import data_root


country = Country('Senegal')
pieces = []
for t in country.waves:
    wave = country[t]
    # Wave.data_scheme includes both native scripts and YAML declarations.
    # A parquet's presence is never a table declaration.
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = data_root(country.name) / wave.wave_folder / '_' / 'plot_features.parquet'
    if not fn.exists():
        raise FileNotFoundError(
            f'Senegal plot_features: wave {t} declares plot_features '
            f'but its parquet was not built; refusing to concatenate the '
            f'other waves without it (GH #1287). Run that wave script and '
            f'read its error.')
    pieces.append(get_dataframe(fn))

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(p, '../var/plot_features.parquet')
