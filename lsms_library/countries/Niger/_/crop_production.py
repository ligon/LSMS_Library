"""Concatenate wave-level crop_production for Niger (GAP 1, item-level).

Each wave's ``Niger/<wave>/_/crop_production.py`` writes a parquet indexed
by ``(t, i, plot, crop, u)`` with the reported harvest columns.  This
script concatenates the four waves (2011-12, 2014-15, 2018-19, 2021-22),
each of which has a crop/harvest module.  ``v`` is NOT baked in — the
framework joins it from ``sample()`` at API time.

As in the plot_features sibling, this does NOT apply ``id_walk`` here;
the framework runs it in ``_finalize_result`` on every read.  The
``(t, i, plot, crop, u)`` index is intentionally NON-unique: a single
(plot, crop) may carry several reported harvest lines (different units /
records), and collapsing them would be a sum — forbidden by the
item-level / reported-values discipline.  Duplicate rate stays well under
the diagnostics tolerance.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for crop_production (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, 'crop_production: no wave-level parquets found'

p = pd.concat(pieces)

to_parquet(p, '../var/crop_production.parquet')
