"""Concatenate wave-level plot_inputs for Niger (GAP 2, item-level).

Each wave's ``Niger/<wave>/_/plot_inputs.py`` writes a parquet indexed by
``(t, i, input, crop, u)`` with the reported per-input columns.  This
script concatenates the four waves (2011-12, 2014-15, 2018-19, 2021-22),
each of which has a household agricultural-input roster.  ``v`` is NOT
baked in — the framework joins it from ``sample()`` at API time.

As in the crop_production / plot_features siblings, this does NOT apply
``id_walk`` here; the framework runs it in ``_finalize_result`` on every
read.  The ``(t, i, input, crop, u)`` index is intentionally NON-unique
(a household can report the same input on several lines / crops) and the
``crop`` level is partly NaN (the EHCVM waves carry no crop for non-seed
inputs) — both are expected and stay within the diagnostics tolerance.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for plot_inputs (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, 'plot_inputs: no wave-level parquets found'

p = pd.concat(pieces)

to_parquet(p, '../var/plot_inputs.parquet')
