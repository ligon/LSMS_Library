"""Concatenate wave-level plot_inputs for Mali (GAP 2; parity loop).

Each declared wave's ``Mali/<wave>/_/plot_inputs.py`` writes the native
``(t, i, plot_id, input, crop)`` key. EACI records plot inputs; EHCVM s16b
records household inputs with structural plot_id='Farm-level' (GH #1483).
The two EHCVM waves retain separate gift/purchase amounts and units and
their own literal input labels/codes. EACI producers remain unchanged.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is left to ``_finalize_result`` (cached parquets store
pre-transformation data), matching the crop_production / plot_features /
food_coping pattern.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet

# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'plot_inputs' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/plot_inputs: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot, input, crop) after concat"

to_parquet(p, '../var/plot_inputs.parquet')
