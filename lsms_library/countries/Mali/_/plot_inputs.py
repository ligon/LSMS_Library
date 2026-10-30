"""Concatenate wave-level plot_inputs for Mali (GAP 2; parity loop).

Each EACI wave's ``Mali/<wave>/_/plot_inputs.py`` writes a parquet indexed
by ``(t, i, plot, input, crop)`` with the reported item-level input columns
(Quantity, u, Purchased, Quantity_purchased, Improved).  The crop/input
module lives in the EACI waves only (2014-15 EACI14, 2017-18 EACI17); the
EHCVM waves (2018-19, 2021-22) carry no agriculture-input block, so they
contribute nothing — the same wave split as crop_production.

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
