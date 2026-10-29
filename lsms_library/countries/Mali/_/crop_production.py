"""Concatenate wave-level crop_production for Mali (GAP 1; parity loop).

Each EACI wave's ``Mali/<wave>/_/crop_production.py`` writes a parquet
indexed by ``(t, i, plot, crop)`` with the reported item-level harvest
columns (Quantity, u, Quantity_sold, Value_sold, planting_month,
harvest_month, intercropped, perennial; since GH #824 / #1267 also
Unit_sold, months_harvested and, on tree rows, Derivation).  Only the EAC-I
waves (2014-15 EACI14, 2017-18 EACI17) are wired; the EHCVM waves (2018-19,
2021-22) ship a harvest and sale block (s16c / s16d) that is not wired, so
they contribute nothing.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is left to ``_finalize_result`` (cached parquets store
pre-transformation data), matching the plot_features / food_coping pattern.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet

# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'crop_production' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/crop_production: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "crop_production: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot, crop) after concat"

to_parquet(p, '../var/crop_production.parquet')
