"""Concatenate wave-level food_coping (rCSI day-counts) for Mali (#332).

Each wave's ``Mali/<wave>/_/food_coping.py`` writes a parquet indexed by
``(t, i, Strategy)`` with column ``Days`` (0-7, days in the last 7 the
household used the coping strategy).  Only the EACI 2014-15 wave carries
the Section-17 rCSI battery; the EHCVM waves (2018-19, 2021-22) use the
FAO FIES instrument instead (wired as ``food_security``) and have no
day-count coping block.  This single-wave concatenator mirrors the
plot_features pattern for consistency and future-proofing.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is likewise left to ``_finalize_result`` (cached
parquets store pre-transformation data).
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet

# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'food_coping' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/food_coping.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/food_coping: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, Strategy) after concat"

to_parquet(p, '../var/food_coping.parquet')
