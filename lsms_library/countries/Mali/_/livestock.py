"""Concatenate wave-level livestock for Mali (GAP 4; parity loop).

Each EACI wave's ``Mali/<wave>/_/livestock.py`` writes a parquet indexed by
``(t, i, animal)`` with the reported item-level columns (HeadCount,
HeadAcquired, HeadSold, SalesValue).  The livestock module lives in the
EACI waves only (2014-15 EACIS4A_p2 / s4a, 2017-18 eaci17_s8ap2 + eaci17_s8b1p2 /
s8a+s8b1).  The EHCVM waves (2018-19, 2021-22) carry no livestock roster, so
they contribute nothing — the same wave split as crop_production /
plot_inputs.

``v`` is NOT baked in — and is NOT joined for livestock at all: the
framework's ``_no_v_join`` set already excludes 'livestock', so the
canonical grain is (t, i, animal) with no cluster level.  ``id_walk`` is
left to ``_finalize_result`` (cached parquets store pre-transformation
data), matching the crop_production / plot_inputs / food_coping pattern.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet

# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'livestock' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/livestock: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, animal) after concat"

to_parquet(p, '../var/livestock.parquet')
