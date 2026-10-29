"""Concatenate wave-level plot_labor for Mali (GAP 3a; parity loop).

Each EACI wave's ``Mali/<wave>/_/plot_labor.py`` writes a parquet indexed by
``(t, i, plot, source)`` with the reported item-level columns (PersonDays,
Wage).  The plot-labor module lives in the EACI waves only (2014-15
EACIMAINOUVRE_p1 + EACIS2F_p2, 2017-18 eaci17_s11ep1 + eaci17_s7ep2).  The
EHCVM waves (2018-19, 2021-22) carry no plot-labor roster, so they
contribute nothing — the same wave split as crop_production / plot_inputs /
livestock.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time (plot_labor is household-linked and is NOT in the framework
``_no_v_join`` set).  ``id_walk`` is left to ``_finalize_result`` (cached
parquets store pre-transformation data), matching the crop_production /
plot_inputs / food_coping pattern.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet

# Reuse Country/Wave declarations and folders (ledger #1287 §2/4).
country = Country('Mali', preload_panel_ids=False)
pieces = []
for t in country.waves:
    wave = country[t]
    if 'plot_labor' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_labor.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        raise RuntimeError(
            f'Mali/{t}/plot_labor: declared wave parquet could not be read: {fn}'
        ) from exc
    pieces.append(df)

assert pieces, "plot_labor: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot, source) after concat"

to_parquet(p, '../var/plot_labor.parquet')
