"""Concatenate wave-level plot_labor data for Tanzania NPS
(parity-loop GAP 3).

Each buildable wave's ``Tanzania/<wave>/_/plot_labor.py`` produces a parquet
with index ``(t, i, plot_id, source)`` and the canonical reported columns from
data_scheme.yml (PersonDays, Wage).  This script concatenates the per-wave
parquets and applies cross-wave id_walk so the household index uses the panel
canonical id scheme.

Only 2019-20 (NPS-SDD Extended Panel) and 2020-21 (NPS Y5 Refresh Panel) are
buildable: the 2008-15 multi-round folder has no agriculture source file on
disk (only the household upd4_hh_* modules), so those four NPS rounds are
deferred -- exactly as for plot_features / crop_production / plot_inputs
(GH #167).
"""
import json

import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import id_walk


WAVES = ['2019-20', '2020-21']

# GH #1287: declarations belong to logical waves; readers use physical folders.
country = Country('Tanzania', preload_panel_ids=False)
declared_folders = {
    wave.wave_folder
    for wave in (country[year] for year in country.waves)
    if 'plot_labor' in wave.data_scheme
}

pieces = []
for t in WAVES:
    if t not in declared_folders:
        continue
    fn = f'../{t}/_/plot_labor.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        # Each folder reaching this reader declares a producer (ledger #1287 §8).
        # Refuse partial country output when a declared build cannot be read.
        raise RuntimeError(
            f'Tanzania/plot_labor: could not load declared folder {t} ({fn})'
        ) from exc
    pieces.append(df)

assert pieces, "plot_labor: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

to_parquet(p, '../var/plot_labor.parquet')
