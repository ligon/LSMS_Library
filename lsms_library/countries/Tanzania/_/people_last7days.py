"""Concatenate wave-level people_last7days data for Tanzania NPS
(parity-loop GAP 3).

Each buildable wave's ``Tanzania/<wave>/_/people_last7days.py`` produces a
parquet with index ``(t, i, pid)`` and the canonical reported columns from
data_scheme.yml (farm_work, SOB_work, wage_work, farm_hrs, SB_hrs, wage_hrs,
industry, working_age).  This script concatenates the per-wave parquets and
applies cross-wave id_walk so the household index uses the panel canonical id
scheme.

Only 2019-20 (NPS-SDD Extended Panel) and 2020-21 (NPS Y5 Refresh Panel) are
buildable: the 2008-15 multi-round folder carries the HARMONISED panel labor
module (upd4_hh_e), which is a "last 12 months" employment screen with NO 7-day
HOURS items -- it cannot populate the farm_hrs / SB_hrs / wage_hrs columns this
feature requires, so those four NPS rounds are deferred (the WB cleaning code
reads the per-round raw SEC_B_C_D_E1_F_G1_U.dta with its 7-day `seq*` block,
which is not the file present in our 2008-15 panel folder).
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
    if 'people_last7days' in wave.data_scheme
}

pieces = []
for t in WAVES:
    if t not in declared_folders:
        continue
    fn = f'../{t}/_/people_last7days.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as exc:
        # Each folder reaching this reader declares a producer (ledger #1287 §8).
        # Refuse partial country output when a declared build cannot be read.
        raise RuntimeError(
            f'Tanzania/people_last7days: could not load declared folder {t} ({fn})'
        ) from exc
    pieces.append(df)

assert pieces, "people_last7days: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

to_parquet(p, '../var/people_last7days.parquet')
