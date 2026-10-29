"""Concatenate wave-level anthropometry data for Uganda (GAP 5).

Each wave's ``Uganda/<wave>/_/anthropometry.py`` produces a parquet indexed
``(t, i, pid)`` with the REPORTED body measures (Weight, Height, MUAC,
Age_months).  This script concatenates them across waves and applies
cross-wave id_walk so the household index uses the panel canonical id scheme
(the 2018-19 / 2019-20 GSEC6 files carry the hashed survey hhid; id_walk
maps it to the same canonical ``i`` household_roster uses).

Source: the GSEC6 anthropometry module that the WB code reads to compute
z-scores.  We keep only the raw measures it feeds in — the z-scores
(haz06/waz06/whz06/bmiz06) and wasting/stunting flags are WHO-2006
reference-population TRANSFORMS, never stored here.  2005-06 is intentionally
absent: it has no GSEC6 anthropometry module and the UNPS panel the WB
harmonise effort tracks starts at wave 1 = 2009-10.
"""
import json

import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import id_walk


pieces = []
country = Country('Uganda')
for t in country.waves:
    wave = country[t]
    # GH #1287; ledger §§2–4: absence is declared, not inferred from a read.
    if 'anthropometry' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/anthropometry.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as error:
        raise RuntimeError(
            f'Uganda/{t}: declared anthropometry failed to load {fn}; '
            'refusing a partial country table.'
        ) from error
    pieces.append(df)

assert pieces, "anthropometry: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/anthropometry.parquet')
