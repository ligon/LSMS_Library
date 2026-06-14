"""Concatenate wave-level livestock data for Tanzania NPS
(parity-loop GAP 4).

Each buildable wave's ``Tanzania/<wave>/_/livestock.py`` produces a parquet
with index ``(t, i, animal)`` and the canonical reported columns from
data_scheme.yml (HeadCount, HeadAcquired, HeadSold).  This script concatenates
the per-wave parquets and applies cross-wave id_walk so the household index
uses the panel canonical id scheme.

Only 2019-20 (NPS-SDD Extended Panel) and 2020-21 (NPS Y5 Refresh Panel) are
buildable: the 2008-15 multi-round folder has no livestock source file on disk
(only the household upd4_hh_* modules), so those four NPS rounds are deferred
-- exactly as for plot_features / crop_production / plot_inputs (GH #167).

'livestock' is in the framework _no_v_join set, so the framework joins NO v
cluster level: the API grain stays (t, i, animal).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import id_walk


WAVES = ['2019-20', '2020-21']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

to_parquet(p, '../var/livestock.parquet')
