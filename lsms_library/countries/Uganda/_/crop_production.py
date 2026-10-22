"""Concatenate wave-level crop_production data for Uganda (GAP 1).

Each wave's ``Uganda/<wave>/_/crop_production.py`` produces a parquet
indexed ``(t, i, plot_id, j, u, condition, season, production_obs)`` with the REPORTED harvest columns
(Quantity, Quantity_sold, Value_sold, harvest_month, intercropped,
perennial, planting_month).  This script concatenates them across waves
and applies cross-wave id_walk so the household index uses the panel
canonical id scheme.

2005-06 is intentionally absent: its AGSEC5A is a crop-area allocation
matrix with no crop-level harvest quantities (and the WB LSMS-ISA panel
starts at wave 1 = 2009-10).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, read_parquet_grain_audit
from lsms_library.paths import data_root
from uganda import Waves, id_walk


pieces = []
# Preserve the existing wave-to-country grain-audit transport. GH1198 no
# longer reduces distinct native observations or blanks conflicting labels.
# Wave parquets live under data_root() only (GH #803), never in-tree.
audit = []
for t in Waves.keys():
    fn = f'../{t}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for crop_production (no .py / parquet, e.g.
        # 2005-06).  DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)
    for r in read_parquet_grain_audit(
            data_root('Uganda') / t / '_' / 'crop_production.parquet') or []:
        if r not in audit:
            audit.append({**r, 'wave': r.get('wave') or t})

assert pieces, "crop_production: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/crop_production.parquet', grain_audit=audit or None)
