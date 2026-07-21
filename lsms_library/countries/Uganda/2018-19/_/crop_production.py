#!/usr/bin/env python
"""crop_production for Uganda UNPS 2018-19 (GAP 1 item-level build).

Reads AGSEC5A (season 1), AGSEC5B (season 2) and AGSEC4A (intercrop
flag) via get_dataframe and emits a canonical (t,i,plot,j,u,condition,season)
parquet of REPORTED harvest values.

2018-19 is the odd wave.  AGSEC5A carries NO harvest-unit column at all
(-> u='Unknown'); its condition is a5aq6b (labelled) -- a5aq6c holds the
same 20-code condition scheme unlabelled and disagreeing on 158 of 7 144
rows, so the labelled column wins.  AGSEC5B does carry a unit (a5bq6b,
40 labels) with the condition in a5bq6c, but the unit is still wired as
None here; wiring it is a separate defect (see Uganda/_/CONTENTS.org).
The harvest CONDITION is now an index level in its own right
(GH #323/#637).  See uganda.CROP_COLMAPS for the per-wave column map.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2018-19'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df5a = _try('../Data/AGSEC5A.dta')
df5b = _try('../Data/AGSEC5B.dta')
df4a = _try('../Data/AGSEC4A.dta')

df = crop_production_for_wave(t, df5a, df5b, df4a, CROP_COLMAPS[t])
assert len(df) > 0, f"crop_production produced no rows for {t}"
assert df.index.is_unique, f"Non-unique crop_production index for {t}"
to_parquet(df, 'crop_production.parquet')
