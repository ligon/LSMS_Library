#!/usr/bin/env python
"""crop_production for Uganda UNPS 2011-12 (GAP 1 item-level build).

Reads AGSEC5A (season 1), AGSEC5B (season 2) and AGSEC4A (intercrop
flag) via get_dataframe and emits a canonical (t,i,plot,j,u,condition,season)
parquet of REPORTED harvest values.  Harvest unit is a5aq6c (the column
whose labels decode to Kg/Sack/Bunch); the harvest CONDITION is a5aq6b,
now an index level in its own right (GH #323/#637) so fresh and dry
records for one plot-crop no longer collide and get summed.
See uganda.CROP_COLMAPS for the per-wave column map.

The SOLD unit (a5?q7c) is carried as Unit_sold (GH #824): Value_sold /
Quantity_sold is a price per Unit_sold, never per u.  This wave ships NO 7b
column, so Condition_sold is all-NA -- the questionnaire asks it (p.13 / p.21
both print '7a Qty | 7b Condition/State Code | 7c Unit Code'), so this is
asked-not-distributed and CROP_COLMAPS declares condition_sold: None.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2011-12'

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
