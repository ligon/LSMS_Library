#!/usr/bin/env python
"""crop_production for Uganda UNPS 2010-11 (GAP 1 item-level build).

Reads AGSEC5A (season 1), AGSEC5B (season 2) and the two plot-crop
rosters AGSEC4A / AGSEC4B (the crop-stand flag, ONE PER SEASON -- GH #872;
season B's flag comes from AGSEC4B and is never borrowed from AGSEC4A)
via get_dataframe and emits a canonical (t,i,plot,j,u,condition,season)
parquet of REPORTED harvest values.  Harvest unit is a5aq6c (the column
whose labels decode to Kg/Sack/Bunch); the harvest CONDITION is a5aq6b,
now an index level in its own right (GH #323/#637) so fresh and dry
records for one plot-crop no longer collide and get summed.
See uganda.CROP_COLMAPS for the per-wave column map.

The SOLD unit (a5?q7c) and SOLD condition (a5?q7b) are carried as
Unit_sold / Condition_sold (GH #824): Value_sold / Quantity_sold is a price
per Unit_sold, never per u.  KNOWN DEFECT: AGSEC5A's a5aq7c is CAPPED AT
CODE 20 in the shipped extract, so Unit_sold is NA on most season-A sales
and its non-nullity is not random -- see Uganda/_/CONTENTS.org.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2010-11'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df5a = _try('../Data/AGSEC5A.dta')
df5b = _try('../Data/AGSEC5B.dta')
df4a = _try('../Data/AGSEC4A.dta')
df4b = _try('../Data/AGSEC4B.dta')

df = crop_production_for_wave(t, df5a, df5b, df4a, CROP_COLMAPS[t], df4b=df4b)
assert len(df) > 0, f"crop_production produced no rows for {t}"
assert df.index.is_unique, f"Non-unique crop_production index for {t}"
to_parquet(df, 'crop_production.parquet')
