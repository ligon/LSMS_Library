#!/usr/bin/env python
"""plot_labor for Uganda UNPS 2009-10 (GAP 3 item-level build).

Reads AGSEC3A (season 1) + AGSEC3B (season 2) plot-input modules via
get_dataframe and emits a canonical (t,i,plot,source,season) parquet of
REPORTED plot-labor person-days (PersonDays) and the reported hired wage
(Wage).  Source axis in {family, hired, other}.  See uganda.LABOR_COLMAPS
for the per-wave column map.  Only REPORTED person-days are kept -- the WB
per-parcel total_*_labor_days sums and median-wage hired_labor_value are
transformations, never stored here.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_labor_for_wave, LABOR_COLMAPS

t = '2009-10'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df3a = _try('../Data/AGSEC3A.dta')
df3b = _try('../Data/AGSEC3B.dta')

df = plot_labor_for_wave(t, df3a, df3b, LABOR_COLMAPS[t])
assert len(df) > 0, f"plot_labor produced no rows for {t}"
assert df.index.is_unique, f"Non-unique plot_labor index for {t}"
to_parquet(df, 'plot_labor.parquet')
