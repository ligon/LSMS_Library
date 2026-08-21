#!/usr/bin/env python
"""plot_inputs for Uganda UNPS 2011-12 (GAP 2 item-level build).

Reads AGSEC3A (season 1) + AGSEC3B (season 2) plot-input modules (organic /
inorganic fertilizer + pesticide blocks) and AGSEC4A (plot-crop roster, seed
block) via get_dataframe, and emits a canonical (t,i,plot,input,j,season) parquet of
REPORTED input values.  See uganda.INPUT_COLMAPS for the per-wave column map.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import plot_inputs_for_wave, INPUT_COLMAPS

t = '2011-12'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df3a = _try('../Data/AGSEC3A.dta')
df3b = _try('../Data/AGSEC3B.dta')
df4a = _try('../Data/AGSEC4A.dta')

df = plot_inputs_for_wave(t, df3a, df3b, df4a, INPUT_COLMAPS[t])
assert len(df) > 0, f"plot_inputs produced no rows for {t}"
assert df.index.is_unique, f"Non-unique plot_inputs index for {t}"
to_parquet(df, 'plot_inputs.parquet')
