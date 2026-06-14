#!/usr/bin/env python
"""livestock for Uganda UNPS 2013-14 (GAP 4 item-level build).

Reads the livestock roster (AGSEC6A large ruminants / AGSEC6B small ruminants
/ AGSEC6C poultry & other) via get_dataframe and emits a canonical
(t, i, animal) parquet of REPORTED head counts / acquired / sold / per-head
value.  This is the PRE-collapse roster the WB code (UGA_UNPS1.do:710-720)
reads only to build a single engaged-livestock binary.

The animal type is integer-coded here, so sections are loaded with
convert_categoricals=False; livestock_for_wave maps the codes to canonical
species via the harmonize_species categorical table (with the 2010-11 6B
code_overrides where that wave's scheme diverges).  See
uganda.LIVESTOCK_COLMAPS for the per-wave column map.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import livestock_for_wave, LIVESTOCK_COLMAPS

t = '2013-14'
colmap = LIVESTOCK_COLMAPS[t]


def _load(path, section):
    cats = colmap.get(section, {}).get('type_kind') == 'string'
    try:
        return get_dataframe(path, convert_categoricals=cats)
    except Exception:
        return None


df6a = _load('../Data/AGSEC6A.dta', 'A')
df6b = _load('../Data/AGSEC6B.dta', 'B')
df6c = _load('../Data/AGSEC6C.dta', 'C')

df = livestock_for_wave(t, df6a, df6b, df6c, colmap)
assert len(df) > 0, f"livestock produced no rows for {t}"
assert df.index.is_unique, f"Non-unique livestock index for {t}"
to_parquet(df, 'livestock.parquet')
