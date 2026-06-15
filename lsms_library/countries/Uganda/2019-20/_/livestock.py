#!/usr/bin/env python
"""livestock for Uganda UNPS 2019-20 (GAP 4 item-level build).

Reads the livestock roster (agsec6a large ruminants / agsec6b small ruminants
/ agsec6c poultry & other, under Data1/Agric/) via get_dataframe and emits a
canonical (t, i, animal) parquet of REPORTED head counts / acquired / sold /
per-head value.  This is the PRE-collapse roster the WB code reads only to
build a single engaged-livestock binary.

The animal type is integer-coded here (codes 1-27, same scheme as 2011-12+),
so sections are loaded with convert_categoricals=False and mapped to canonical
species via the harmonize_species categorical table.  See
uganda.LIVESTOCK_COLMAPS for the per-wave column map.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import livestock_for_wave, LIVESTOCK_COLMAPS

t = '2019-20'
colmap = LIVESTOCK_COLMAPS[t]


def _load(path, section):
    cats = colmap.get(section, {}).get('type_kind') == 'string'
    try:
        return get_dataframe(path, convert_categoricals=cats)
    except Exception:
        return None


df6a = _load('../Data1/Agric/agsec6a.dta', 'A')
df6b = _load('../Data1/Agric/agsec6b.dta', 'B')
df6c = _load('../Data1/Agric/agsec6c.dta', 'C')

df = livestock_for_wave(t, df6a, df6b, df6c, colmap)
assert len(df) > 0, f"livestock produced no rows for {t}"
assert df.index.is_unique, f"Non-unique livestock index for {t}"
to_parquet(df, 'livestock.parquet')
