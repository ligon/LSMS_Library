#!/usr/bin/env python
"""anthropometry for Uganda UNPS — one wave (GAP 5 item-level build).

Reads the GSEC6 anthropometry module via get_dataframe and emits a canonical
(t, i, pid) parquet of the REPORTED body measures (Weight kg, Height cm,
MUAC cm, Age_months).  This is the raw input the WB cleaning code
(UGA_UNPS{1..8}.do anthro section) feeds into ``zscore06`` — we keep ONLY the
reported measures, never the z-scores or wasting/stunting flags (those are
WHO-2006 reference-population transforms, computed at query time).

The wave id is the parent folder name; the source file and column map come
from uganda.ANTHRO_FILES / uganda.ANTHRO_COLMAPS.
"""
import os
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import anthropometry_for_wave, ANTHRO_COLMAPS, ANTHRO_FILES

t = os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
colmap = ANTHRO_COLMAPS[t]

df = get_dataframe(ANTHRO_FILES[t])

out = anthropometry_for_wave(t, df, colmap)
assert len(out) > 0, f"anthropometry produced no rows for {t}"
assert out.index.is_unique, f"Non-unique anthropometry index for {t}"
to_parquet(out, 'anthropometry.parquet')
