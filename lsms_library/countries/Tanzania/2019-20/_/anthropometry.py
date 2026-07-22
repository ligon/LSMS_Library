#!/usr/bin/env python
"""Tanzania 2019-20 (NPS-SDD Extended Panel) anthropometry -- reported body
measures (HH_SEC_V).

Parity-loop GAP 5 -- a NEW item-level feature, distinct from ``nutrition``
(nutrient intake).  Stores ONLY the RAW reported measures; the WHO/2006
z-scores (haz06 / waz06 / whz06) and wasting/stunting flags are a query-time
TRANSFORM, never stored here.  See GAP_RANKING.org GAP 5 and TZA_NPS5.do.

Source: ``HH_SEC_V.dta`` (the raw section-V anthropometry roster).  We read
this rather than ``nps_sdd.child.anthro.dta`` because the latter carries ONLY
the pre-computed z-scores (zwxa/zhxa/zwxh) -- it has no raw weight/height -- and
is child-only; HH_SEC_V carries the reported Weight/Height/MUAC for every
measured member.

Panel keys match household_roster (data_info.yml idxvars i=sdd_hhid,
pid=sdd_indid):
    sdd_hhid   -> i    (household id)
    sdd_indid  -> pid  (individual id; format_id matches roster pid)

Reported columns (from the .dta variable labels):
    hh_v05   Weight (KG)                  -> Weight
    hh_v06   Height (CM)                  -> Height
    hh_v09   Upper arm circumference (CM) -> MUAC

Sex and Age live in household_roster at the same (t, i, pid) grain; kept narrow
here.  Cluster identity (v) is joined from sample() at API time.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

t = '2019-20'

df = get_dataframe('../Data/HH_SEC_V.dta', convert_categoricals=False)

anthro = pd.DataFrame({
    'i': df['sdd_hhid'].apply(format_id),
    'pid': df['sdd_indid'].apply(format_id),
    'Weight': pd.to_numeric(df['hh_v05'], errors='coerce'),  # Weight (KG)
    'Height': pd.to_numeric(df['hh_v06'], errors='coerce'),  # Height (CM)
    'MUAC': pd.to_numeric(df['hh_v09'], errors='coerce'),    # Upper arm circ (CM)
})
anthro['t'] = t

# Keep only genuinely-measured rows (at least one reported reading).
anthro = anthro.dropna(subset=['Weight', 'Height', 'MUAC'], how='all')
anthro = anthro.dropna(subset=['pid'])

out = anthro[['t', 'i', 'pid', 'Weight', 'Height', 'MUAC']].set_index(['t', 'i', 'pid'])

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# HH_SEC_V.dta is (sdd_hhid, sdd_indid)-unique: 5,587 rows, 5,587 groups, 0
# duplicates, 0 null sdd_hhid; format_id is injective over the wave's
# households (1,184 distinct -> 1,184).  The 2019-20 wave carries none of the
# 2008-15 panel-line replication (that is a property of the upd4 panel files,
# not of the NPS).  .first() is never called on a cold build.
if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'anthropometry.parquet')
