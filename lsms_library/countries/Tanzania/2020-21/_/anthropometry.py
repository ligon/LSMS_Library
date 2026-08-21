#!/usr/bin/env python
"""Tanzania 2020-21 (NPS Y5 Refresh Panel) anthropometry -- reported body
measures (hh_sec_v).

Parity-loop GAP 5 -- a NEW item-level feature, distinct from ``nutrition``
(nutrient intake).  Stores ONLY the RAW reported measures; the WHO/2006
z-scores and wasting/stunting flags are a query-time TRANSFORM, never stored
here.  See GAP_RANKING.org GAP 5.

Source: ``hh_sec_v.dta`` (section-V anthropometry roster).  Panel keys match
household_roster (data_info.yml idxvars i=y5_hhid, pid=indidy5):
    y5_hhid   -> i    (household id)
    indidy5   -> pid  (individual id; format_id matches roster pid)

Reported columns (from the .dta variable labels):
    hh_v05   Weight (KG)                  -> Weight
    hh_v06   Height (CM)                  -> Height
    hh_v09   Upper arm circumference (CM) -> MUAC

Sex and Age live in household_roster at the same (t, i, pid) grain; kept narrow
here.  Cluster identity (v) is joined from sample() at API time.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

t = '2020-21'

df = get_dataframe('../Data/hh_sec_v.dta', convert_categoricals=False)

anthro = pd.DataFrame({
    'i': df['y5_hhid'].apply(format_id),
    'pid': df['indidy5'].apply(format_id),
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
# hh_sec_v.dta is (y5_hhid, indidy5)-unique: 23,592 rows, 23,592 groups, 0
# duplicates, 0 null y5_hhid; format_id injective over the wave's 4,709
# households.  .first() is never called on a cold build.
if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'anthropometry.parquet')
