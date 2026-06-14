#!/usr/bin/env python
"""Tanzania 2008-15 anthropometry -- reported body measures (SECTION V).

Source file: ``upd4_hh_v.dta`` -- the multi-round "Anthropometry" module
covering NPS rounds 1-4 (waves 2008-09, 2010-11, 2012-13, 2014-15), stacked
with a ``round`` column.  As with the other multi-round Tanzania scripts this
writes ONE parquet carrying all rounds with ``t`` in the index;
``Wave.grab_data`` filters to the requested sub-wave.

This is parity-loop GAP 5 -- a NEW item-level feature, distinct from our
``nutrition`` feature (which is nutrient *intake*).  We store ONLY the RAW
reported measures the WB cleaning code feeds into ``zscore06``; the WHO/2006
z-scores (haz06 / waz06 / whz06 / bmiz06) and the wasting/stunting flags are a
query-time TRANSFORM (they require the WHO reference population), never stored
here.  See GAP_RANKING.org GAP 5 and TZA_NPS1.do:1002-1024.

The module is INDIVIDUAL-level (one measured row per (round, r_hhid, UPI)).
Panel keys match household_roster exactly:
    r_hhid  -> i    (household id, household_roster i)
    UPI     -> pid  (individual panel id, household_roster pid; formatted the
                     same way as household_roster.py: float -> int -> str)
    round   -> t    (round 1..4 mapped to wave label)

Reported columns (from the .dta variable labels, confirmed via pyreadstat):
    hv_06   WEIGHT (KG)                    -> Weight
    hv_07   HEIGHT (CM)                    -> Height
    hv_10   UPPER ARM CIRCUMFERENCE (CM)   -> MUAC

NB the WB Reproduction code (TZA_NPS1.do) reads the per-round raw files
(suq4/suq5 etc.); we read the harmonised panel file ``upd4_hh_v.dta`` whose
section is "V" (hv_*), and where weight/height/MUAC are hv_06/hv_07/hv_10.
The WB panel keeps only rounds 1-3 for anthropometry (NPS4 dropped it for
"confidential birth dates", NPS5 used pre-computed SDD z-scores); we keep all
four rounds' RAW measures because z-scores are not our concern.

Sex and Age live in household_roster at the same (t, i, pid) grain and join
there for the z-score transform, so we keep this feature narrow (reported body
measures only) rather than duplicating roster characteristics.  Cluster
identity (v) is joined from sample() at API time, not baked here.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

df = get_dataframe('../Data/upd4_hh_v.dta', convert_categoricals=False)

anthro = pd.DataFrame({
    'i': df['r_hhid'].astype(str),
    'round': pd.to_numeric(df['round'], errors='coerce'),
    'pid': df['UPI'],
    'Weight': pd.to_numeric(df['hv_06'], errors='coerce'),   # WEIGHT (KG)
    'Height': pd.to_numeric(df['hv_07'], errors='coerce'),   # HEIGHT (CM)
    'MUAC': pd.to_numeric(df['hv_10'], errors='coerce'),     # UPPER ARM CIRC (CM)
})

# pid formatted exactly like household_roster.py ("1.0" -> "1")
anthro['pid'] = anthro['pid'].astype(float).astype('Int64').astype(str).replace('<NA>', pd.NA)
anthro['t'] = anthro['round'].map(round_match)
anthro = anthro.drop(columns=['round'])

# Keep only rows with at least one reported measure (drop members who were not
# measured -- hv_04 == NO / skipped -- so the parquet carries genuine readings).
anthro = anthro.dropna(subset=['Weight', 'Height', 'MUAC'], how='all')
anthro = anthro.dropna(subset=['t', 'pid'])

out = anthro[['t', 'i', 'pid', 'Weight', 'Height', 'MUAC']].set_index(['t', 'i', 'pid'])

if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'anthropometry.parquet')
