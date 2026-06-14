#!/usr/bin/env python
"""Build item-level anthropometry for Nigeria GHS-Panel (GAP 5).

Natural grain (t, i, pid): one row per measured individual, carrying the
REPORTED body measures the GHS-Panel anthropometry module collects --
Weight (kg) and Height (cm) -- plus the individual's Sex and reported Age
(years), so the row is self-describing for the downstream WHO-2006 z-score
transform.

This is a NEW feature, distinct from `nutrition` (nutrient intake): it holds
the RAW measured body dimensions the WB code (NGA_GHS1.do:1296-1308) reads,
feeds into `zscore06` to derive haz06/waz06/whz06/bmiz06 and a `wasting`
flag, then discards down to the z-scores.  We store ONLY the raw reported
measures: the z-scores and wasting/stunting are TRANSFORMS (they require the
WHO-2006 reference population and child age-in-months), computed at query
time, never stored.

Anthropometry is collected in the post-harvest round only, so each wave maps
to a single t = PH_QUARTER[wave] (2011Q1 / 2013Q1 / 2016Q1 / 2019Q1 /
2024Q1), matching the post-harvest slice of household_roster.  The
individual key (i = format_id(hhid), pid = str(int(indiv))) aligns 100% with
household_roster on measured rows.

`v` is NOT baked in: anthropometry is an ordinary household/individual table,
so the framework joins `v` from sample() at API time (it is NOT in the
`_no_v_join` set).

Per-wave source structure (anthropometry section + roster section that
supplies Sex/Age, merged on (hhid, indiv)):

  wave     anthro file                 weight cols              height cols              roster file (Sex s1q2, Age)
  W1 10-11 sect4a_harvestw1            s4aq52                   s4aq53                   sect1_harvestw1 (Age s1q4)
  W2 12-13 sect4a_harvestw2            s4aq52                   s4aq53                   sect1_harvestw2 (Age s1q4)
  W3 15-16 sect4a_harvestw3            s4aq52                   s4aq53                   sect1_harvestw3 (Age s1q4)
  W4 18-19 sect4a_harvestw4            s4aq52_1/_2/_3 (median)  s4aq53_1/_2/_3 (median)  sect1_harvestw4 (Age s1q4)
  W5 23-24 sect4b_harvestw5            s4bq8a/b/c (median)      s4bq12a/b/c (median)     sect1_harvestw5 (Age s1q6)

W4/W5 took three readings per measure; the WB uses their row median (egen
rowmedian) -- mirrored here (measurement-error reduction, not an aggregation
across the item grain).

MUAC (mid-upper-arm circumference) is NOT recorded in any Nigeria GHS wave ->
no MUAC column.  Age_months is NOT stored (it is derived from interview-month
minus birth-month -- a transform); the z-score helper derives months from
Age + interview_date.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, anthropometry_for_wave

# (wave, anthro file, [weight cols], [height cols], roster file, age col)
SPECS = [
    ('2010-11',
     '../2010-11/Data/Post Harvest Wave 1/Household/sect4a_harvestw1.dta',
     ['s4aq52'], ['s4aq53'],
     '../2010-11/Data/Post Harvest Wave 1/Household/sect1_harvestw1.dta', 's1q4'),
    ('2012-13',
     '../2012-13/Data/Post Harvest Wave 2/Household/sect4a_harvestw2.dta',
     ['s4aq52'], ['s4aq53'],
     '../2012-13/Data/Post Harvest Wave 2/Household/sect1_harvestw2.dta', 's1q4'),
    ('2015-16',
     '../2015-16/Data/sect4a_harvestw3.dta',
     ['s4aq52'], ['s4aq53'],
     '../2015-16/Data/sect1_harvestw3.dta', 's1q4'),
    ('2018-19',
     '../2018-19/Data/sect4a_harvestw4.dta',
     ['s4aq52_1', 's4aq52_2', 's4aq52_3'],
     ['s4aq53_1', 's4aq53_2', 's4aq53_3'],
     '../2018-19/Data/sect1_harvestw4.dta', 's1q4'),
    ('2023-24',
     '../2023-24/Data/Post Harvest Wave 5/Household/sect4b_harvestw5.dta',
     ['s4bq8a', 's4bq8b', 's4bq8c'],
     ['s4bq12a', 's4bq12b', 's4bq12c'],
     '../2023-24/Data/Post Harvest Wave 5/Household/sect1_harvestw5.dta', 's1q6'),
]

pieces = []
for (wave, anthro_f, wcols, hcols, roster_f, age_col) in SPECS:
    t = PH_QUARTER[wave]
    anthro = get_dataframe(anthro_f, convert_categoricals=False)
    # Roster supplies Sex / reported Age; convert_categoricals=False keeps
    # the raw 'n. NAME' / 'male' labels, normalized inside the helper.
    roster = get_dataframe(roster_f, convert_categoricals=False)
    pieces.append(anthropometry_for_wave(
        t, anthro, roster, weight_cols=wcols, height_cols=hcols,
        sex_col='s1q2', age_col=age_col))

df = pd.concat(pieces, axis=0).sort_index()

to_parquet(df, '../var/anthropometry.parquet')
