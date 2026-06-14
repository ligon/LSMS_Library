#!/usr/bin/env python
"""Build item-level people_last7days for Nigeria GHS-Panel (GAP 3, grain 1).

Natural grain (t, i, pid): one row per household member with the reported
last-7-days labor-activity items the WB code (NGA_GHS{1..5}.do labor
section) builds.  Nigeria LACKS this feature; we build it mirroring the
construct Uganda already exposes (per-individual 7-day activity), so the
six new GAP-3 countries match the one we already had.

Stores REPORTED per-individual fields ONLY (no household rollups):
  farm_work / SOB_work / wage_work  0/1 did farm / own-business / wage work
  farm_hrs / SB_hrs / wage_hrs      hours on each activity (last 7 days)
  Industry                          harmonized industry of wage work
                                    (harmonize_industry Preferred Label;
                                    <NA> for non-wage-workers)
  working_age                       0/1 working-age filter (s3q1 / s4aq1)

`pid` matches household_roster's pid (raw indiv as string); `v` auto-joins
from sample() at API time (people_last7days is NOT in `_no_v_join`).

Per-wave source (the labor module; one t per wave):
  W1 2010-11  sect3_plantingw1   s3q* ; hours via job-type logic (PP round)
  W2 2012-13  sect3a_plantingw2  s3aq*; hours via job-type logic (PP round)
  W3 2015-16  sect3_plantingw3   s3q* ; direct hours s3q5b/6b/4b (PP round)
  W4 2018-19  sect3_plantingw4   s3q* ; direct hours s3q5b/6b/4b (PP round)
  W5 2023-24  sect4a_harvestw5   s4aq*; post-harvest labor module

t is the round-quarter the module sits in: W1-W4 the post-planting quarter
(PP_QUARTER), W5 the post-harvest quarter (PH_QUARTER), matching the round
each labor module belongs to.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import (PP_QUARTER, PH_QUARTER,
                     people_last7days_from_s3q, people_last7days_from_s4aq)

pieces = []

# ----------------------------- W1 2010-11 -----------------------------
# s3q* scheme; hours only available as job-1/job-2 totals -> joblogic.
t = PP_QUARTER['2010-11']
raw = get_dataframe('../2010-11/Data/Post Planting Wave 1/Household/sect3_plantingw1.dta',
                    convert_categoricals=False)
pieces.append(people_last7days_from_s3q(
    t, raw, farm='s3q5', sob='s3q6', wage='s3q4', working='s3q1',
    industry='s3q14', hrs_mode='joblogic',
    hour_job1='s3q18', hour_job2='s3q30'))

# ----------------------------- W2 2012-13 -----------------------------
# s3aq* scheme; hours job-1/job-2 totals (s3aq18 / s3aq31) -> joblogic.
t = PP_QUARTER['2012-13']
raw = get_dataframe('../2012-13/Data/Post Planting Wave 2/Household/sect3a_plantingw2.dta',
                    convert_categoricals=False)
pieces.append(people_last7days_from_s3q(
    t, raw, farm='s3aq5', sob='s3aq6', wage='s3aq4', working='s3aq1',
    industry='s3aq14', hrs_mode='joblogic',
    hour_job1='s3aq18', hour_job2='s3aq31'))

# ----------------------------- W3 2015-16 -----------------------------
# s3q* scheme; direct per-activity hours s3q5b/6b/4b.
t = PP_QUARTER['2015-16']
raw = get_dataframe('../2015-16/Data/sect3_plantingw3.dta',
                    convert_categoricals=False)
pieces.append(people_last7days_from_s3q(
    t, raw, farm='s3q5', sob='s3q6', wage='s3q4', working='s3q1',
    industry='s3q14', hrs_mode='direct',
    farm_hrs='s3q5b', sb_hrs='s3q6b', wage_hrs='s3q4b'))

# ----------------------------- W4 2018-19 -----------------------------
t = PP_QUARTER['2018-19']
raw = get_dataframe('../2018-19/Data/sect3_plantingw4.dta',
                    convert_categoricals=False)
pieces.append(people_last7days_from_s3q(
    t, raw, farm='s3q5', sob='s3q6', wage='s3q4', working='s3q1',
    industry='s3q14', hrs_mode='direct',
    farm_hrs='s3q5b', sb_hrs='s3q6b', wage_hrs='s3q4b'))

# ----------------------------- W5 2023-24 -----------------------------
# Post-harvest labor module (sect4a_harvestw5, s4aq* scheme).
t = PH_QUARTER['2023-24']
raw = get_dataframe('../2023-24/Data/Post Harvest Wave 5/Household/sect4a_harvestw5.dta',
                    convert_categoricals=False)
pieces.append(people_last7days_from_s4aq(t, raw))

# ----------------------------- combine -------------------------------
df = pd.concat(pieces, axis=0)
df = df.sort_index()

to_parquet(df, '../var/people_last7days.parquet')
