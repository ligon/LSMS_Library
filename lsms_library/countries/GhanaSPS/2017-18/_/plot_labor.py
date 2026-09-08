#!/usr/bin/env python
"""GhanaSPS 2017-18 plot_labor -> (t, i, plot_id, season, stage, source).

ONE FILE, ONE SEASON, SEVEN STAGES -- AND `stageid` IS NOT THE STAGE.

  04m_aglabourquestions.dta  20,663 x 64 -- Part M, one row per plot-stage,
      but only for the stages the plot ACTUALLY DID (mean 3.85 rows per plot,
      min 1, max 7), unlike 2013-14's full seven-per-plot grid.  The hired-pay
      block (M190-M196) is folded into this file rather than kept in a
      plot-level sibling.

  `stageid` IS AN ORDINAL, NOT A STAGE CODE.  It numbers the plot's stages
  1..n in the order they were asked, so `stageid == 1` is "Clearing and land
  preparation" on 2,285 rows, "Planting" on 1,130, "Chemical application" on
  460, "Plowing" on 554, "Weeding" on 248, "Harvesting" on 42 and
  "Post-harvest processing" on 4.  Mapping it through 2013-14's `stagenum`
  vocabulary (where 1..7 IS the stage, 4,693 rows each) would mislabel about
  60 per cent of this wave's rows and NOTHING would catch it -- both columns
  are small integers, the index would still be unique and every sanity check
  would pass.  This script keys on `stagename`, and the sibling 2013-14
  script asserts its own `stagenum` really is a full grid.

  640 rows have a BLANK `stagename` and a null `stageid`; they are the 640
  plots whose `cultivated` is null (uncultivated), carry no person-days at
  all, and are dropped.  A null index level would otherwise be deleted by the
  framework's collapse with a GrainCollapseWarning (fatal under strict).

SEASON is the constant `last` -- "Were any agricultural activities performed
on [Plot Name] in the last farming season (2017)?".  See the 2013-14 script
and ../../_/data_scheme.yml for why not `major` and not `annual`.

PERSON-DAYS ARE NOT THE `*days` COLUMNS: "How many days on average did each
of the female hired laborers work on this plot ...?" is a per-worker
duration, so PersonDays = workers x days over the two sexes.  Verified
internally consistent here too (`familyworkers == familywomen + familymen` on
all 11,463 rows that report them, and likewise communal / hired / other).
`personaldays` is the respondent alone.

WAGE RATES.  M191/M192 ask the man and the woman rate; THIS WAVE DROPS THE
CHILD RATE that 2013-14's M193 asks, so `WageRateChildren` is null
throughout.  M196 is the same eight-option unit list, with -666 for Other.
The instrument prints the pay block once per plot, but the shipped file
stores it per stage row and it is NOT constant within a plot -- 620 of 5,366
plots carry two or more distinct man rates across their stage rows -- so it
is delivered at the stage grain the data actually has, unlike 2013-14 where
the block genuinely is plot-level.  A rate of 0 (38 man, 4 woman cells) is
read as missing; this wave has no -1.

HOURS is never asked in this wave -> NaN, declared `optional: true`.

Dropped, deliberately and counted on stdout: the 640 blank-stage rows and
every (plot, stage, source) cell with no positive person-days.
"""
import sys

sys.path.append('../../_/')

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from ghanasps import (finish_labor_frame, labor_source_map, labor_stage_map,
                      later_wave_labor, wage_unit_map)

t = '2017-18'

stage_df = get_dataframe('../Data/04m_aglabourquestions.dta')

# `stageid` is an ordinal here, so it must NOT be used as the stage key; the
# assertion states that in a form that fails the build if the file changes.
counts = stage_df.groupby('stageid').size()
assert counts.nunique() > 1, (
    f'{t}: stageid now looks like a full grid ({counts.to_dict()}); it has '
    f'always been the ordinal position of the stage in the plot -- re-check '
    f'before trusting it')

flat = later_wave_labor(stage_df, 'stagename', stage_df, True, t,
                        labor_source_map(), labor_stage_map(), wage_unit_map())

out = finish_labor_frame(flat, t)
print(f'{t}: rows per stage: {out.groupby(level="stage").size().to_dict()}')
print(f'{t}: PersonDays sum by source: '
      f'{out.groupby(level="source")["PersonDays"].sum().round(0).to_dict()}')
to_parquet(out, 'plot_labor.parquet')
