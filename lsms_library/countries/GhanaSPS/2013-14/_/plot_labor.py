#!/usr/bin/env python
"""GhanaSPS 2013-14 plot_labor -> (t, i, plot_id, season, stage, source).

TWO FILES, ONE SEASON, SEVEN STAGES.

  04m_aglabourquestions.dta  32,851 x 43 -- the plot-x-stage grid: EVERY plot
      is asked about all seven stages (7 x 4,693 rows exactly, `stagenum`
      1..7 with 4,693 rows each, `stage` its lower-case name), with an `any`
      Yes/No screener (18,617 Yes / 14,225 No / 9 null).
  04m_aglabour.dta           4,693 x 29 -- the PLOT-level tail of Section M:
      the seven stage flags again, plus the hired-pay block (M190-M196) which
      the instrument asks ONCE per plot ("IF M2=4 OR M29=4 OR M56=4 OR M83=4
      OR M110=4 OR M137=4 OR M164=4"), so its rate is stamped on all seven of
      the plot's stage rows.  2017-18 stores the same block per stage row and
      it genuinely varies there -- that grain difference is real, not a
      transcription choice.

`stagenum` IS THE STAGE HERE (each value has exactly one row per plot).  Do
not carry that reading into 2017-18, whose same-looking `stageid` is the
ORDINAL POSITION of the stage in the plot's list of stages actually done --
`stageid == 1` spans Clearing (2,285), Planting (1,130), Chemical application
(460), ... in that wave.  This script keys on the stage NAME in both.

SEASON is the constant `last`.  Section M's own READ is "Now I'd like to ask
you about how much time you and others spent working on your farms in the
last farming season", and the instrument never names which season that is --
so it is neither `major` (2009-10's named season) nor `annual` (which is
crop_production's explicit 12-month N5 recall, a different question in this
same wave).  ../../_/data_scheme.yml records the choice.

PERSON-DAYS ARE NOT THE `*days` COLUMNS -- see ghanasps.later_wave_labor.
Part M asks the worker count and the per-worker duration separately, so
PersonDays = workers x days summed over the two sexes of one question.  The
worker totals are internally consistent: `familyworkers == familywomen +
familymen` on all 8,954 rows that report them, and likewise for communal,
hired and other.  `personaldays` is the respondent alone and needs no
multiplication.

HOURS is never asked in this wave (only 2009-10 asks it) -> NaN, declared
`optional: true`.

WAGE RATES.  M191/M192/M193 "How much on average did you pay each man /
woman / child as hired labor during the last farming season?  GHC", and M194
"Do you pay those amounts per day or per acre?" with the eight-option unit
list this wave's `thiredpayunit` value label reproduces exactly (Per day /
week / month / plot / acre / pole / rope / Other - specify).  The three rates
are delivered as THREE columns, not one: they are three separately reported
cells and no weight exists that would combine them for a `Per acre` rate.
-1 and 0 are the producer's fillers for "no such worker" (591 and 701 of the
1,811 woman cells, against 449 plots that hired any woman) and are read as
missing -- ghanasps.drop_nonpositive_rate.

Dropped, deliberately and counted on stdout: every (plot, stage, source) cell
with no positive person-days.  This wave's grid asks all seven stages of
every plot, so the great majority of the 164,255 candidate cells are the
screener's "No" and never existed as labour.
"""
import sys

sys.path.append('../../_/')

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from ghanasps import (finish_labor_frame, labor_source_map, labor_stage_map,
                      later_wave_labor, wage_unit_map)

t = '2013-14'

stage_df = get_dataframe('../Data/04m_aglabourquestions.dta')
pay_df = get_dataframe('../Data/04m_aglabour.dta')

assert stage_df.groupby('stagenum').size().nunique() == 1, (
    f'{t}: stagenum is not a full grid -- it may be an ordinal, as 2017-18 '
    f'stageid is: {stage_df.groupby("stagenum").size().to_dict()}')

flat = later_wave_labor(stage_df, 'stage', pay_df, False, t,
                        labor_source_map(), labor_stage_map(), wage_unit_map())

out = finish_labor_frame(flat, t)
print(f'{t}: rows per stage: {out.groupby(level="stage").size().to_dict()}')
print(f'{t}: PersonDays sum by source: '
      f'{out.groupby(level="source")["PersonDays"].sum().round(0).to_dict()}')
to_parquet(out, 'plot_labor.parquet')
