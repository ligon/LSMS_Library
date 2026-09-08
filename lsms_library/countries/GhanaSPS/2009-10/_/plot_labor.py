#!/usr/bin/env python
"""GhanaSPS 2009-10 plot_labor -> (t, i, plot_id, season, stage, source).

EIGHT FILES, TWO SEASONS, FOUR STAGES -- AND THE STAGE IS NOT IN THE DATA'S
PROSE.  Section 4 Part A(ix) "LABOR INPUTS" asks one nine-cell labour battery
FOUR times for the major season (S4AIX1..4) and again for the minor season
(S4AIX5..8).  The variable-label TEXT is identical across the four stage files
-- every one of them reads "Number of days casual labor provided by men",
"Average hours per day worked by men on casual labor", ... -- so a wrong
stage assignment would be invisible to every automated check.

WHAT *IS* IN THE DATA IS THE QUESTION NUMBER.  Each label is prefixed with its
A-number (S4AIX1 carries A290..A298, S4AIX2 A299..A307, S4AIX3 A308..A316,
S4AIX4 A317..A325, and A327..A362 for the four minor-season files), and the
questionnaire's own section headers name the stage of each A-range:

  A290-A298 / A327-A335  "... LABOR USE ON LAND PREPARATION E.G. CLEARING OR
                         WEEDING BEFORE PLANTING, SEEDING/NURSERY, PLANTING
                         AND TRANSPLANTING DURING THE MAJOR [MINOR] SEASON"
  A299-A307 / A336-A344  "... LABOR USED ON FIELD MANAGEMENT (WEEDING AFTER
                         PLANTING, FERTILIZED AND PESTICIDE APPLICATIONS,
                         IRRIGATION, MANAGEMENT ETC ..."
  A308-A316 / A345-A353  "... LABOR USED ON HARVESTING OF CROPS ..."
  A317-A325 / A354-A362  "... LABOR USED ON POST-HARVEST ACTIVITIES
                         (INCLUDING PRESERVATION FOR STORAGE ETC) ..."

So the file -> stage map below is ASSERTED, not assumed: `_assert_block()`
reads each file's own variable labels and fails the build if the A-numbers are
not the ones this stage claims.  A file swap, a re-export in a different order
or a renamed column becomes a build failure instead of a silent mislabel.
(FINDINGS_agriculture.org §5 and the task brief both say the labels are
"identical across the four files"; the TEXT is, the A-number is not, and that
difference is the whole check.)

SEASON is anchored the same way, and additionally in the data: S4AIX1 carries
A289 "Starting/Ending month for the MAJOR season of dominant crop" and S4AIX5
carries A326 "... for the MINOR season ...", in their own variable labels.
The month distributions corroborate: A289.1 clusters February-June (3,698 of
4,801), A326.1 clusters July-October (990 of 1,520).

SOURCE.  Each block asks the same battery of three sources x three sex groups:
  +0/+1/+2  casual labor    by men / women / children (under 15)
  +3/+4/+5  permanent labor by men / women / children
  +6/+7/+8  family labor    by men / women / children
`casual` and `permanent` are KEPT DISTINCT rather than folded to the later
waves' `hired` -- the survey's own grain is the truth and a consumer can sum;
the reverse is impossible.  `family` here is broader than the later waves'
`family`: this instrument has no "yourself" question and A362's own wording is
"How much family labor is worked by children? (including exchanged labor)", so
it absorbs what 2013-14 / 2017-18 split into self + family + communal.
_/categorical_mapping.org records both containments.

PERSONDAYS = sum over the three sex groups of (# of days) x (average # of
workers), the cells .1 and .3 of ONE question -- Uganda's within-question
convention.  A group needs BOTH cells; min_count=1, so the row is NaN only if
no group has both.

HOURS = the person-day-weighted mean of the .2 cells ("average hours per day
worked by ... "), i.e. the number that multiplies PersonDays to give
person-hours.  Groups whose hours cell is null are excluded from BOTH the
numerator and the denominator; Hours is NaN if no group has one.  2013-14 and
2017-18 never ask hours, so this column is `optional: true`.

NO WAGE.  Section 4 IX asks NO payment question -- the whole module is days /
hours / workers, A289 through A362, and the only "pay" questions anywhere in
Section 4 are A47 (cash rent for the plot) and the Part B crop-sale revenue.
WageRateMen / WageRateWomen / WageRateChildren / WageUnit are therefore null
throughout this wave and are declared `optional: true`.  (FINDINGS
_agriculture.org says "W1 has hired-labour payment in the S4AIX blocks" and
the task brief says "W1 from the S4AIX payment cells"; both are wrong -- see
the plot-labour section of ../../_/CONTENTS.org.)

Rows dropped, deliberately and counted on stdout: null plot numbers (15-17 per
file in S4AIX2..8, none in S4AIX1; the non-null key sets of all eight files
are IDENTICAL, 5,686 keys); negative sentinels (-10 and -1, undocumented in
the CODE_BOOK) read as missing; and every (plot, season, stage, source) cell
with no positive person-days.
"""
import sys

sys.path.append('../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from ghanasps import (_W1_CELL_SUFFIXES, _W1_SOURCE_OFFSETS, _W1_STAGE_BLOCKS,
                      drop_labor_sentinels, finish_labor_frame)

t = '2009-10'

def _variable_labels(path):
    """The .dta variable labels, read from the DVC-cached blob by its sidecar
    md5 -- the lock-free L1 path (get_dataframe drops labels; get_data_file
    walks the whole DVC index, GH #763)."""
    import io
    from pathlib import Path

    import yaml

    from lsms_library.local_tools import _ensure_dvc_pulled, data_root

    src = Path(path).resolve()
    sidecar = src.parent / (src.name + '.dvc')
    md5 = yaml.safe_load(sidecar.read_text())['outs'][0]['md5']
    _ensure_dvc_pulled(str(src))
    blob = Path(data_root()) / 'dvc-cache' / md5[:2] / md5[2:]
    with pd.io.stata.StataReader(io.BytesIO(blob.read_bytes())) as rdr:
        return rdr.variable_labels()


def _assert_block(stage, season, fileno, lo, hi):
    """Fail the build unless S4AIX{fileno}'s labour cells really are A{lo}..A{hi}.

    This is what turns "the stage is assigned from which file the row came
    from" into "the stage is assigned from the question number the producer
    stamped in the label, whose section header the questionnaire supplies".
    """
    labels = _variable_labels(f'../Data/S4AIX{fileno}.dta')
    seen = set()
    for var, lab in labels.items():
        if not var.startswith('s4aix_'):
            continue
        n = var[len('s4aix_'):].rstrip('i')
        if not n.isdigit():
            continue
        n = int(n)
        if lo <= n <= hi:
            seen.add(n)
            assert lab.startswith(f'A{n}.'), (
                f'{t} S4AIX{fileno}: {var} is labelled {lab!r}, which does not '
                f'begin with the question number A{n} the column name claims')
    expected = set(range(lo, hi + 1))
    assert seen == expected, (
        f'{t} {season}/{stage}: S4AIX{fileno} carries labour questions '
        f'{sorted(seen)}, not the A{lo}-A{hi} range the questionnaire heads '
        f'"{stage}" -- the file -> stage map is wrong or the extract changed')


def block(stage, season, fileno, lo):
    df = get_dataframe(f'../Data/S4AIX{fileno}.dta', convert_categoricals=False)
    plotcol = f's4aix{fileno}_plotno'
    n_nullplot = int(df[plotcol].isna().sum())
    df = df[df[plotcol].notna()].copy()

    pieces = []
    for source, offset in _W1_SOURCE_OFFSETS:
        person_days, hour_num, hour_den = None, None, None
        n_days_no_workers = 0
        for g in range(3):                       # men / women / children
            q = lo + offset + g
            days, hours, workers = (drop_labor_sentinels(df[f's4aix_{q}{s}'])
                                    for s in _W1_CELL_SUFFIXES)
            n_days_no_workers += int((days.notna() & workers.isna()).sum())
            pd_g = days * workers                # person-days of this sex group
            person_days = pd_g if person_days is None else person_days.add(pd_g, fill_value=0)
            wt = pd_g.where(hours.notna())
            num = wt * hours
            hour_num = num if hour_num is None else hour_num.add(num, fill_value=0)
            hour_den = wt if hour_den is None else hour_den.add(wt, fill_value=0)
        pieces.append(pd.DataFrame({
            'hhno': df['hhno'].to_numpy(),
            'plotno': df[plotcol].to_numpy(),
            'source': source,
            'PersonDays': person_days.to_numpy(),
            'Hours': (hour_num / hour_den.where(hour_den > 0)).to_numpy(),
        }))
        if n_days_no_workers:
            print(f'{t} {season}/{stage}/{source}: {n_days_no_workers} sex-group cells '
                  f'report days with no worker count -> no person-days')
    out = pd.concat(pieces, ignore_index=True)
    out['season'] = season
    out['stage'] = stage
    if n_nullplot:
        print(f'{t} {season}/{stage}: dropped {n_nullplot} rows of S4AIX{fileno} '
              f'with a null plot number')
    return out


parts = []
for stage, major_file, minor_file, major_range, minor_range in _W1_STAGE_BLOCKS:
    for season, fileno, rng in (('major', major_file, major_range),
                                ('minor', minor_file, minor_range)):
        _assert_block(stage, season, fileno, *rng)
        parts.append(block(stage, season, fileno, rng[0]))

flat = pd.concat(parts, ignore_index=True)
flat['t'] = t
flat['i'] = flat['hhno'].map(format_id)
flat['plot_id'] = flat['plotno'].map(format_id)
for c in ('WageRateMen', 'WageRateWomen', 'WageRateChildren'):
    flat[c] = np.nan
flat['WageUnit'] = pd.NA

out = finish_labor_frame(flat, t)
print(f'{t}: rows per (season, stage): '
      f'{out.groupby(level=["season", "stage"]).size().to_dict()}')
print(f'{t}: PersonDays sum by source: '
      f'{out.groupby(level="source")["PersonDays"].sum().round(0).to_dict()}')
to_parquet(out, 'plot_labor.parquet')
