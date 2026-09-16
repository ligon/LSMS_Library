#!/usr/bin/env python
"""GhanaLSS 1988-89 labor -- Section 5 (Employment), ANNUAL labour supply.

One row per (person, job slot) at (t, i, pid, job).  Four slots, from four
source files, each contributing a constant `job` label -- the same
melt-with-a-constant-label shape as `plot_inputs.py`, which YAML cannot
express.

    main           Y05B1  Part B, main job      "during the past 7 days"
    secondary      Y05C1  Part C, secondary job "during the past 7 days"
    main_12m       Y05E1  Part E, main job      "during the past 12 months"
    secondary_12m  Y05G1  Part G, secondary job "during the past 12 months"

WHY THE "PAST 7 DAYS" PARTS ARE THE PRIMARY ANNUAL SOURCE.  Their TITLE names
the reference period used to IDENTIFY the job; two of their questions are
nonetheless about the past twelve months (Part B, PDF p.19, form block 5B1):

    Q6  "For how many WEEKS during the past 12 MONTHS did you do this work?"
    Q7  "For how many HOURS PER WEEK did you usually do this work DURING THE
         PAST 12 MONTHS?"

So `WeeksPerYear x HoursPerWeek` is an annual measure on the same window as
Section 9's agriculture, for every person who worked in the reference week
(6,705 rows in 1987-88, 6,206 in 1988-89).  Measured: median 1,050 annual
hours, p10 200, p90 2,400.

Parts E and G are a SUPPLEMENT, not an alternative.  Their Q3 ("Is this work
the same as your main or secondary job during the past 7 days?") skips the
rest of the grid to Part F when the answer is yes, so their weeks / days /
hours / earnings / own-farm columns are populated ONLY where the 12-month job
DIFFERED -- 1,701 of 8,182 rows in 1987-88 and 1,986 of 7,806 in 1988-89, i.e.
about a fifth.  Building annual labour from Parts E/G alone would miss ~78% of
workers.  `SameAs7DayJob` carries that answer so a consumer can drop the
pointer rows instead of double-counting them.

THE TWO FAMILIES EXPRESS HOURS DIFFERENTLY, and this is why `HoursPerWeek` is
part reported and part derived:

    Parts B/C  weeks (Q6) x HOURS PER WEEK (Q7)          -> reported
    Parts E/G  weeks (Q5) x DAYS PER WEEK (Q6) x HOURS PER DAY (Q7)

`HoursPerWeek` is therefore READ on the B/C rows and CONSTRUCTED on the E/G
rows as DaysPerWeek x HoursPerDay, so that one formula --
`WeeksPerYear x HoursPerWeek` -- is valid on every row.  The constructed rows
carry the registry key in `Derivation`; the reported rows carry NA.
`DaysPerWeek` and `HoursPerDay` are served raw beside it, so nothing is hidden
behind the construction.

NOT CARRIED (@ligon, 2026-09-16): the genuinely 7-day quantities -- Part B Q4
"for how many DAYS during the past 7 days" and Q5 "during these days, how many
HOURS PER DAY" -- which describe the reference week rather than the year.

OWN-FAMILY FARM LABOUR is `OwnFarmOrBusiness` (Part B Q11 / Part E Q11: "In
this work, were you self-employed on a farm or in a business belonging to your
household?") together with an agricultural `Industry`.  Measured on Part B:
4,404 people in 1987-88 and 3,787 in 1988-89, giving 1,770 / 1,782 households
with a median 1,856 / 1,440 annual own-farm hours.

OCCUPATION AND INDUSTRY ARE SERVED AS RAW CODES.  No code list ships with the
data in this repo and none has been verified, so nothing is decoded and no
standard is asserted.  What the data shows: `Industry` 111 is overwhelmingly
dominant (4,374 of 6,705) and co-occurs with `Occupation` 61, the modal code
among people whose main job is farming; 112 and 130 also appear.  The 1xx block
is agriculture/forestry/fishing on any reading, but a DECODE TABLE IS A
DOCUMENTED GAP -- filter on the codes, and see `_/CONTENTS.org`.
"""
import sys
sys.path.append('../../_')
from mapping import i as i_helper
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from ghanalss import derive_5eg_hours_per_week
from lsms_library.local_tools import (df_from_orgfile, format_id, get_dataframe,
                                      to_parquet)

t = '1988-89'

HOURS_PER_WEEK_DERIVATION = 'GhanaLSS::labor::5eg-hours-per-week-from-days-and-hours'

#: (file, job label, occupation, industry, weeks, hours/week, days/week,
#:  hours/day, earnings, earnings unit, own farm/business, same-as-7-day)
BLOCKS = [
    ('Y05B1.DAT', 'main',          'OCCM',  'INDM',  'OCCMW',  'OCCMHU', None,
     None, 'EARN',   'EARNU',    'OCCMSL',  None),
    ('Y05C1.DAT', 'secondary',     'OCCS',  'INDS',  'OCCSW',  'OCCSHW', None,
     None, 'SEARN',  'SEARNU',   'OCCSSL',  None),
    ('Y05E1.DAT', 'main_12m',      'OCCMY', 'INDMY', 'OCCMYW', None,     'OCCMYD',
     'OCCMYH', 'EARNY',  'EARNYU',  'OCCMYSL', 'OCCMSWK'),
    ('Y05G1.DAT', 'secondary_12m', 'OCCSY', 'INDSY', 'OCCSYW', None,     'OCCSYD',
     'OCCSYH', 'EARNYS', 'EARNYSU', 'OCCSYSL', 'OCCSSWK'),
]

_root = '../../_/categorical_mapping.org'


def _table(name):
    d = df_from_orgfile(_root, name=name, encoding='ISO-8859-1')
    return (d.assign(Code=d['Code'].astype('Int64').astype('string'))
             .set_index('Code')['Label'].to_dict())


TIME_UNIT = _table('time_unit')
SAME_7DAY = _table('same_as_7day_job')


def _num(df, col):
    if col is None or col not in df:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors='coerce')


def _decode(df, col, table):
    if col is None or col not in df:
        return pd.Series(pd.NA, index=df.index, dtype='string')
    return df[col].apply(format_id).astype('string').replace(table)


frames = []
for (fn, job, occ, ind, wks, hpw, dpw, hpd, earn, earnu, own, same) in BLOCKS:
    df = get_dataframe(f'../Data/{fn}')
    out = pd.DataFrame({
        't': t,
        'i': df['HID'].apply(i_helper),
        'pid': df['PID'].apply(format_id),
        'job': job,
        'Occupation': _num(df, occ),
        'Industry': _num(df, ind),
        'WeeksPerYear': _num(df, wks),
        'DaysPerWeek': _num(df, dpw),
        'HoursPerDay': _num(df, hpd),
        'Earnings': _num(df, earn),
        'EarningsUnit': _decode(df, earnu, TIME_UNIT),
        'SameAs7DayJob': _decode(df, same, SAME_7DAY),
    })
    # Q11 is 1 = YES, 2 = NO; anything else (including the question never being
    # reached) stays NA rather than becoming False.
    out['OwnFarmOrBusiness'] = _num(df, own).map({1.0: True, 2.0: False}).astype('boolean')

    if hpw is not None:                      # Parts B / C -- REPORTED
        out['HoursPerWeek'] = _num(df, hpw)
        out['Derivation'] = pd.Series(pd.NA, index=out.index, dtype='string')
    else:                                    # Parts E / G -- CONSTRUCTED
        v = pd.Series(derive_5eg_hours_per_week(out['DaysPerWeek'],
                                                out['HoursPerDay']),
                      index=out.index)
        out['HoursPerWeek'] = v
        out['Derivation'] = pd.Series(
            np.where(pd.notna(v), HOURS_PER_WEEK_DERIVATION, pd.NA),
            index=out.index, dtype='string')
    frames.append(out)

f = pd.concat(frames, ignore_index=True)
f = f.set_index(['t', 'i', 'pid', 'job']).sort_index()

# One row per person per slot by construction -- each source file is keyed on
# (HID, PID).  Assert rather than trust: a duplicate would be collapsed by the
# API-layer canonical-shape guard, silently dropping a job.
assert f.index.is_unique, (
    't, i, pid, job is not unique: '
    f'{f.index[f.index.duplicated()].tolist()[:5]}')

to_parquet(f[['Occupation', 'Industry', 'WeeksPerYear', 'HoursPerWeek',
              'DaysPerWeek', 'HoursPerDay', 'Earnings', 'EarningsUnit',
              'OwnFarmOrBusiness', 'SameAs7DayJob', 'Derivation']],
           'labor.parquet')
