"""Build people_last7days for Niger ECVMA 2011-12 (GAP 3, item-level).

Single source file: ecvmaind_p1p2.dta (the individual roster, which carries
the ms04 employment / time-use module).  Follows the WB .do individual-labor
recipe (NER_ECVMA1.do:1372-1449) except for the hours formula (see UNITS
below, GH #877), and keeps the REPORTED per-individual values -- no
nb_members_working_age rollup.

ID / grain: i = str(int(hid)) (2011-12 hid = grappe*100+menage); pid =
format_id(ms01q00), matching household_roster's pid.  Grain (t, i, pid).

Reported fields:
  farm_work  ms04q03 -- worked >=1h on own field/garden or raised livestock
             in the last 30 DAYS (questionnaire 4.03), NOT the last 7 days.
  SOB_work   ms04q05 -- worked >=1h in own/household business in the last
             30 DAYS (questionnaire 4.05).
  wage_work  ms04q02 -- worked >=1h for an enterprise / the state / an
             employer in the last 12 MONTHS (questionnaire 4.02).  The
             30-day analogue is ms04q01 (4.01); the WB uses q02 and so do
             we, for parity.  2011-12 asks NO 7-day participation question
             at all (2014-15's MS04Q01-03 do use 7 days).  Reference periods
             confirmed from the Stata labels + questionnaire, GH #877; the
             choice of what to serve is left open there.
  working_age ms01q06a (age) >= 6  (the survey's working-age threshold)
  Industry   ms04q24 activity-section code -> broad industry label
             (Agriculture/Fishing/Mining/Manufacturing/Construction/Services)
  farm_hrs / SB_hrs / wage_hrs : annual-average weekly hours per job,
             allocated to farm / own-business (SB) / wage by the job's
             occupation code (ms04q23 / ms04q51), then summed across the two
             jobs.  Set to 0 for non-working-age members (matching the WB
             code).  See the UNITS note below: this wave DIVERGES from
             NER_ECVMA1.do:1413, which is dimensionally wrong (GH #877).

UNITS (GH #877, 2026-09-12 -- confirmed from the Stata variable labels in
BOTH the French and the English-labelled source, and from questionnaire
ECVMA_Quest_MEN_P1_V10_ENG.pdf section 4 part B):
  ms04q29 / ms04q55  months this job was done in the last 12   (months/year)
  ms04q30 / ms04q56  hours per DAY usually devoted to this job (hours/day)
  ms04q31 / ms04q57  days per WEEK usually devoted to this job (days/WEEK)
                     -- questionnaire 4.31: "How many days per week does
                     [NAME] usually devote to this work?"
`d * h` is therefore ALREADY usual hours per week.  The WB's
`(month * hour * day) / 52` (NER_ECVMA1.do:1413, commented "week average of
hours") treats `d` as days per MONTH, so it delivers (months/52) x usual
weekly hours -- 0.23x the intended figure for a 12-month job, and ~4.3x
below the 2014-15 wave's dimensionally coherent
`(month * week * day * hour) / 52`.  We divide by 12 instead, i.e. impute
the exact 52/12 weeks per month that 2011-12 does not ask for:

    annual hours = m * (52/12) * d * h     ;  weekly average = that / 52
                                           ;  == (m * d * h) / 12

This is a DELIBERATE departure from WB parity.  Residual vs 2014-15: that
wave's reported weeks-per-month (MS04Q26) is 4 for 86% of jobs (mean 3.78),
not 52/12 = 4.33, so the same schedule still comes out ~1.15x higher here
than there.  The alternative (divide by 13, matching a reported w=4) was
considered and rejected: 52/12 is exact, and 4 weeks/month is a respondent
approximation in the other wave, not a property of this one.

DECLARED MISSING CODES (same issue).  All six inputs declare a `manquant`
code in the file's own value labels -- ms04q29/q30/q55/q56 -> 99,
ms04q31/q57 -> 9 -- and `convert_categoricals=False` hands those back as
quantities (99 hours a day, 9 days in a week).  They are NA'd per variable
via `niger._num_no_declared_missing`, reading the labels at build time.  Per
variable, NEVER blanket: 610 people genuinely report ms04q30 == 9 hours a
day, and that variable's missing code is 99.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import (get_categorical_mapping, get_dataframe,
                                      to_parquet, format_id)
from niger import (i as niger_i, _industry_label, _yn_bool,
                   _finish_people_last7days, _num_no_declared_missing)


# Occupation-code sets the WB uses to classify a job as farm vs own-business
# (SB) vs wage (NER_ECVMA1.do:1417-1428).
FARM_CODES = {1101, 1102, 1103, 1104, 1105, 1106, 1107,
              1201, 1202, 1203, 1204, 1205}
SB_CODES = {6101, 6202, 6203, 6204, 6205, 6206, 6207,
            6209, 6210, 6211, 6212}


SRC = '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaind_p1p2.dta'
src = get_dataframe(SRC, convert_categoricals=False)
# The file's own value labels, so the declared `manquant` code of each time-use
# variable can be NA'd per variable rather than guessed (GH #877).
VALUE_LABELS = get_categorical_mapping(SRC)


def _num(col):
    return pd.to_numeric(src[col], errors='coerce')


i_val = src['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
pid = src['ms01q00'].apply(format_id)

age = _num('ms01q06a')
working_age = (age >= 6)

farm_work = _yn_bool(src['ms04q03'])
SOB_work = _yn_bool(src['ms04q05'])
wage_work = _yn_bool(src['ms04q02'])

industry = _industry_label(src['ms04q24'])
# Self-employment / not-worked filter the WB applies before keeping industry.
self_emp = _num('ms04q26').isin([4]) | _num('ms04q25').isin([6, 7, 8])
not_worked = _num('ms04q22') == 2
industry = industry.where(~(self_emp | not_worked).values, pd.NA)

# --- usual weekly hours per job, allocated to farm/SB/wage --------------
unemployed = (_num('ms04q11') == 2) & (_num('ms04q12') == 2)


# Months -> weeks.  2011-12 does not ask weeks-per-month (2014-15's MS04Q26
# does), so we impute the exact calendar value.  See the UNITS note above.
WEEKS_PER_MONTH = 52 / 12


def _hours_input(col):
    # NA the variable's OWN declared `manquant` code before it is multiplied
    # into the formula: ms04q29/30/55/56 declare 99, ms04q31/57 declare 9.
    return _num_no_declared_missing(src, col, VALUE_LABELS)


def _job_hours(month_c, day_c, hour_c):
    # m = months/year, d = days/WEEK, h = hours/day  (labels + questionnaire).
    # annual hours / 52 == (m * WEEKS_PER_MONTH * d * h) / 52 == (m*d*h)/12.
    m, d, h = (_hours_input(month_c), _hours_input(day_c),
               _hours_input(hour_c))
    hrs = (m * WEEKS_PER_MONTH * d * h) / 52
    return hrs.where(~unemployed, 0)


av_hours1 = _job_hours('ms04q29', 'ms04q31', 'ms04q30')
av_hours2 = _job_hours('ms04q55', 'ms04q57', 'ms04q56')


def _classify(occ_col):
    occ = _num(occ_col)
    farm = occ.isin(FARM_CODES)
    sb = occ.isin(SB_CODES)
    wage = occ.notna() & ~farm & ~sb
    return farm, sb, wage


farm1, sb1, wage1 = _classify('ms04q23')
farm2, sb2, wage2 = _classify('ms04q51')

farm_hrs = (av_hours1.where(farm1, 0).fillna(0)
            + av_hours2.where(farm2, 0).fillna(0))
SB_hrs = (av_hours1.where(sb1, 0).fillna(0)
          + av_hours2.where(sb2, 0).fillna(0))
wage_hrs = (av_hours1.where(wage1, 0).fillna(0)
            + av_hours2.where(wage2, 0).fillna(0))

# WB zeroes the activity fields for non-working-age members.
for s in (farm_hrs, SB_hrs, wage_hrs):
    s[~working_age.values] = 0

df = pd.DataFrame({
    'i': i_val.values,
    'pid': pid.values,
    'farm_work': farm_work.values,
    'SOB_work': SOB_work.values,
    'wage_work': wage_work.values,
    'farm_hrs': farm_hrs.values,
    'SB_hrs': SB_hrs.values,
    'wage_hrs': wage_hrs.values,
    'Industry': industry.values,
    'working_age': working_age.values,
})

df = _finish_people_last7days(df, '2011-12')

assert len(df) > 0, 'people_last7days 2011-12 produced no rows'
to_parquet(df, 'people_last7days.parquet')
