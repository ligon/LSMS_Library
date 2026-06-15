"""Build people_last7days for Niger ECVMA 2011-12 (GAP 3, item-level).

Single source file: ecvmaind_p1p2.dta (the individual roster, which carries
the ms04 employment / time-use module).  Mirrors the WB .do individual-labor
recipe (NER_ECVMA1.do:1372-1449) exactly, but keeps the REPORTED
per-individual values — no nb_members_working_age rollup.

ID / grain: i = str(int(hid)) (2011-12 hid = grappe*100+menage); pid =
format_id(ms01q00), matching household_roster's pid.  Grain (t, i, pid).

Reported fields:
  farm_work  ms04q03 (1=Oui worked on own farm last 7 days)
  SOB_work   ms04q05 (1=Oui worked in own business last 7 days)
  wage_work  ms04q02 (1=Oui worked for a wage last 7 days)
  working_age ms01q06a (age) >= 6  (the survey's working-age threshold)
  Industry   ms04q24 activity-section code -> broad industry label
             (Agriculture/Fishing/Mining/Manufacturing/Construction/Services)
  farm_hrs / SB_hrs / wage_hrs : usual weekly hours, computed as the WB does
             — av weekly hours per job = month*day*hour/52 (ms04q29-31 job1,
             ms04q55-57 job2), allocated to farm / own-business (SB) / wage
             by the job's occupation code (ms04q23 / ms04q51), then summed
             across the two jobs.  Set to 0 for non-working-age members
             (matching the WB code).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _industry_label, _yn_bool,
                   _finish_people_last7days)


# Occupation-code sets the WB uses to classify a job as farm vs own-business
# (SB) vs wage (NER_ECVMA1.do:1417-1428).
FARM_CODES = {1101, 1102, 1103, 1104, 1105, 1106, 1107,
              1201, 1202, 1203, 1204, 1205}
SB_CODES = {6101, 6202, 6203, 6204, 6205, 6206, 6207,
            6209, 6210, 6211, 6212}


src = get_dataframe('../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaind_p1p2.dta',
                    convert_categoricals=False)


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


def _job_hours(month_c, day_c, hour_c):
    m, d, h = _num(month_c), _num(day_c), _num(hour_c)
    hrs = (m * d * h) / 52
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
