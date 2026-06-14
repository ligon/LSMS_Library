"""Build people_last7days for Niger ECVMA 2014-15 (GAP 3, item-level).

Two source files (the WB ${labor_hh} merged to ${indiv_roster}):
  ECVMA2_MS04P1.dta — the ms04 employment / time-use module (one row per
    person; person line number in MS04Q00).
  ECVMA2_MS01P1.dta — the individual roster (age in MS01Q06A; person line
    number in MS01Q00).
Merged on (GRAPPE, MENAGE, EXTENSION, person line number), exactly as
NER_ECVMA2.do:1392-1473.  Keeps the REPORTED per-individual values — no
nb_members_working_age rollup.

ID / grain: i from (GRAPPE, MENAGE) via niger.i (matching household_roster /
sample, which omit EXTENSION); pid = format_id(MS01Q00), matching
household_roster's pid.  Grain (t, i, pid).

Reported fields:
  farm_work  MS04Q01 (1=Oui own farm last 7 days)
  SOB_work   MS04Q02 (1=Oui own business last 7 days)
  wage_work  MS04Q03 (1=Oui wage work last 7 days)
  working_age MS01Q06A (age) >= 6
  Industry   MS04Q23 activity-section code -> broad industry label
  farm_hrs/SB_hrs/wage_hrs : usual weekly hours, av per job =
             month*week*day*hour/52 (MS04Q25-28 job1, MS04Q51-53/51B job2),
             allocated farm/SB/wage by occupation code (MS04Q22/MS04Q48),
             summed across jobs, zeroed for non-working-age members.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _industry_label, _yn_bool,
                   _finish_people_last7days)


FARM_CODES = {1101, 1102, 1103, 1104, 1105, 1106, 1107,
              1201, 1202, 1203, 1204, 1205}
SB_CODES = {6101, 6202, 6203, 6204, 6205, 6206, 6207,
            6209, 6210, 6211, 6212}

base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
lab = get_dataframe(base + 'ECVMA2_MS04P1.dta', convert_categoricals=False)
ros = get_dataframe(base + 'ECVMA2_MS01P1.dta', convert_categoricals=False)

key = ['GRAPPE', 'MENAGE', 'EXTENSION']
ros_small = ros[key + ['MS01Q00', 'MS01Q06A']].copy()
lab = lab.rename(columns={'MS04Q00': 'MS01Q00'})
src = lab.merge(ros_small, on=key + ['MS01Q00'], how='left')


def _num(col):
    return pd.to_numeric(src[col], errors='coerce')


i_val = src.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE']],
                                              index=['GRAPPE', 'MENAGE'])),
                  axis=1)
pid = src['MS01Q00'].apply(format_id)

age = _num('MS01Q06A')
working_age = (age >= 6)

farm_work = _yn_bool(src['MS04Q01'])
SOB_work = _yn_bool(src['MS04Q02'])
wage_work = _yn_bool(src['MS04Q03'])

industry = _industry_label(src['MS04Q23'])
self_emp = _num('MS04Q24').isin([4]) | _num('MS04Q29').isin([7, 8, 9])
not_worked = _num('MS04Q20') == 2
industry = industry.where(~(self_emp | not_worked).values, pd.NA)

unemployed = (_num('MS04Q05') == 2) & (_num('MS04Q06') == 2)


def _job_hours(month_c, week_c, day_c, hour_c):
    m, w, d, h = _num(month_c), _num(week_c), _num(day_c), _num(hour_c)
    hrs = (m * w * d * h) / 52
    return hrs.where(~unemployed, 0)


av_hours1 = _job_hours('MS04Q25', 'MS04Q26', 'MS04Q27', 'MS04Q28')
av_hours2 = _job_hours('MS04Q51', 'MS04Q51B', 'MS04Q53', 'MS04Q52')


def _classify(occ_col):
    occ = _num(occ_col)
    farm = occ.isin(FARM_CODES)
    sb = occ.isin(SB_CODES)
    wage = occ.notna() & ~farm & ~sb
    return farm, sb, wage


farm1, sb1, wage1 = _classify('MS04Q22')
farm2, sb2, wage2 = _classify('MS04Q48')

farm_hrs = (av_hours1.where(farm1, 0).fillna(0)
            + av_hours2.where(farm2, 0).fillna(0))
SB_hrs = (av_hours1.where(sb1, 0).fillna(0)
          + av_hours2.where(sb2, 0).fillna(0))
wage_hrs = (av_hours1.where(wage1, 0).fillna(0)
            + av_hours2.where(wage2, 0).fillna(0))

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

df = _finish_people_last7days(df, '2014-15')

assert len(df) > 0, 'people_last7days 2014-15 produced no rows'
to_parquet(df, 'people_last7days.parquet')
