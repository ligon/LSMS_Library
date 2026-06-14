"""Build people_last7days (per-individual 7-day activity) for Mali EACI
2017-18.

GAP 3b (parity loop).  One row per (t, i, pid), mirroring Uganda's
per-individual labor/time-use feature.

Source: eaci17_s04p1.dta (the s4 labor/time-use block) merged with
eaci17_s01p1.dta (the s01 demographic block, for the s1q04a age needed by
working_age) on (grappe, exploitation, codeid).  pid = (grappe,
exploitation, codeid), the SAME person key as household_roster.

REPORTED per-individual columns only (no rollups):
  farm_work / SOB_work / wage_work — 7-day activity dummies (s4q01/02/03).
  farm_hrs  / SB_hrs / wage_hrs    — average weekly hours per activity, the
                                     WB (months*days*hours)/52 construction.
  Industry — wage-work industry (harmonize_industry Code) from the WB ind_*
             split over s4q13 (zeroed for self-employment / no-work).
  working_age — s1q04a >= 6.

Variable map traced from MLI_EACI2.do:1340-1427:
  farm_work s4q01; SOB_work s4q02; wage_work s4q03.
  working_age = s1q04a >= 6.
  industry over s4q13: ag 1-4, fish 5, mining 6-7, manuf 8-29, const 30,
    serv 31-43; zeroed where s4q14==3 | s4q18 in {7,8,9} (self-emp) or
    s4q11==2 (did not work).
  hours: job1 = s4q15(months)*s4q17(hours)*s4q16(days)/52; job2 = the
    s4q30/32/31 analogues; attributed by occupation code (s4q12/s4q27).
"""
import sys

sys.path.append('../../_/')
import pandas as pd
import numpy as np

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, pid as mali_pid, people_last7days_finalize

WAVE = '2017-18'

# convert_categoricals=False keeps the integer survey codes that the WB
# recodes expect (dummies 1=Oui/2=Non; industry/occupation integer codes).
lab = get_dataframe('../Data/eaci17_s04p1.dta', convert_categoricals=False).copy()
ros = get_dataframe('../Data/eaci17_s01p1.dta', convert_categoricals=False).copy()

# Bring the age variable s1q04a in from the roster; merge on the person key.
keys = ['grappe', 'exploitation', 'codeid']
age_cols = keys + (['s1q04a'] if 's1q04a' in ros.columns else [])
src = lab.merge(ros[age_cols], on=keys, how='left', suffixes=('', '_ros'))

src['i'] = src.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])), axis=1)
src['pid'] = src.apply(
    lambda r: mali_pid(pd.Series([r['grappe'], r['exploitation'], r['codeid']])), axis=1)


def _num(col):
    if col in src.columns:
        return pd.to_numeric(src[col], errors='coerce')
    return pd.Series(np.nan, index=src.index)


def _dummy(col):
    """WB recode `(2=0)(.=NA)(else=1)` on the integer Oui/Non code:
    1 (Oui) -> True, 2 (Non) -> False, NA -> NA."""
    s = _num(col)
    out = pd.Series(pd.NA, index=src.index, dtype='boolean')
    out = out.mask(s.eq(2), False)
    out = out.mask(s.eq(1), True)
    return out


farm_work = _dummy('s4q01')
SOB_work = _dummy('s4q02')
wage_work = _dummy('s4q03')

age = _num('s1q04a')
working_age = (age >= 6)
working_age = working_age.astype('boolean').mask(age.isna(), pd.NA)

# industry over s4q13, zeroed for self-employment / no-work.
ind = _num('s4q13')
q14 = _num('s4q14')
q18 = _num('s4q18')
q11 = _num('s4q11')
zero_ind = (q14.eq(3) | q18.isin([7, 8, 9]) | q11.eq(2))
ind_frame = pd.DataFrame({
    'ind_ag': ind.between(1, 4),
    'ind_fish': ind.eq(5),
    'ind_mining': ind.isin([6, 7]),
    'ind_manuf': ind.between(8, 29),
    'ind_const': ind.eq(30),
    'ind_serv': ind.between(31, 43),
})
ind_frame = ind_frame.mask(zero_ind, False)
def _first_industry(row):
    for c in ['ind_ag', 'ind_fish', 'ind_mining', 'ind_manuf', 'ind_const', 'ind_serv']:
        if bool(row[c]):
            return c
    return pd.NA
Industry = ind_frame.apply(_first_industry, axis=1).astype('string')


# Clear the EACI "Manquant" sentinel (99) on each component first (none
# present in 2017-18, but kept for parity with the 2014-15 script so a
# missing months/hours/days code can never multiply into an impossible
# >168-hour week).
_HRS_SENTINELS = [99, 999, 9999]


def _hrs_num(col):
    s = _num(col)
    return s.mask(s.isin(_HRS_SENTINELS))


def _av_hours(month_c, hour_c, day_c):
    return (_hrs_num(month_c) * _hrs_num(hour_c) * _hrs_num(day_c)) / 52.0


av1 = _av_hours('s4q15', 's4q17', 's4q16')   # job 1
av2 = _av_hours('s4q30', 's4q32', 's4q31')   # job 2

occ1 = _num('s4q12')
occ2 = _num('s4q27')


def _farm_job(occ):
    return occ.isin([11, 12])


def _sb_job(occ):
    return occ.eq(62)


def _wage_job(occ):
    return occ.isin([20, 21, 22, 23, 24, 25, 31, 41, 42, 43, 51, 52, 61, 63, 71, 72, 81])


def _act_hrs(jobmask1, av_1, jobmask2, av_2, work_dummy):
    h = pd.Series(np.nan, index=src.index)
    h = h.where(~jobmask1.fillna(False), av_1)
    h2 = pd.Series(np.nan, index=src.index)
    h2 = h2.where(~jobmask2.fillna(False), av_2)
    total = h.add(h2, fill_value=0)
    total = total.where(work_dummy.fillna(False).astype(bool), 0.0)
    return total


farm_hrs = _act_hrs(_farm_job(occ1), av1, _farm_job(occ2), av2, farm_work)
SB_hrs = _act_hrs(_sb_job(occ1), av1, _sb_job(occ2), av2, SOB_work)
wage_hrs = _act_hrs(_wage_job(occ1), av1, _wage_job(occ2), av2, wage_work)

df = pd.DataFrame({
    't': WAVE, 'i': src['i'], 'pid': src['pid'],
    'farm_work': farm_work, 'SOB_work': SOB_work, 'wage_work': wage_work,
    'farm_hrs': farm_hrs, 'SB_hrs': SB_hrs, 'wage_hrs': wage_hrs,
    'Industry': Industry, 'working_age': working_age,
})

df = people_last7days_finalize(df)

assert len(df) > 0, "people_last7days 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,pid) in people_last7days 2017-18"

to_parquet(df, 'people_last7days.parquet')
