"""Build people_last7days (per-individual 7-day activity) for Mali EACI
2014-15.

GAP 3b (parity loop).  One row per (t, i, pid), mirroring Uganda's
per-individual labor/time-use feature.

Source: EACIIND_p1.dta (the individual roster, which carries both the s01
demographic block and the s04 labor/time-use block).  pid = (grappe,
menage, s01q00), the SAME person key as household_roster.

REPORTED per-individual columns only (no rollups):
  farm_work / SOB_work / wage_work — 7-day activity dummies (s04q01/02/03).
  farm_hrs  / SB_hrs / wage_hrs    — average weekly hours per activity, the
                                     WB (months*days*hours)/52 construction
                                     attributed to whichever job is of that
                                     type.
  Industry — wage-work industry (harmonize_industry Code) from the WB ind_*
             split over s04q22 (zeroed for self-employment).
  working_age — s01q04a >= 6 (the WB floor).

Variable map traced from MLI_EACI1.do:1435-1520:
  farm_work s04q01 (Non->0, else 1); SOB_work s04q02; wage_work s04q03.
  working_age = s01q04a >= 6.
  industry over s04q22: ag 11-40, fish 51-52, mining 71-72, manuf 81-292,
    const 301-302, serv 310-430; zeroed where s04q23==4 (self-employed).
  hours: job1 = s04q24(months)*s04q25(hours)*s04q26(days)/52; job2 = the
    s04q47/48/49 analogues; attributed to farm/SB/wage by the job's
    occupation code (s04q21 / s04q44).  Filter zeros where the person
    answered 'no' to the activity filter (s04q05==2 & s04q06==2) or did not
    work (s04q19==2).
"""
import sys

sys.path.append('../../_/')
import pandas as pd
import numpy as np

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, pid as mali_pid, people_last7days_finalize

WAVE = '2014-15'

# convert_categoricals=False keeps the integer survey codes that the WB
# recodes expect (dummies 1=Oui/2=Non/9=Manquant; industry/occupation
# integer codes); the labelled-string default would break every numeric cut.
src = get_dataframe('../Data/EACIIND_p1.dta', convert_categoricals=False).copy()
src['i'] = src.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)
src['pid'] = src.apply(
    lambda r: mali_pid(pd.Series([r['grappe'], r['menage'], r['s01q00']])), axis=1)


def _num(col):
    if col in src.columns:
        return pd.to_numeric(src[col], errors='coerce')
    return pd.Series(np.nan, index=src.index)


def _dummy(col):
    """WB recode `(2=0)(9 .=NA)(else=1)` on the integer Oui/Non code:
    1 (Oui) -> True, 2 (Non) -> False, 9 / NA (Manquant) -> NA."""
    s = _num(col)
    out = pd.Series(pd.NA, index=src.index, dtype='boolean')
    out = out.mask(s.eq(2), False)
    out = out.mask(s.eq(1), True)
    return out


# 7-day activity dummies
farm_work = _dummy('s04q01')
SOB_work = _dummy('s04q02')
wage_work = _dummy('s04q03')

# working_age = age >= 6
age = _num('s01q04a')
working_age = (age >= 6)
working_age = working_age.astype('boolean').mask(age.isna(), pd.NA)

# industry over s04q22 (primary-job occupation/industry code), zeroed for
# self-employment (s04q23 == 'A son propre compte' coded 4 in the WB recode).
ind = _num('s04q22')
# WB: `replace ind_* = 0 if s04q23==4` (self-employment / own-account).
self_emp = _num('s04q23').eq(4)
ind_ag = (ind.between(11, 40))
ind_fish = (ind.between(51, 52))
ind_mining = (ind.isin([71, 72]))
ind_manuf = (ind.between(81, 292))
ind_const = (ind.isin([301, 302]))
ind_serv = (ind.between(310, 430))
ind_frame = pd.DataFrame({
    'ind_ag': ind_ag, 'ind_fish': ind_fish, 'ind_mining': ind_mining,
    'ind_manuf': ind_manuf, 'ind_const': ind_const, 'ind_serv': ind_serv,
})
# zero out self-employment (matches the WB `replace = 0 if s04q23==4`)
ind_frame = ind_frame.mask(self_emp, False)
# reduce to one Code per individual (first True; NA if none)
def _first_industry(row):
    for c in ['ind_ag', 'ind_fish', 'ind_mining', 'ind_manuf', 'ind_const', 'ind_serv']:
        if bool(row[c]):
            return c
    return pd.NA
Industry = ind_frame.apply(_first_industry, axis=1).astype('string')

# average weekly hours per job, then attributed to farm/SB/wage by job type.
# Clear the EACI "Manquant" sentinel (99) on each component first, so a
# missing months/hours/days code does not multiply into an impossible
# >168-hour week (e.g. 99*99*99/52).
_HRS_SENTINELS = [99, 999, 9999]


def _hrs_num(col):
    s = _num(col)
    return s.mask(s.isin(_HRS_SENTINELS))


def _av_hours(month_c, hour_c, day_c):
    return (_hrs_num(month_c) * _hrs_num(hour_c) * _hrs_num(day_c)) / 52.0

av1 = _av_hours('s04q24', 's04q25', 's04q26')   # job 1
av2 = _av_hours('s04q47', 's04q48', 's04q49')   # job 2

# occupation code of each job -> activity class (WB recode of s04q21/s04q44).
occ1 = _num('s04q21')
occ2 = _num('s04q44')


def _farm_job(occ):
    return occ.isin([11, 12])


def _sb_job(occ):
    return occ.eq(62)


def _wage_job(occ):
    return occ.isin([21, 22, 23, 24, 25, 41, 42, 43, 51, 52, 61, 63, 71, 72, 81])


def _act_hrs(jobmask1, av_1, jobmask2, av_2, work_dummy):
    h = pd.Series(np.nan, index=src.index)
    h = h.where(~jobmask1.fillna(False), av_1)
    h2 = pd.Series(np.nan, index=src.index)
    h2 = h2.where(~jobmask2.fillna(False), av_2)
    total = h.add(h2, fill_value=0)
    # zero out where the person did not do that activity at all
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

assert len(df) > 0, "people_last7days 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,pid) in people_last7days 2014-15"

to_parquet(df, 'people_last7days.parquet')
