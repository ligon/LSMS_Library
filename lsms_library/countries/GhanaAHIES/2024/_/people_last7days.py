#!/usr/bin/env python
"""Individual 7-day labour participation for GhanaAHIES 2024, all four
quarters (t = 2024Q1 .. 2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta, Section 4A ("current economic
activity status and characteristics of main job"; RESPONDENTS: ALL HOUSEHOLD
MEMBERS AGED 5 YEARS OR OLDER -- questionnaire header and field manual s9.4).
Read once through get_dataframe.  (t, i, pid) index as in household_roster;
the same 'Not member anymore' filter is applied (45 answered rows in 2024).
Schema: the Guinea-Bissau / Niger people_last7days shape.

Columns:
  wage_work    s4aq1   worked >= 1 h in the past 7 days for a wage / salary /
                       commission / in-kind pay for a NON-member (Q1).
  farm_work    s4aq7   worked >= 1 h on a farm owned or rented by a member
                       of the household -- crops, livestock, fishing (Q7).
  SOB_work     s4aq11  ran / managed a non-farm enterprise owned by the
                       household for >= 1 h (Q11).
  wage_hrs     sum of s4aq3a-g, the hours worked on that activity on each of
  farm_hrs     sum of s4aq9a-g    the seven days (the manual: "for the ..
  SB_hrs       sum of s4aq13a-g   days worked, how many hours .. in each of
                       the days").  NOTE s4aq2 / s4aq8 / s4aq12 are DAYS
                       (1-7) despite the .dta label on s4aq2 saying "hours":
                       the manual's Q2 is "for how many days", the range is
                       1-7, and it equals the count of days with hours > 0 on
                       99.9% of rows.  Daily entries above 24 h exist (7 rows,
                       max 66 / 70) and are served as recorded.
  Industry     s4aq41a1, the ISIC Rev.4 SECTION label of the person's main
                       job ('Agriculture, forestry and fishing', ...), asked
                       after the interviewer check Q28 of anyone who worked
                       in the 7 days or (Q29-Q38) has a job to return to.
                       Served as recorded: 19,755 rows carry an industry
                       with none of the three served activities Yes --
                       10,808 of them did no 7-day work at all (temporarily
                       absent, s4aq28 = No), the rest did one of the five
                       unserved forms listed below.
  working_age  Age >= 5 -- the module's own eligibility threshold, which is
                       the corpus convention (EHCVM 6, Malawi 5), NOT the
                       ILO 15.  Age = s1aq4y completed years.  s4aq1 is null
                       for every member under 5 and for 790 members aged 5+
                       (median age 5, 548 of them in 2024Q4).

THE MODULE IS A FIRST-YES CASCADE, NOT A SET OF INDEPENDENT DUMMIES.  The
CAPI asks the eight activity questions in order (Q1 wage, Q4 domestic, Q7
farm, Q11 non-farm enterprise, Q15 family help, Q19 fishing / gathering, Q22
apprentice, Q25 voluntary) and, once one is Yes, records that activity's days
and hours and skips the REST (measured exactly: null(s4aq11) = null(s4aq7) +
yes(s4aq7), and so on down the list; 390 rows are answered after a prior Yes).
So farm_work is NA for a wage worker and SOB_work is NA for anyone who
answered Yes to wage, domestic or farm work.  Those NAs are served as NA (a
nullable bool, as the schema declares) -- never as False -- and a "share
doing farm work" must be computed on the answered rows only.  The five forms
not served (2024 Yes counts: domestic 447, family help 7,341, fishing /
gathering 417, apprentice 3,305, voluntary 241) sit below the served ones in
that order.

GSS's own dummies (`worked`, `wagework`, `farmwork`, `nonfarmwork`,
`employed`) are used as a cross-check only, never served: wagework =
yes(s4aq1) | yes(s4aq29 temporary absence) exactly (16,007 = 14,723 + 1,284);
farmwork and nonfarmwork add the temporary-absence Yes (s4aq30 / s4aq32) and
REMOVE production 'Only for own/family use' (s4aq10 / s4aq14: 1,148 of the
1,155 farm and all 67 non-farm disagreements) -- the ILO employment boundary,
which this table deliberately does not draw.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import NOT_MEMBER_LABEL, person_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'
WORKING_AGE = 5          # Section 4A eligibility: members aged 5 years or older
DAYS = 'abcdefg'
ACTIVITIES = {           # column -> (yes/no question, per-day hours stem)
    'wage_work': ('s4aq1', 's4aq3'),
    'farm_work': ('s4aq7', 's4aq9'),
    'SOB_work': ('s4aq11', 's4aq13'),
}
HOURS = {'wage_work': 'wage_hrs', 'farm_work': 'farm_hrs', 'SOB_work': 'SB_hrs'}


def _strip(s: pd.Series) -> pd.Series:
    out = s.astype(object).where(s.notna(), pd.NA)
    return out.map(lambda x: x.strip() if isinstance(x, str) else x)


def _yes_no(s: pd.Series) -> pd.Series:
    """'Yes' -> True, 'No' -> False, anything else (unasked, skipped by the
    first-yes cascade) -> NA, as a nullable boolean."""
    lab = _strip(s)
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(lab == 'Yes', True)
    out = out.mask(lab == 'No', False)
    unexpected = lab.dropna().loc[~lab.dropna().isin(['Yes', 'No'])]
    assert unexpected.empty, f'unexpected labels: {unexpected.unique()[:5]}'
    return out


def _hours(df: pd.DataFrame, stem: str) -> pd.Series:
    """Sum of the seven per-day hour entries; NA when none is reported."""
    days = df[[f'{stem}{d}' for d in DAYS]].apply(pd.to_numeric, errors='coerce')
    over = int((days > 24).sum().sum())
    if over:
        print(f'people_last7days 2024: {stem}a-g: {over} daily entries above 24 h '
              f'(max {days.max().max()}), served as recorded')
    return days.sum(axis=1, min_count=1).astype('Float64')


def build(df: pd.DataFrame) -> pd.DataFrame:
    rel = _strip(df['s1aq2x'])
    departed = (rel == NOT_MEMBER_LABEL).fillna(False)

    out = person_keys(df)
    for col, (q, stem) in ACTIVITIES.items():
        out[col] = _yes_no(df[q])
        out[HOURS[col]] = _hours(df, stem)
        yes = out[col].fillna(False)
        assert out.loc[yes, HOURS[col]].notna().all(), f'{col} yes without hours'
    out['Industry'] = _strip(df['s4aq41a1']).astype('string')
    age = pd.to_numeric(df['s1aq4y'], errors='coerce')
    out['working_age'] = (age >= WORKING_AGE).where(age.notna(), pd.NA).astype('boolean')

    n_departed = int((departed & out['wage_work'].notna()).sum())
    print(f'people_last7days 2024: dropping {int(departed.sum())} "{NOT_MEMBER_LABEL}" rows '
          f'({n_departed} with a Section 4A answer)')
    out = out.loc[~departed]
    out = out[['t', 'i', 'pid', 'farm_work', 'SOB_work', 'wage_work',
               'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']]
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    assert out.index.is_unique, 'duplicate (t, i, pid) in people_last7days'

    # The cascade, measured: every skipped question is NA, reported per column.
    print('people_last7days 2024: answered / yes / NA per column',
          {c: (int(out[c].notna().sum()), int(out[c].fillna(False).sum()), int(out[c].isna().sum()))
           for c in ACTIVITIES})
    absent = out['Industry'].notna() & ~out[list(ACTIVITIES)].fillna(False).any(axis=1)
    print(f'people_last7days 2024: Industry present on {int(out["Industry"].notna().sum())} rows, '
          f'{int(absent.sum())} of them with no served 7-day activity')
    return out


if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    out = build(df)
    print('people_last7days 2024:', out.groupby(level='t').size().to_dict())
    to_parquet(out, 'people_last7days.parquet')
