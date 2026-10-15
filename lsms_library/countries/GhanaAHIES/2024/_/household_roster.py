#!/usr/bin/env python
"""Household roster for GhanaAHIES 2024, all four quarters (t = 2024Q1 ..
2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta, Section 1A.  Read once
through get_dataframe.  Produces a (t, i, pid)-indexed roster; cluster
identity (v) is joined from sample() at API time, never baked in.

Keys: i = "{cluster}-{HholdID}"; pid = new_pid (person line within the
household, stable across quarters: Q1 x Q2 join 45,890 of 47,963 with sex
agreeing 100% -- README); t = `quarter`.  0 nulls / 0 duplicates on the key.

Columns:
  Sex          s1aq1 ('Male'/'Female'; canonical M/F applied at read).
  Age          s1aq4y (completed years, never null) + s1aq4m/12 where the
               months-beyond-years field is filled (under-5s, 0-11 months),
               so infants carry a fractional age as other countries' DOB-
               derived ages do.
  Relationship s1aq2x -- GSS's CORRECTED relationship-to-head code, not the
               raw s1aq2.  Measured on 2024: s1aq2x has exactly one 'Head' in
               38,983 of 39,298 household-quarters (315 headless, 2 null
               codes); s1aq2 has 31 double-headed and 1,423 headless
               household-quarters and 4,561 nulls.  The two disagree on
               10,249 rows.  Kinship (Generation/Distance/Affinity) is
               expanded from the label at read via kinship.yml.

Departed members: code 17 'Not member anymore' is a CAPI code absent from the
paper questionnaire (whose list ends at 16 = Househelp).  It marks a panel
line whose person has left the household; the survey has no residence-
duration question that could carry this as MonthsSpent, so serving the row
would count a non-member in household_characteristics.  Those rows are
DROPPED here and counted (52 rows on s1aq2x in 2024, all in 2024Q1; see
CONTENTS.org); individual_education applies the same filter.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import NOT_MEMBER_LABEL, person_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'

df = get_dataframe(PERSON_FILE)

rel = df['s1aq2x'].astype(object).where(df['s1aq2x'].notna(), pd.NA)
rel = rel.map(lambda x: x.strip() if isinstance(x, str) else x)

departed = rel == NOT_MEMBER_LABEL
n_departed = int(departed.sum())
print(f'household_roster 2024: dropping {n_departed} "{NOT_MEMBER_LABEL}" rows '
      f'({df.loc[departed].groupby("quarter", observed=True).size().to_dict()})')

years = pd.to_numeric(df['s1aq4y'], errors='coerce')
months = pd.to_numeric(df['s1aq4m'], errors='coerce')
months_ok = months.between(0, 11)
age = years + np.where(months_ok, months / 12.0, 0.0)

roster = person_keys(df)
roster['Sex'] = df['s1aq1'].astype(object).where(df['s1aq1'].notna(), pd.NA)
roster['Age'] = age.astype(float)
roster['Relationship'] = rel
roster = roster.loc[~departed]

roster = roster.set_index(['t', 'i', 'pid']).sort_index()
assert roster.index.is_unique, 'duplicate (t, i, pid) in the roster'

to_parquet(roster, 'household_roster.parquet')
