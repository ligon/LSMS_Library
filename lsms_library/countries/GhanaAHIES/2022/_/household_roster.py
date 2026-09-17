#!/usr/bin/env python
"""Household roster for GhanaAHIES 2022, all four quarters (t = 2022Q1 ..
2022Q4) from the one person x quarter file.

Source: ../Data/2022 AHIES Q1-Q4_Rev_20250827.dta, Section 1A.  Read once
through get_dataframe.  Produces a (t, i, pid)-indexed roster; cluster
identity (v) is joined from sample() at API time, never baked in.

Keys: i = "{cluster}-{HholdID}"; pid = new_pid; t = `quarter` as shipped
('2022Q1' .. '2022Q4').  0 nulls / 0 duplicates on the key; person rows
52,914 / 53,617 / 53,130 / 50,282 per quarter.

Columns:
  Sex          s1aq1 ('Male'/'Female'; canonical M/F applied at read).
  Age          s1aq4y (completed years, never null, 0-120) + s1aq4m/12 where
               the months-beyond-years field is a valid 0-11 (23,129 rows
               filled; 11 carry the '98' don't-know code and are ignored).
  Relationship s1aq2 -- the RAW relationship-to-head code.  THIS IS THE
               ONE SUBSTANTIVE DIFFERENCE FROM 2024, where GSS's corrected
               `s1aq2x` is used: in the 2022 file `s1aq2x` is 100% NULL
               (209,943 of 209,943; it is one of 319 empty columns in the
               shared 838-column layout), and the raw `s1aq2` is already
               clean here -- 0 null codes, exactly one 'Head' in 41,922 of
               42,120 household-quarters and 0 in the other 198, none
               double-headed (2024's raw s1aq2 had 31 double-headed and
               4,561 nulls, which is why the corrected column exists there).
               The assert below makes a GSS re-release that populates
               `s1aq2x` fail loudly instead of silently serving the raw
               code.  Kinship (Generation/Distance/Affinity) is expanded from
               the label at read via kinship.yml; the 2022 label set is
               identical to 2024's.

Departed members: code 17 'Not member anymore' (a CAPI code absent from the
paper questionnaire) marks a panel line whose person has left the
household; such rows are dropped and counted, as in 2024.  In 2022 there
are NONE (0 rows on s1aq2 in every quarter -- the code first appears in
2024Q1), so the served roster equals the person file: the filter is kept
for parity and prints its count.  See CONTENTS.org s"2022 folder".
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import NOT_MEMBER_LABEL, person_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2022 AHIES Q1-Q4_Rev_20250827.dta'
RELATIONSHIP = 's1aq2'   # 2024 uses s1aq2x; empty in the 2022 file (see docstring)

df = get_dataframe(PERSON_FILE)

assert df['s1aq2x'].isna().all(), (
    "s1aq2x is populated in this 2022 file -- it was 100% null in the "
    "2025-08-27 release, which is why Relationship reads the raw s1aq2 here. "
    "Re-measure both columns (CONTENTS.org s'2022 folder') before choosing.")

rel = df[RELATIONSHIP].astype(object).where(df[RELATIONSHIP].notna(), pd.NA)
rel = rel.map(lambda x: x.strip() if isinstance(x, str) else x)

departed = rel == NOT_MEMBER_LABEL
n_departed = int(departed.sum())
print(f'household_roster 2022: dropping {n_departed} "{NOT_MEMBER_LABEL}" rows '
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
