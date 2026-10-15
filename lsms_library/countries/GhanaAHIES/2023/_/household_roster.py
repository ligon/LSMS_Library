#!/usr/bin/env python
"""Household roster for GhanaAHIES 2023, all four quarters (t = 2023Q1 ..
2023Q4) from the one person x quarter file.

Source: ../Data/2023 AHIES Q1-Q4_Rev_20250827.dta, Section 1A (same
838-column layout as 2024).  Read once through get_dataframe.  Produces a
(t, i, pid)-indexed roster; cluster identity (v) is joined from sample() at
API time, never baked in.

Keys: i = "{cluster}-{HholdID}"; pid = new_pid; t = `quarter` ('2023Q1' ..
'2023Q4', asserted).  0 nulls / 0 duplicates on the key in the source.

Columns:
  Sex          s1aq1 ('Male'/'Female'; canonical M/F applied at read).
  Age          s1aq4y (completed years, never null, 0-116) + s1aq4m/12 where
               the months-beyond-years field is filled (under-fives only:
               0 rows with months and years >= 5).
  Relationship see below.  Kinship (Generation/Distance/Affinity) is
               expanded from the label at read via kinship.yml; the 2023
               label set is identical to 2024's.

WHAT DIFFERS FROM 2024 -- the relationship column is chosen PER QUARTER.
2024 reads `s1aq2x`, GSS's corrected relationship-to-head code, throughout.
In the 2023 file `s1aq2x` is unfilled in 2023Q1: of Q1's 50,039 rows, 31,779
are null and 10,012 carry an unlabelled numeric 0.0, so only 8,248 rows have
a label and only 1,583 of the 10,131 Q1 household-quarters get exactly one
head from it (8,547 get none).  The raw `s1aq2` is complete in Q1 (0 nulls;
9,915 one-head / 216 headless / 0 double-headed).  In Q2-Q4 the picture is
the 2024 one: `s1aq2x` is complete (6 nulls, all Q4) with one head in
10,044 / 10,024 / 9,825 household-quarters, while `s1aq2` has 3,205 nulls in
Q4 and 251 / 61 / 261 headless.  So:

    2023Q1        <- s1aq2     (s1aq2x 83.5% unfilled)
    2023Q2..Q4    <- s1aq2x    (as 2024)

This is a per-wave choice of SOURCE COLUMN, not a row-wise composite: a
row-wise coalesce (s1aq2x where labelled, else s1aq2) was measured and
rejected because the Q1 fill is partial WITHIN 329 households and it
manufactures 4 double-headed households there; the per-quarter rule gives
0.  Cost of the rule: the 168 Q1 rows where a filled s1aq2x disagrees with
s1aq2 are served as s1aq2 says.  Where both columns carry a label (151,957
rows) they disagree on 5,058, 3,223 of them in Q4 -- mostly code 17 below
and Child <-> Grandchild / Other relative recodes.

Departed members: code 17 'Not member anymore' (a CAPI code absent from the
paper questionnaire) marks a panel line whose person has left; the survey
has no residence-duration question to carry this as MonthsSpent, so those
rows are DROPPED here and counted -- 0 / 17 / 11 / 1,382 for Q1-Q4 under the
rule above (1,410 rows; 2024 had 52).  The Q4 cluster is GSS's end-of-year
clean-up: the same 1,382 rows are ordinary members in `s1aq2`, and the
person file shrinks from 50,741 rows in 2023Q4 to 47,963 in 2024Q1.
individual_education applies the same relationship rule and filter.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import NOT_MEMBER_LABEL, person_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2023 AHIES Q1-Q4_Rev_20250827.dta'

# Per-quarter relationship source (see the docstring).  individual_education.py
# carries an identical copy; keep the two in step.
RELATIONSHIP_SOURCE = {'2023Q1': 's1aq2', '2023Q2': 's1aq2x',
                       '2023Q3': 's1aq2x', '2023Q4': 's1aq2x'}


def relationship(df: pd.DataFrame) -> pd.Series:
    """Relationship label per row from the quarter's chosen source column;
    non-string codes (nulls, the unlabelled 0.0) become NA."""
    q = df['quarter'].astype(str)
    assert set(q.unique()) == set(RELATIONSHIP_SOURCE), sorted(q.unique())
    out = pd.Series(pd.NA, index=df.index, dtype=object)
    for t, col in RELATIONSHIP_SOURCE.items():
        m = (q == t).values
        out.loc[m] = df.loc[m, col].astype(object).map(
            lambda x: x.strip() if isinstance(x, str) else pd.NA).values
    return out


df = get_dataframe(PERSON_FILE)

rel = relationship(df)

departed = (rel == NOT_MEMBER_LABEL).fillna(False).astype(bool)
n_departed = int(departed.sum())
print(f'household_roster 2023: dropping {n_departed} "{NOT_MEMBER_LABEL}" rows '
      f'({df.loc[departed].groupby("quarter", observed=True).size().to_dict()})')
heads = (rel == 'Head').groupby([df['quarter'].astype(str), df['cluster'], df['HholdID']]).sum()
print('household_roster 2023: heads per household-quarter by quarter:',
      {t: g.value_counts().sort_index().to_dict() for t, g in heads.groupby(level=0)})

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
