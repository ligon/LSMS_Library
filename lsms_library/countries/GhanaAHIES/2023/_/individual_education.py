#!/usr/bin/env python
"""Individual educational attainment for GhanaAHIES 2023, all four quarters
(t = 2023Q1 .. 2023Q4) from the one person x quarter file.

Source: ../Data/2023 AHIES Q1-Q4_Rev_20250827.dta, Section 2A (asked of
members aged 3+).  Read once through get_dataframe.  (t, i, pid) index as in
household_roster; the same per-quarter relationship rule and 'Not member
anymore' filter are applied so the two person tables agree (see
household_roster.py: 2023Q1 reads s1aq2 because s1aq2x is 83.5% unfilled
there; Q2-Q4 read s1aq2x as 2024 does; 1,410 departed rows dropped).

Educational Attainment is built from three questions exactly as in 2024:
  s2aq1  ever attended: 'Never attended' -> 'No education'; NA (12,701
         rows, under-threes, mean age 1.2; 65 are aged 3+) -> NA -> the row
         is dropped by the framework's dropna(how='all') (not asked);
  s2aq3  highest LEVEL attending now / attended (16 labels, the 2024 set);
  s2aq4  highest GRADE COMPLETED at that level (0-7; 0 = none yet).
For the five graded levels the composite "<level> <grade>" label decides
incomplete vs complete; every other level maps from its label alone, via
`harmonize_education` in ../../_/categorical_mapping.org (no 2023 label
falls outside that table).  Residues in 2023: an unlabelled 0.0 in s2aq3
(81 rows; 401 in 2024) and attended-but-level-missing (351 rows; 596 in
2024) -> 'Unknown'.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import (NOT_MEMBER_LABEL, education_label,  # noqa: E402
                        harmonize_education_labels, person_keys)
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2023 AHIES Q1-Q4_Rev_20250827.dta'

# Identical copy of household_roster.py's rule; keep the two in step.
RELATIONSHIP_SOURCE = {'2023Q1': 's1aq2', '2023Q2': 's1aq2x',
                       '2023Q3': 's1aq2x', '2023Q4': 's1aq2x'}


def relationship(df: pd.DataFrame) -> pd.Series:
    q = df['quarter'].astype(str)
    assert set(q.unique()) == set(RELATIONSHIP_SOURCE), sorted(q.unique())
    out = pd.Series(pd.NA, index=df.index, dtype=object)
    for t, col in RELATIONSHIP_SOURCE.items():
        m = (q == t).values
        out.loc[m] = df.loc[m, col].astype(object).map(
            lambda x: x.strip() if isinstance(x, str) else pd.NA).values
    return out


df = get_dataframe(PERSON_FILE)

departed = (relationship(df) == NOT_MEMBER_LABEL).fillna(False).astype(bool)

attended = df['s2aq1'].map(lambda x: x.strip() if isinstance(x, str) else x)
level = df['s2aq3']
grade = pd.to_numeric(df['s2aq4'], errors='coerce')

raw = pd.Series([education_label(l, g) for l, g in zip(level, grade)],
                index=df.index, dtype=object)
raw = raw.where(attended != 'Never attended', 'Never attended')
# Asked (3+) and attended, but no usable level -> Unknown; not asked -> NA.
raw = raw.where(~(attended.notna() & raw.isna()), 'Unknown')
raw = raw.where(attended.notna(), pd.NA)

edu = person_keys(df)
edu['Educational Attainment'] = harmonize_education_labels(raw)
edu = edu.loc[~departed]
edu = edu.set_index(['t', 'i', 'pid']).sort_index()
assert edu.index.is_unique

print(f'individual_education 2023: dropped {int(departed.sum())} "{NOT_MEMBER_LABEL}" rows;',
      edu['Educational Attainment'].value_counts(dropna=False).to_dict())

to_parquet(edu, 'individual_education.parquet')
