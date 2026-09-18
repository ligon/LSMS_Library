#!/usr/bin/env python
"""Individual educational attainment for GhanaAHIES 2024, all four quarters
(t = 2024Q1 .. 2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta, Section 2A (asked of members
aged 3+).  Read once through get_dataframe.  (t, i, pid) index as in
household_roster; the same 'Not member anymore' filter is applied so the two
person tables agree.

Educational Attainment is built from three questions:
  s2aq1  ever attended: 'Never attended' -> 'No education';
         NA (members under 3, 10,486 rows, mean age 1.2; 4 of them are also code-17
         rows) -> NA -> the row is
         dropped by the framework's dropna(how='all') (not asked);
  s2aq3  highest LEVEL attending now / attended (16 labels);
  s2aq4  highest GRADE COMPLETED at that level (0 = none yet; field manual
         Section 2 Q4 and Appendix 5 grading system).
For the five graded levels (Primary, JSS/JHS, Middle, SSS/SHS, Secondary) the
composite "<level> <grade>" label decides incomplete vs complete; every other
level maps from its label alone.  The mapping table is `harmonize_education`
in ../../_/categorical_mapping.org, applied here (Tanzania pattern) so the
wave parquet carries canonical labels.  Residues: an unlabelled 0.0 in s2aq3
(401 rows) and an attended-but-level-missing case (596 rows) -> 'Unknown'.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import (NOT_MEMBER_LABEL, education_label,  # noqa: E402
                        harmonize_education_labels, person_keys)
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'

df = get_dataframe(PERSON_FILE)

rel = df['s1aq2x'].map(lambda x: x.strip() if isinstance(x, str) else x)
departed = (rel == NOT_MEMBER_LABEL).fillna(False)

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

print('individual_education 2024:',
      edu['Educational Attainment'].value_counts(dropna=False).to_dict())

to_parquet(edu, 'individual_education.parquet')
