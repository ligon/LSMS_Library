#!/usr/bin/env python
"""Food security (FAO FIES, 8 items) for GhanaAHIES 2023, all four quarters
(t = 2023Q1 .. 2023Q4) from the household x quarter Sections 5-7 file.

Source: ../Data/2023 AHIES Q1-Q4_SEC567_edt.dta (39,631 x 132), Section 5:
s5q1..s5q8, the eight FIES experience items with a 12-MONTH recall, asked of
the household respondent once per quarter.  Items map in order to the
canonical names (Worried, HealthyDiet, FewFoods, SkippedMeal, AteLess,
RanOut, Hungry, WholeDay): Yes -> True, No -> False, "Don't Know" (s5q6
only, 599 rows) -> NA.  FIES_score = count of True over the eight items, NA
only when all eight are NA.  Nothing is aggregated.

Keys: i = "{cluster}-{HholdID}", t = `quarter` ('2023Q1' .. '2023Q4',
asserted); 9,915 / 9,862 / 10,027 / 9,827 rows per quarter, key unique.

What differs from 2024: the 2023 file has 132 columns, not 147 -- it lacks
`qtr`, the eleven `s6q8*`/`s6q0b` respondent-occupation columns and the
three weight columns (`hhwt_q9`, `wtpp_q10`, `hhwt_q10`); none is used
here.  It has NO null-HholdID non-response shells (2024 had 332), so the
shell drop below is a guard that removes 0 rows.  The 216 / 251 / 58 / 97
household-quarters present in the person file but absent here were simply
not interviewed for Sections 5-7.  `region` is UPPERCASE as in 2024 and is
not served from here.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

HH_FILE = '../Data/2023 AHIES Q1-Q4_SEC567_edt.dta'
QUARTERS = {'2023Q1', '2023Q2', '2023Q3', '2023Q4'}
ITEMS = {
    'Worried': 's5q1', 'HealthyDiet': 's5q2', 'FewFoods': 's5q3',
    'SkippedMeal': 's5q4', 'AteLess': 's5q5', 'RanOut': 's5q6',
    'Hungry': 's5q7', 'WholeDay': 's5q8',
}
YESNO = {'Yes': True, 'No': False}

df = get_dataframe(HH_FILE)
assert set(df['quarter'].astype(str).unique()) == QUARTERS, df['quarter'].unique()

shell = df['HholdID'].isna()
print(f'food_security 2023: dropping {int(shell.sum())} rows with null HholdID '
      f'(2024 had 332 such non-response shells; 2023 is expected to have 0)')
df = df.loc[~shell]

fs = household_keys(df)
for col, var in ITEMS.items():
    fs[col] = (df[var].map(lambda x: x.strip() if isinstance(x, str) else x)
                       .map(YESNO).astype('boolean'))

items = fs[list(ITEMS)]
score = items.sum(axis=1, skipna=True)
score = score.where(~items.isna().all(axis=1), other=pd.NA).astype('Int64')
fs['FIES_score'] = score

fs = fs.set_index(['t', 'i']).sort_index()
assert fs.index.is_unique, 'duplicate (t, i) in food_security'

to_parquet(fs, 'food_security.parquet')
