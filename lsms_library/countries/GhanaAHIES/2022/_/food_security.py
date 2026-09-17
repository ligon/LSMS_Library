#!/usr/bin/env python
"""Food security (FAO FIES, 8 items) for GhanaAHIES 2022, all four quarters
(t = 2022Q1 .. 2022Q4) from the household x quarter Sections 5-7 file.

Source: ../Data/2022 AHIES Q1-Q4_SEC567_edt.dta (42,120 x 167), Section 5:
s5q1..s5q8, the eight FIES experience items with a 12-MONTH recall, asked of
the household respondent once per quarter.  Items map in order to the
canonical names (Worried, HealthyDiet, FewFoods, SkippedMeal, AteLess,
RanOut, Hungry, WholeDay): Yes -> True, No -> False, "Don't Know" (s5q6
only, 618 rows) -> NA.  FIES_score = count of True over the eight items, NA
only when all eight are NA.  Nothing is aggregated.

Keys: i = "{cluster}-{HholdID}", t = `quarter` as shipped.  201 rows (1 / 2 /
1 / 197 per quarter) carry a null HholdID (and null qtr, hhsize, loctype)
and NO Section 5-7 content -- their only non-null s5*/s7* cells are empty
strings in the three free-text columns s7dq14 / s7dq17 / s7dq22.  Their
six-digit `hhid` (qtr + 3-digit cluster + 2-digit household, verified on all
41,919 keyed rows) would recover a unique key, and identifies them as exactly
the 201 household-quarters present in the person file but absent from the
keyed part of this file; with nothing to serve they are dropped here with a
count, as 2024's 332 shells are.  After the drop the key is unique (0
duplicates) and the table has 10,760 / 10,626 / 10,421 / 10,112 rows.

What differs from 2024: the file name (`SEC567_edt`, 167 columns: it also
carries Q4-only weight columns, Q1-only `hhsize`, and Section 10/11B
columns for Q1-Q3 -- none used here; see CONTENTS.org), the shell count,
and the "Don't Know" count.  The file's `region` is UPPERCASE ('ASHANTI')
and `loctype` is null for 10,827 rows, as in 2024; neither is served from
here.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

HH_FILE = '../Data/2022 AHIES Q1-Q4_SEC567_edt.dta'
ITEMS = {
    'Worried': 's5q1', 'HealthyDiet': 's5q2', 'FewFoods': 's5q3',
    'SkippedMeal': 's5q4', 'AteLess': 's5q5', 'RanOut': 's5q6',
    'Hungry': 's5q7', 'WholeDay': 's5q8',
}
YESNO = {'Yes': True, 'No': False}

df = get_dataframe(HH_FILE)

shell = df['HholdID'].isna()
print(f'food_security 2022: dropping {int(shell.sum())} rows with null HholdID '
      f'(no Section 5-7 content; {df.loc[shell].groupby("quarter", observed=True).size().to_dict()})')
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
