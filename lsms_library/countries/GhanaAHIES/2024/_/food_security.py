#!/usr/bin/env python
"""Food security (FAO FIES, 8 items) for GhanaAHIES 2024, all four quarters
(t = 2024Q1 .. 2024Q4) from the household x quarter Sections 5-7 file.

Source: ../Data/2024 AHIES Q1-Q4_SEC01567_edt.dta (39,298 x 147), Section 5:
s5q1..s5q8, the eight FIES experience items with a 12-MONTH recall, asked of
the household respondent once per quarter.  Items map in order to the
canonical names (Worried, HealthyDiet, FewFoods, SkippedMeal, AteLess,
RanOut, Hungry, WholeDay): Yes -> True, No -> False, "Don't Know" (s5q6
only, 639 rows) -> NA.  FIES_score = count of True over the eight items, NA
only when all eight are NA.  Nothing is aggregated.

Keys: i = "{cluster}-{HholdID}", t = `quarter`.  332 rows (81/94/95/62 per
quarter) carry a null HholdID and no Section 5-7 content at all; their
quarter-scoped `hhid` identifies them as exactly the 332 household-quarters
present in the person file but interviewed for no Section 5-7 module
(non-response shells).  They are dropped here with a count -- there is no
content to key.  After that drop the key is unique (0 duplicates).  The file's
`region` is UPPERCASE ('ASHANTI') unlike the person file's title case; it is
not served from here (sample carries region via strata).
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

HH_FILE = '../Data/2024 AHIES Q1-Q4_SEC01567_edt.dta'
ITEMS = {
    'Worried': 's5q1', 'HealthyDiet': 's5q2', 'FewFoods': 's5q3',
    'SkippedMeal': 's5q4', 'AteLess': 's5q5', 'RanOut': 's5q6',
    'Hungry': 's5q7', 'WholeDay': 's5q8',
}
YESNO = {'Yes': True, 'No': False}

df = get_dataframe(HH_FILE)

shell = df['HholdID'].isna()
print(f'food_security 2024: dropping {int(shell.sum())} rows with null HholdID '
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
