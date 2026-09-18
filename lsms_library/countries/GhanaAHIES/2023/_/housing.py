#!/usr/bin/env python
"""Housing (dwelling characteristics) for GhanaAHIES 2023, all four quarters
(t = 2023Q1 .. 2023Q4) from the household x quarter Sections 5-7 file.

Source: ../Data/2023 AHIES Q1-Q4_SEC567_edt.dta, Section 7 (asked every
quarter).  One row per household-quarter, (t, i) index; categorical
columns per the GhanaLSS sibling's housing shape plus Kosovo's `Type`:

  Type         s7aq1  type of dwelling (12 labels)
  Rooms        s7aq2  rooms occupied by the household
  Tenure       s7bq1  present holding arrangement, spelled as GLSS7 does
                      (Owning -> Own, Renting -> Rent, Rent-free, Perching,
                      Squatting, Caretaker)
  Roof         s7eq3  main roof material  -> GLSS7 intermediate labels, then
  Floor        s7eq2  main floor material    the country `Roof`/`Floor` org
  Walls        s7eq1  main outer-wall material   tables (API time)
  Water        s7dq1  main source of drinking water (16 labels)
  Toilet       s7dq24 toilet facility usually used (12 labels)
  Electricity  s7dq10a first-listed source of electricity used in the past
                      3 months ('National Grid Connection', 'No Electric
                      Power', solar, ...).  GLSS7 served the "most-used"
                      source (s7dq11a); AHIES asks a multi-response list and
                      this is its first entry.

Value labels in the .dta carry stray whitespace and punctuation (' Semi-
detached house', 'Metal sheet.', 'Palm leaves/Thatch (grass/Raffia' with an
unclosed paren, "neighbour?s" for an apostrophe); labels are stripped and
those known defects repaired before mapping.  Unmapped Roof/Floor/Walls
labels fall to 'Other' (only 'Other (specify)' does so, as in 2024).

Keys: as food_security.py ((t, i); t asserted to be '2023Q1' .. '2023Q4';
9,915 / 9,862 / 10,027 / 9,827 rows per quarter).

What differs from 2024: the 2023 SEC567 file (132 columns; no `qtr`, no
`s6q8*`, no weight columns) has NO null-HholdID non-response shells (2024
had 332), so the shell drop is a guard that removes 0 rows.  Every 2023
label of s7aq1 / s7bq1 / s7eq1-3 / s7dq1 / s7dq24 / s7dq10a is in the 2024
vocabulary (same stray whitespace and '?s' defects), so the tables below are
unchanged; only 'Other (specify)' falls to 'Other'.  72 household-quarters
answered Section 7A (dwelling type, rooms) but carry NA on the three
material questions (2024: 3) and 71-164 on the utility questions; they are
served with NA in those columns, not dropped.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import household_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

HH_FILE = '../Data/2023 AHIES Q1-Q4_SEC567_edt.dta'

ROOF = {
    'Metal sheet.': 'Metal Sheet', 'Metal sheet': 'Metal Sheet',
    'Slate/Asbestos': 'Slate/Asbestos',
    'Cement blocks/concrete': 'Cement/Concrete',
    'Palm leaves/Thatch (grass/Raffia': 'Thatch',
    'Palm leaves/Thatch (grass/Raffia)': 'Thatch',
    'Mud bricks/earth': 'Mud/Earth',
    'Roofing Tiles': 'Roofing Tiles',
    'Wood': 'Wood', 'Bamboo': 'Bamboo',
    'Other (specify)': 'Other',
}
FLOOR = {
    'Cement/Concrete': 'Cement/Concrete', 'Earth/Mud': 'Earth/Mud',
    'Ceramic/Porcelain/Granite/ Marble tiles': 'Ceramic/Marble Tiles',
    'Ceramic/Porcelain/Granite/Marble tiles': 'Ceramic/Marble Tiles',
    'Vinyl tiles': 'Vinyl Tiles',
    'Terrazzo/Terrazzo tiles': 'Terrazzo',
    'Burnt bricks': 'Burnt Bricks', 'Stone': 'Stone', 'Wood': 'Wood',
    'Other (specify)': 'Other',
}
WALLS = {
    'Mud bricks/earth': 'Mud/Earth', 'Wood': 'Wood',
    'Metal sheet/slate/asbestos': 'Metal/Asbestos', 'Stone': 'Stone',
    'Burnt bricks': 'Burnt Bricks',
    'Cement blocks/concrete': 'Cement/Concrete',
    'Landcrete': 'Landcrete', 'Bamboo': 'Bamboo',
    'Palm leaves/Thatch (grass/Raffia': 'Thatch',
    'Palm leaves/Thatch (grass/Raffia)': 'Thatch',
    'Other (specify)': 'Other',
}
TENURE = {
    'Owning': 'Own', 'Renting': 'Rent', 'Rent-free': 'Rent-free',
    'Perching': 'Perching', 'Squatting': 'Squatting', 'Caretaker': 'Caretaker',
}


def clean(s: pd.Series) -> pd.Series:
    """Strip whitespace and repair the known label defects; NA stays NA."""
    return s.map(lambda x: (x.strip().replace('?s', "'s").replace('(specify)', '(specify)')
                            if isinstance(x, str) else pd.NA))


def mapped(s: pd.Series, table: dict, fallback='Other') -> pd.Series:
    s = clean(s)
    out = s.map(table)
    unmapped = sorted(set(s[out.isna() & s.notna()]))
    if unmapped:
        print(f'housing 2023: labels not in table -> {fallback!r}: {unmapped}')
    return out.where(out.notna() | s.isna(), fallback)


QUARTERS = {'2023Q1', '2023Q2', '2023Q3', '2023Q4'}

df = get_dataframe(HH_FILE)
assert set(df['quarter'].astype(str).unique()) == QUARTERS, df['quarter'].unique()

shell = df['HholdID'].isna()
print(f'housing 2023: dropping {int(shell.sum())} rows with null HholdID (2024 had 332; 2023 expected 0)')
df = df.loc[~shell]

h = household_keys(df)
h['Type'] = clean(df['s7aq1'])
h['Rooms'] = pd.to_numeric(df['s7aq2'], errors='coerce').astype('Int64')
h['Tenure'] = mapped(df['s7bq1'], TENURE, fallback=pd.NA)
h['Roof'] = mapped(df['s7eq3'], ROOF)
h['Floor'] = mapped(df['s7eq2'], FLOOR)
h['Walls'] = mapped(df['s7eq1'], WALLS)
h['Water'] = clean(df['s7dq1'])
h['Toilet'] = clean(df['s7dq24'])
h['Electricity'] = clean(df['s7dq10a'])

h = h.set_index(['t', 'i']).sort_index()
assert h.index.is_unique, 'duplicate (t, i) in housing'

to_parquet(h, 'housing.parquet')
