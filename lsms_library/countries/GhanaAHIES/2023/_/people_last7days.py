#!/usr/bin/env python
"""Individual 7-day labour participation for GhanaAHIES 2023, all four quarters
(t = 2023Q1 .. 2023Q4) from the one person x quarter file.

Thin caller: the table is built by `ghanaahies.build_people_last7days`, which
is shared by all three year folders and states the rule (Section 4A, the
Guinea-Bissau / Niger shape, working_age = Age >= 5, and the FIRST-YES CASCADE
whose skipped questions are served as NA and never as False).  The builder
re-measures the cascade identity for the year it is given rather than
inheriting it; the per-year answered / yes / NA counts are in
`_/CONTENTS.org`.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import build_people_last7days, person_file, wave_year  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

YEAR = wave_year(__file__)
PERSON_FILE = person_file(__file__)

if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    out = build_people_last7days(df, YEAR)
    print(f'people_last7days {YEAR}:', out.groupby(level='t').size().to_dict())
    to_parquet(out, 'people_last7days.parquet')
