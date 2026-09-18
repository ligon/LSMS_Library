#!/usr/bin/env python
"""Anthropometry (reported body measures) for GhanaAHIES 2022, all four
quarters (t = 2022Q1 .. 2022Q4) from the one person x quarter file.

Thin caller: the table is built by `ghanaahies.build_anthropometry`, which is
shared by all three year folders and states the rule (Section 3B Q3-Q5, the
GAP 5 reported-measures shape, the negative not-measured decode, the
departed-member filter).  What that rule TOUCHES differs by year -- the
not-measured encoding in particular -- so the builder measures and prints it
per year and `_/CONTENTS.org` records the per-year numbers.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import build_anthropometry, person_file, wave_year  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

YEAR = wave_year(__file__)
PERSON_FILE = person_file(__file__)

if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    out = build_anthropometry(df, YEAR)
    print(f'anthropometry {YEAR}:', out.groupby(level='t').size().to_dict())
    to_parquet(out, 'anthropometry.parquet')
