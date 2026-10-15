#!/usr/bin/env python
"""Cluster features (Region, District, Rural) for GhanaAHIES 2022, all four
quarters (t = 2022Q1 .. 2022Q4) from the one person x quarter file.

Thin caller: the table is built by `ghanaahies.build_cluster_features`, which
is shared by all three year folders and states the rule (built directly at the
(t, v) grain from the person file's design variables, with the within-cluster-
quarter constancy of the three attributes ASSERTED and the row count per
quarter measured).  Per-year numbers are in `_/CONTENTS.org`.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import build_cluster_features, person_file, wave_year  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

YEAR = wave_year(__file__)
PERSON_FILE = person_file(__file__)

if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    cf = build_cluster_features(df, YEAR)
    print(f'cluster_features {YEAR}:', cf.groupby(level='t').size().to_dict(),
          '| districts', cf['District'].nunique(), '| regions', cf['Region'].nunique())
    to_parquet(cf, 'cluster_features.parquet')
