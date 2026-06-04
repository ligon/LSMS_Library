#!/usr/bin/env python
"""Concatenate wave-level interview_date data for Cambodia."""
import pandas as pd
from lsms_library.local_tools import to_parquet, get_dataframe

X = []
for t in ['2019-20']:
    X.append(get_dataframe('../%s/_/interview_date.parquet' % t))

x = pd.concat(X, axis=0)

to_parquet(x, '../var/interview_date.parquet')
