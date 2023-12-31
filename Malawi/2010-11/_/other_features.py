#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import get_other_features

with dvc.api.open('../Data/Panel/Round 1 (2010) Consumption Aggregate.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

df = get_other_features(df, '2010-11')

df.to_parquet('other_features.parquet')
