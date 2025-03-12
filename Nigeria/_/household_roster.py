import pandas as pd
import json
import numpy as np

X = []
for t in ['2010-11','2012-13','2015-16','2018-19']:
    X.append(pd.read_parquet('../%s/_/household_roster.parquet' % t))

x = pd.concat(X,axis=0)

x.to_parquet('../var/household_roster.parquet')
