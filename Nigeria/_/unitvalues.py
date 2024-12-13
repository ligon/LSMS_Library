#!/usr/bin/env python

import pandas as pd
import numpy as np
import json

P = []
for t in ['2010-11','2012-13','2015-16']: #,'2018-19']:
    P.append(pd.read_parquet('../%s/_/unitvalues.parquet' % t))

p = pd.concat(P,axis=0)

# Eliminate infinities
p = p.replace(np.inf,np.nan)

with open('aggregate_items.json') as f:
    lbl = json.load(f)

p = p.rename(columns=lbl['Aggregated Label'])

p.columns.name = 'i'
p = p.groupby('i',axis=1).sum()

p = p.replace(0,np.nan)

p.to_parquet('../var/unitvalues.parquet')
