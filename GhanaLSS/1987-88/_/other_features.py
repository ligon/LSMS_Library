#!/usr/bin/env python
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id
from collections import defaultdict

#rural = df_from_orgfile('./categorical_mapping.org',name='rural',encoding='ISO-8859-1')
#rurald = rural.set_index('Code').to_dict('dict')['Label']
region = df_from_orgfile('../../_/categorical_mapping.org',name='region',encoding='ISO-8859-1')
regiond = defaultdict(lambda: np.nan,region.set_index('Code').to_dict('dict')['Label'])

# Painful hack. Data seems *not to include* information on region that household
# or cluster is in! But a Q about birthplace is close?
# Operating on the assumption that remittances will be most often
# given locally, infer region from this for each cluster.

idxvars = dict(j=('HID',format_id),
               indiv=('PID',format_id)
               )

myvars = dict(cluster=('CLUST',format_id),
              m='REGION',
              Age = ('AGEY',lambda x: pd.to_numeric(x,errors='coerce')),
              )

df = df_data_grabber('../Data/Y01A.DAT',idxvars,**myvars)

# Just use younger kids, on grounds that migration is less likely?
youngsters = df.query("Age<12")
foo = youngsters.reset_index().groupby(['cluster','m']).count().squeeze()
idx = foo.groupby('cluster').idxmax()['j']

region = idx.apply(lambda x: regiond[str(x[1])])
region.name = 'm'

# Merge region info to get hh level dataframe
df = df[['cluster']].join(region,on='cluster')[['m']].groupby(['j','m']).head(1)
df = df.droplevel('indiv')
df['t'] = '1987-88'
df['Rural'] = np.nan
df = df.reset_index().set_index(['j','t','m'])

#no data on specific region and rural/urban classification
to_parquet(df,'other_features.parquet')
