#!/usr/bin/env python
import numpy as np
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

idxvars = dict(j='y5_hhid',
               m='domain')

myvars = dict(Rural=('y5_rural',lambda x: x.lower()!='urban'))

df = df_data_grabber('../Data/hh_sec_a.dta',idxvars,**myvars)

# There are hundreds of nans in domain, but looking at "region1",
# which is a finer geographic division, suggests that these are all
# in the "mainland other urban" domain.
df = df.rename({np.nan:'Mainland Other Urban'},level='m')

# Capitalization of regions is irregular...
df = df.rename(index=lambda s:s.title(),level='m')

assert df.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(df,'other_features.parquet')
