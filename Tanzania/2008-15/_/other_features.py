#!/usr/bin/env python
import numpy as np
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

round_match = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}

idxvars = dict(j='r_hhid',
               t=('round',round_match),
               m=('domain',lambda s:s.title()),
               uphi='UPHI')

myvars = dict(Rural=('urb_rur',lambda x: x.lower()!='urban'))

df = df_data_grabber('../Data/upd4_hh_a.dta',idxvars,**myvars)

# Splitoffs in later rounds retroactively added to earlier rounds.
# This leads to double-counting if we're focused on households.
# Drop this retroactive additions.

df = df.sort_index().droplevel('uphi')
df = df.loc[~df.index.duplicated(keep='first')]

assert df.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(df,'other_features.parquet')
