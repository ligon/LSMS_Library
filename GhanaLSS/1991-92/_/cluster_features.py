#!/usr/bin/env python
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id, get_categorical_mapping

regiond = get_categorical_mapping(tablename='region')
rurald = get_categorical_mapping(tablename='rural')

idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh', lambda x: "1991-92"),
               )

myvars = dict(v=('clust',format_id),
              Region=('region',lambda x: regiond[f"{x:3.0f}".strip()]),
              Rural=('loc2',rurald))

of = df_data_grabber('../Data/POV_GH.DTA',idxvars,**myvars)

# Aggregate to cluster level
of = of.groupby('v').head(1).reset_index().set_index(['h','w','v'])
of = of.droplevel('h')

if __name__=='__main__':
    to_parquet(of,'other_features.parquet')
