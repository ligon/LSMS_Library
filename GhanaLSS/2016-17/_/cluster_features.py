#!/usr/bin/env python
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet, df_from_orgfile, format_id, get_categorical_mapping

regiond = get_categorical_mapping(tablename='region')

idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh', lambda x: "2016-17"),
               )

myvars = dict(v=('clust',format_id),
              Region='region',
              Rural='loc2')

of = df_data_grabber('../Data/g7sec8h.dta',idxvars,**myvars)

assert len(set(of.Region.value_counts().index.tolist()).difference(regiond.values()))==0, "Non-canonical labels for region."

# Aggregate to cluster level
of = of.groupby('v').head(1).reset_index().set_index(['h','w','v'])
of = of.droplevel('h')

if __name__=='__main__':
    to_parquet(of,'other_features.parquet')
