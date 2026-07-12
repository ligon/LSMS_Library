import pandas as pd, os
from lsms_library.local_tools import data_root
for rel in ['Mali/2014-15/_/community_prices.parquet', 'Mali/var/community_prices.parquet']:
    p = os.path.join(str(data_root()), rel)
    df = pd.read_parquet(p)
    uniq = df.reset_index().duplicated(['t', 'v', 'j', 'u']).sum() == 0
    print(f"{rel}: index={df.index.names} shape={df.shape} cols={list(df.columns)} unique(t,v,j,u)={uniq}")
