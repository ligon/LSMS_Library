import sys
import pandas as pd
cb = pd.read_parquet(f'{sys.argv[1]}/cluster_features.parquet').xs('2016-17', level='t')
ct = pd.read_parquet(f'{sys.argv[2]}/cluster_features.parquet').xs('2016-17', level='t')
m = (cb.Rural.astype(str) != ct.Rural.astype(str)).values
print('base', cb.Rural[m].tolist(), '-> tip', ct.Rural[m].tolist())
