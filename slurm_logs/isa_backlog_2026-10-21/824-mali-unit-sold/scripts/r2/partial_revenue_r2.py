"""2017-18 households with an unrecorded sale that gross_crop_revenue still serves.  Aggregates only."""
import sys, warnings; warnings.simplefilter('ignore')
import pandas as pd
old, new = sys.argv[1], sys.argv[2]
OUT = '/local/job40390255/data-private/824-ml-r2/out'
b = pd.read_parquet(f'{OUT}/{old}_served.parquet'); n = pd.read_parquet(f'{OUT}/{new}_served.parquet')
t = n.index.get_level_values('t'); i = n.index.get_level_values('i')
moved = ((b.Value_sold == 0) & n.Value_sold.isna()).fillna(False).to_numpy(bool)
hh = set(zip(t[moved], i[moved]))
g = pd.read_parquet(f'{OUT}/{new}_gross_crop_revenue.parquet').iloc[:, 0]
both = [k for k in hh if k in g.index]
print(f'households with >=1 sale row moved 0 -> NA: {len(hh)}; still served a gross_crop_revenue: {len(both)} '
      f'(zero {sum(g[k] == 0 for k in both)}, positive {sum(g[k] > 0 for k in both)})')
