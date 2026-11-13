"""gross_crop_revenue old vs new: what was dropped and what moved.  Aggregates only."""
import sys, warnings; warnings.simplefilter('ignore')
import numpy as np, pandas as pd
old, new = sys.argv[1], sys.argv[2]
OUT = sys.argv[3] if len(sys.argv) > 3 else '/local/job40390255/data-private/824-ml-r2/out'
n = pd.read_parquet(f'{OUT}/{new}_served.parquet')
for name in ['gross_crop_revenue', 'gross_crop_revenue_j']:
    a = pd.read_parquet(f'{OUT}/{old}_{name}.parquet').iloc[:, 0]; z = pd.read_parquet(f'{OUT}/{new}_{name}.parquet').iloc[:, 0]
    gone = a.index.difference(z.index); added = z.index.difference(a.index)
    print(f'{name} {old}->{new}: dropped {len(gone)} (old values: zero {int((a[gone]==0).sum())}, other {int((a[gone]!=0).sum())}; waves {pd.Index(gone.get_level_values("t")).value_counts().to_dict()}); added {len(added)}')
    common = a.index.intersection(z.index)
    d = ~np.isclose(a[common].to_numpy(float), z[common].to_numpy(float), equal_nan=True)
    ch = common[d]
    # which changed rows have 2017-18 tree sales?
    tr = n[(n.perennial.fillna(False)) & (n.index.get_level_values('t') == '2017-18') & (n.Value_sold > 0)]
    hh = set(zip(tr.index.get_level_values('t'), tr.index.get_level_values('i')))
    keys = list(zip(ch.get_level_values('t'), ch.get_level_values('i')))
    print(f'   changed in place {int(d.sum())}: by wave {pd.Index(ch.get_level_values("t")).value_counts().to_dict()}; '
          f'all up {bool((z[ch] > a[ch]).all())}; on a household with a 2017-18 tree sale {sum(k in hh for k in keys)}; '
          f'sum of change {float((z[ch]-a[ch]).sum()):.6g}')
