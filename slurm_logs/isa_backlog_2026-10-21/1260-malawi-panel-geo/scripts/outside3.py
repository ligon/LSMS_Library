import sys
import pandas as pd
for lab, d in [('base', sys.argv[1]), ('tip', sys.argv[2])]:
    cf = pd.read_parquet(f'{d}/cluster_features.parquet').xs('2019-20', level='t')
    wf = pd.read_parquet(f'{d}/wave_2019-20.parquet').reset_index()
    pv = set(wf.loc[wf.i.astype(str).str.contains('-'), 'v'].astype(str))
    out = ~(cf.Latitude.between(-18, -9) & cf.Longitude.between(32, 37)) & cf.Latitude.notna()
    idx = cf.index[out.values].astype(str)
    print(lab, 'served 2019-20 clusters outside loose box:', int(out.sum()), '| of which panel EAs', len(set(idx) & pv))
