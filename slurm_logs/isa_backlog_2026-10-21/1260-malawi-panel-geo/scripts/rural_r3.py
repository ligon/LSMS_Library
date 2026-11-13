import sys
import pandas as pd
B, R1, R2, R3 = sys.argv[1:5]
c = {k: pd.read_parquet(f'{d}/cluster_features.parquet').xs('2016-17', level='t') for k, d in [('dev', B), ('r1', R1), ('r2', R2), ('r3', R3)]}
wf3 = pd.read_parquet(f'{R3}/wave_2016-17.parquet').reset_index()
wf2 = pd.read_parquet(f'{R2}/wave_2016-17.parquet').reset_index()
for v in c['r2'].index[(c['r2'].Rural.astype(str) != c['r3'].Rural.astype(str)).values]:
    e3 = wf3[wf3.v == v]; e2 = wf2[wf2.v == v]
    print('r2->r3 Rural EA: dev', c['dev'].loc[v, 'Rural'], '| r1', c['r1'].loc[v, 'Rural'], '| r2', c['r2'].loc[v, 'Rural'], '| r3', c['r3'].loc[v, 'Rural'],
          '| hh', len(e3), '| r2 stayers kept Rural', e2[e2.Latitude.notna()].Rural.str.lower().value_counts().to_dict(),
          '| r3 kept (on fix) Rural', e3[e3.Latitude.notna()].Rural.str.lower().value_counts().to_dict(),
          '| first kept row in r3 Rural', e3[e3.Latitude.notna()].iloc[0].Rural)
f = c['dev'].index[(c['dev'].Rural.astype(str) != c['r1'].Rural.astype(str)).values]
for v in f:
    print('round-1 flip EA: dev', c['dev'].loc[v, 'Rural'], '| r1', c['r1'].loc[v, 'Rural'], '| r2', c['r2'].loc[v, 'Rural'], '| r3', c['r3'].loc[v, 'Rural'])
