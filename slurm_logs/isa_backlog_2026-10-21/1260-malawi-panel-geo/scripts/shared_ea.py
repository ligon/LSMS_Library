import sys
import pandas as pd
B, T = sys.argv[1], sys.argv[2]
wt = pd.read_parquet(f'{T}/wave_2016-17.parquet').reset_index()
is_cs = wt['i'].astype(str).str.startswith('cs-')
shared = set(wt.loc[is_cs, 'v']) & set(wt.loc[~is_cs, 'v'])
assert len(shared) == 1
v = shared.pop()
e = wt[wt.v == v].assign(half=lambda d: d['i'].astype(str).str.startswith('cs-').map({True: 'CS', False: 'panel'}))
print('households by half', e.half.value_counts().to_dict())
print('Rural by half', e.groupby('half')['Rural'].value_counts().to_dict())
print('distinct coords by half', e.groupby('half')[['Latitude', 'Longitude']].nunique().to_dict())
cs_fix = e[e.half == 'CS'][['Latitude', 'Longitude']].drop_duplicates()
print('CS distinct fixes', len(cs_fix))
panel_at_cs_fix = e[(e.half == 'panel')].merge(cs_fix, on=['Latitude', 'Longitude']).shape[0]
print('panel households at the CS fix', panel_at_cs_fix)
cb = pd.read_parquet(f'{B}/cluster_features.parquet').xs('2016-17', level='t')
ct = pd.read_parquet(f'{T}/cluster_features.parquet').xs('2016-17', level='t')
rb, rt = cb.loc[v], ct.loc[v]
print('served Rural base', rb.Rural, 'tip', rt.Rural)
print('served coordinate: base equals CS fix', bool(((cs_fix.Latitude == rb.Latitude) & (cs_fix.Longitude == rb.Longitude)).any()),
      '| tip equals CS fix', bool(((cs_fix.Latitude == rt.Latitude) & (cs_fix.Longitude == rt.Longitude)).any()))
print('first household in merged order is', e.iloc[0].half)
# 2019-20 id shapes
w9 = pd.read_parquet(f'{T}/wave_2019-20.parquet').reset_index()
s = w9['i'].astype(str)
print('2019-20 i shapes: all-digit', int(s.str.fullmatch(r'\d+').sum()), '| contains "-"', int(s.str.contains('-').sum()), '| starts cs-', int(s.str.startswith('cs-').sum()))
