"""Stayer indicator (cover dist_to_IHS3location) vs panel geo coordinates. COUNTS ONLY."""
import sys, math
import numpy as np, pandas as pd
geo_p, cov_p, key, lat, lon = sys.argv[1:6]
cs_geo_p = sys.argv[6] if len(sys.argv) > 6 else None
geo = pd.read_stata(geo_p, convert_categoricals=False)
cov = pd.read_stata(cov_p, convert_categoricals=False)
d = cov['dist_to_IHS3location']
print('cover dist_to_IHS3location: non-null', int(d.notna().sum()), 'of', len(d), '| <=1km', int((d <= 1).sum()),
      '| ==0', int((d == 0).sum()), '| quantiles', d.quantile([.25, .5, .6, .7, .75, .9]).round(2).tolist())
m = cov[[key, 'ea_id', 'dist_to_IHS3location'] + (['mover'] if 'mover' in cov else [])].merge(geo.drop(columns=['ea_id'], errors='ignore'), on=key)
if 'd_ihs3' in m:
    both = m[['dist_to_IHS3location', 'd_ihs3']].dropna()
    print('d_ihs3 vs cover distance: both non-null', len(both), '| max abs diff', round(float((both.iloc[:, 0] - both.iloc[:, 1]).abs().max()), 4),
          '| <=1 agree', int(((both.iloc[:, 0] <= 1) == (both.iloc[:, 1] <= 1)).sum()))
for thr in (0.5, 1, 2, 5):
    st = m[(m.dist_to_IHS3location <= thr) & m[lat].notna()]
    g = st.groupby('ea_id')[[lat, lon]].nunique()
    print(f'stayers <= {thr} km: {len(st)} hh, EAs covered {st.ea_id.nunique()} of {m.ea_id.nunique()}, EAs with >1 distinct coord {int(((g[lat] > 1) | (g[lon] > 1)).sum())}')
st = m[(m.dist_to_IHS3location <= 1) & m[lat].notna()]
print('stayers per EA: min', int(st.groupby('ea_id').size().min()), 'median', float(st.groupby('ea_id').size().median()))
if 'mover' in m:
    print('mover flag among <=1km stayers', m.loc[m.dist_to_IHS3location <= 1, 'mover'].value_counts(dropna=False).to_dict())
# stayer fix vs all-household modal / vs previously selected (first by sorted key)
fix = st.groupby('ea_id')[[lat, lon]].first()
def km(a1, o1, a2, o2):
    a1, o1, a2, o2 = map(np.radians, (a1, o1, a2, o2))
    h = np.sin((a2 - a1) / 2) ** 2 + np.cos(a1) * np.cos(a2) * np.sin((o2 - o1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(h))
mm = m.dropna(subset=[lat]).join(fix, on='ea_id', rsuffix='_fix')
mm['km'] = km(mm[lat], mm[lon], mm[lat + '_fix'], mm[lon + '_fix'])
print('households: distance from their EA stayer fix: median km', round(float(mm.km.median()), 2), '| >5km', int((mm.km > 5).sum()), 'of', int(mm.km.notna().sum()))
first = m.dropna(subset=[lat]).sort_values(key).groupby('ea_id')[[lat, lon]].first().join(fix, rsuffix='_fix')
first['km'] = km(first[lat], first[lon], first[lat + '_fix'], first[lon + '_fix'])
print('round-1 style selection (first non-null by key) vs stayer fix: EAs differing', int((first.km > 1e-6).sum()), '| >5km', int((first.km > 5).sum()), '| max km', round(float(first.km.max()), 1))
if cs_geo_p:
    # cs_geo_p: round-1 tip wave frame parquet (has v, i, Latitude, Longitude)
    shared = sys.argv[7]
    wf = pd.read_parquet(cs_geo_p).reset_index()
    cs = wf[(wf.v.astype(str) == shared) & wf.i.astype(str).str.startswith('cs-')]
    csf = cs[['Latitude', 'Longitude']].dropna().drop_duplicates()
    sst = st[st.ea_id.astype(str) == shared]
    print('shared EA: CS households', len(cs), 'CS distinct fixes', len(csf), '| panel stayers', len(sst),
          'panel stayer distinct fixes', int(sst[[lat, lon]].drop_duplicates().shape[0]))
    print('   km between CS fix and panel stayer fix', round(float(km(csf.iloc[0, 0], csf.iloc[0, 1], sst.iloc[0][lat], sst.iloc[0][lon])), 2))
