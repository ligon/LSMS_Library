"""development (base) -> round 2 -> round 3.  COUNTS ONLY."""
import sys, pickle
import pandas as pd
B, R2, R3, X16, C16, X19, C19, C13, G13 = sys.argv[1:10]
P = lambda d, f: pd.read_parquet(f'{d}/{f}')
norm = lambda s: s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
def stayer_fix(geo_p, cov_p, key, lat, lon):
    geo = pd.read_stata(geo_p, convert_categoricals=False); cov = pd.read_stata(cov_p, convert_categoricals=False)
    m = cov[[key, 'ea_id', 'dist_to_IHS3location']].merge(geo[[key, lat, lon]], on=key)
    st = m[(m.dist_to_IHS3location <= 1) & m[lat].notna()]
    f = st.groupby(norm(st.ea_id))[[lat, lon]].agg(['nunique', 'first'])
    assert (f[(lat, 'nunique')] == 1).all() and (f[(lon, 'nunique')] == 1).all()
    out = pd.DataFrame({'lat': f[(lat, 'first')].astype(float), 'lon': f[(lon, 'first')].astype(float)})
    return out, m.assign(v=norm(m.ea_id))
fixes = {'2016-17': stayer_fix(X16, C16, 'y3_hhid', 'lat_modified', 'lon_modified'),
         '2019-20': stayer_fix(X19, C19, 'y4_hhid', 'lat_mod', 'lon_mod')}
runs = [('dev', B), ('r2', R2), ('r3', R3)]
cf = {k: P(d, 'cluster_features.parquet') for k, d in runs}
print('cluster_features shapes', {k: v.shape for k, v in cf.items()})
rows = []
for w in ['2016-17', '2019-20']:
    fix, m = fixes[w]
    print(f'\n== {w}: panel EAs {len(fix)}')
    wf = {k: P(d, f'wave_{w}.parquet').reset_index() for k, d in runs}
    for k, x in wf.items():
        x['panel'] = x.i.astype(str).str.fullmatch(r'\d{4}-\d{3}')
    c = {k: v.xs(w, level='t') for k, v in cf.items()}
    for k in c: c[k].index = c[k].index.astype(str)
    pv = list(fix.index)
    cs_v = set(wf['r3'].loc[~wf['r3'].panel, 'v'].astype(str))
    for k, _ in runs:
        x, cc = wf[k], c[k]
        served = cc.loc[pv]
        at = ((served.Latitude.astype(float) - fix.lat).abs() < 1e-9) & ((served.Longitude.astype(float) - fix.lon).abs() < 1e-9)
        g = x.groupby('v')[['Latitude', 'Longitude']].nunique()
        multi = int(((g.Latitude > 1) | (g.Longitude > 1)).sum())
        print(f'   {k}: clusters without GPS {int(cc.Latitude.isna().sum())} of {len(cc)} | panel EAs serving the stayer fix {int(at.sum())} of {len(pv)}'
              f' | EAs with >1 coordinate {multi} | panel hh keeping a coordinate {int(x.loc[x.panel, "Latitude"].notna().sum())} of {int(x.panel.sum())}')
    # households at the EA fix (threshold-free)
    mm = m.join(fix, on='v'); mm = mm[mm.iloc[:, 3].notna()] if False else mm
    latc, lonc = m.columns[3], m.columns[4]
    on_fix = (mm[latc].astype(float) == mm.lat) & (mm[lonc].astype(float) == mm.lon)
    print(f'   source: panel hh on their EA fix {int(on_fix.sum())} (stayers {int(((mm.dist_to_IHS3location <= 1) & mm[latc].notna()).sum())},'
          f' movers {int((on_fix & (mm.dist_to_IHS3location > 1)).sum())}, unrecorded {int((on_fix & mm.dist_to_IHS3location.isna()).sum())})')
    sh = sorted(set(pv) & cs_v)
    for v in sh:
        print(f'   shared EA: served = stayer fix: dev {bool(abs(c["dev"].loc[v, "Latitude"] - fix.loc[v, "lat"]) < 1e-9)}, r2 {bool(abs(c["r2"].loc[v, "Latitude"] - fix.loc[v, "lat"]) < 1e-9)}, r3 {bool(abs(c["r3"].loc[v, "Latitude"] - fix.loc[v, "lat"]) < 1e-9)}'
              f' | Region/District/Rural same as dev: {bool((c["dev"].loc[v, ["Region", "District", "Rural"]].astype(str) == c["r3"].loc[v, ["Region", "District", "Rural"]].astype(str)).all())}')
    co = sorted(cs_v - set(pv))
    print('   CS-only clusters identical dev vs r3:', c['dev'].loc[co].equals(c['r3'].loc[co]))
    for a, b in [('dev', 'r3'), ('r2', 'r3')]:
        ch = {}
        for col in ['Region', 'District', 'Rural']:
            moved = c[a].index[(c[a][col].astype(str) != c[b][col].astype(str)).values]
            hh = int(wf['r3'].v.astype(str).isin(set(moved)).sum())
            ch[col] = f'{len(moved)} EAs ({hh} hh)'
        print(f'   served attribute changes {a} -> {b}:', ch)
    # toward stayers' mode
    st = wf['r3'][wf['r3'].panel].merge(m[['v']].assign(i=norm(m.iloc[:, 0])).drop_duplicates('i'), on='i', suffixes=('', '_m'))
for f in ['sample.parquet', 'housing.parquet']:
    print(f, 'dev vs r3 DataFrame.equals', P(B, f).equals(P(R3, f)))
print('other waves identical dev vs r3:', {w: cf['dev'].xs(w, level='t').equals(cf['r3'].xs(w, level='t')) for w in ['2004-05', '2010-11', '2013-14']})
def summ(d):
    return {r.get('wave'): (r.get('destroyed'), r.get('conflicting_groups')) for r in pickle.load(open(f'{d}/grain_reports.pkl', 'rb')) if r.get('table') == 'cluster_features'}
s = {k: summ(d) for k, d in runs}
print('\nSite 2 (destroyed, conflicting groups): dev -> r2 -> r3')
for w in sorted(s['dev'], key=str):
    print(' ', w, s['dev'].get(w), '->', s['r2'].get(w), '->', s['r3'].get(w))
# 2013-14: served vs its stayer fix, all 204 EAs and the 102 that continue
cov = pd.read_stata(C13, convert_categoricals=False); geo = pd.read_stata(G13, convert_categoricals=False)
m = cov[['y2_hhid', 'ea_id', 'dist_to_IHS3location']].merge(geo[['y2_hhid', 'LAT_DD_MOD', 'LON_DD_MOD']], on='y2_hhid')
st = m[(m.dist_to_IHS3location <= 1) & m.LAT_DD_MOD.notna()]
f13 = st.groupby(norm(st.ea_id))[['LAT_DD_MOD', 'LON_DD_MOD']].first()
c13 = cf['r3'].xs('2013-14', level='t'); c13.index = c13.index.astype(str)
same = ((c13.loc[f13.index, 'Latitude'] - f13.LAT_DD_MOD).abs() < 1e-6) & ((c13.loc[f13.index, 'Longitude'] - f13.LON_DD_MOD).abs() < 1e-6)
cont = [v for v in f13.index if v in fixes['2016-17'][0].index]
print(f'\n2013-14 (unchanged): non-stayer coordinate served in {int((~same).sum())} of {len(f13)} EAs; {int((~same.loc[cont]).sum())} of the {len(cont)} EAs that continue into 2016-17')
# cross-round: stayer fix identical across 2013-14, 2016-17, 2019-20?
a, b = fixes['2016-17'][0], fixes['2019-20'][0]
common = a.index.intersection(b.index).intersection(f13.index)
d1 = ((a.loc[common].lat - b.loc[common].lat).abs().max(), (a.loc[common].lon - b.loc[common].lon).abs().max())
d2 = ((a.loc[common].lat - f13.loc[common].LAT_DD_MOD).abs().max(), (a.loc[common].lon - f13.loc[common].LON_DD_MOD).abs().max())
print(f'stayer fix across rounds, {len(common)} EAs: max |diff| deg 2016 vs 2019 {max(d1):.2e}, 2016 vs 2013 {max(d2):.2e}')
c10 = cf['r3'].xs('2010-11', level='t'); c10.index = c10.index.astype(str)
cm = [v for v in common if v in c10.index]
d3 = max((c10.loc[cm, 'Latitude'] - a.loc[cm].lat).abs().max(), (c10.loc[cm, 'Longitude'] - a.loc[cm].lon).abs().max())
print(f'2016 stayer fix vs served 2010-11 coordinate, {len(cm)} EAs: max |diff| deg {d3:.2e}')
