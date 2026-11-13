"""base (development) vs round 1 vs round 2. COUNTS ONLY."""
import sys, pickle
import pandas as pd
B, R1, R2 = sys.argv[1:4]
P = lambda d, f: pd.read_parquet(f'{d}/{f}')
cf = {k: P(d, 'cluster_features.parquet') for k, d in [('base', B), ('r1', R1), ('r2', R2)]}
print('shapes', {k: v.shape for k, v in cf.items()}, '| index equal', cf['base'].index.equals(cf['r2'].index))
for w in ['2016-17', '2019-20']:
    wf2 = P(R2, f'wave_{w}.parquet').reset_index(); s = wf2['i'].astype(str)
    is_cs = s.str.startswith('cs-') | s.str.fullmatch(r'\d+')
    pv, cv = set(wf2.loc[~is_cs, 'v'].astype(str)), set(wf2.loc[is_cs, 'v'].astype(str))
    po, sh, co = sorted(pv - cv), sorted(pv & cv), sorted(cv - pv)
    print(f'\n== {w}: wave frame columns {list(wf2.columns)} rows {len(wf2)}')
    print(f'   panel households keeping a coordinate (stayers): {int(wf2.loc[~is_cs, "Latitude"].notna().sum())} of {int((~is_cs).sum())}')
    # stayer fix per panel-only EA from r2 wave frame
    st = wf2[~is_cs & wf2.Latitude.notna()]
    nfix = st.groupby('v')[['Latitude', 'Longitude']].nunique()
    print(f'   panel EAs with >1 distinct stayer coordinate: {int(((nfix.Latitude > 1) | (nfix.Longitude > 1)).sum())} of {len(nfix)}')
    fix = st.groupby('v')[['Latitude', 'Longitude']].first(); fix.index = fix.index.astype(str)
    c = {k: v.xs(w, level='t') for k, v in cf.items()}
    for k in c: c[k].index = c[k].index.astype(str)
    for k in ('base', 'r1', 'r2'):
        x = c[k]
        miss = int(x.Latitude.isna().sum())
        po_fix = [e for e in po if e in fix.index]
        same = int(((x.loc[po_fix, 'Latitude'] == fix.loc[po_fix, 'Latitude']) & (x.loc[po_fix, 'Longitude'] == fix.loc[po_fix, 'Longitude'])).sum())
        print(f'   {k}: clusters without GPS {miss} of {len(x)} | panel-only EAs serving the stayer fix {same} of {len(po)}')
    for k in ('base', 'r1'):
        ch = {col: int((c[k][col].astype(str) != c['r2'][col].astype(str)).sum()) for col in ['Region', 'District', 'Rural']}
        chp = {col: int((c[k].loc[po, col].astype(str) != c['r2'].loc[po, col].astype(str)).sum()) for col in ['Region', 'District', 'Rural']}
        print(f'   served changes {k} -> r2 (all clusters): {ch} | of which panel-only: {chp}')
    print('   CS-only clusters identical base vs r2:', c['base'].loc[co].equals(c['r2'].loc[co]))
    if sh:
        print('   shared EA row identical base vs r2:', c['base'].loc[sh].equals(c['r2'].loc[sh]))
    for lab, d in [('base', B), ('r1', R1), ('r2', R2)]:
        wf = P(d, f'wave_{w}.parquet').reset_index()
        g = wf.groupby('v')[['Region', 'District', 'Rural', 'Latitude', 'Longitude']].nunique()
        print(f'   contested EAs ({lab}):', {col: int((g[col] > 1).sum()) for col in g.columns})
print('\nother waves identical base vs r2:', {w: cf['base'].xs(w, level='t').equals(cf['r2'].xs(w, level='t')) for w in ['2004-05', '2010-11', '2013-14']})
for f in ['sample.parquet', 'housing.parquet']:
    print(f, 'base vs r2 DataFrame.equals', P(B, f).equals(P(R2, f)))
def summ(d):
    return {(r.get('wave')): {x: r.get(x) for x in ('rows', 'dropped', 'destroyed', 'conflicting_groups', 'nan_key_rows')}
            for r in pickle.load(open(f'{d}/grain_reports.pkl', 'rb')) if r.get('table') == 'cluster_features'}
sb, s1, s2 = summ(B), summ(R1), summ(R2)
print('\nSite 2 grain reports, base -> r1 -> r2')
for k in sorted(set(sb) | set(s2), key=str):
    print(' ', k, sb.get(k), '->', s1.get(k), '->', s2.get(k))
