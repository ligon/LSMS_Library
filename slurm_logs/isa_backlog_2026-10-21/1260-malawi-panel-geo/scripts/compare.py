"""Compare base vs tip builds. COUNTS ONLY -- no ids or coordinates."""
import sys, pickle
import pandas as pd
B, T = sys.argv[1], sys.argv[2]
P = lambda d, f: pd.read_parquet(f'{d}/{f}')
cfb, cft = P(B, 'cluster_features.parquet'), P(T, 'cluster_features.parquet')
print('cluster_features shape base', cfb.shape, 'tip', cft.shape, '| index equal', cfb.index.equals(cft.index),
      '| columns', list(cft.columns), '| dtypes equal', (cfb.dtypes == cft.dtypes).all())
# panel EAs = v present in Panel half; derive from wave frames (i not starting cs-)
for w in ['2016-17', '2019-20']:
    wb, wt = P(B, f'wave_{w}.parquet').reset_index(), P(T, f'wave_{w}.parquet').reset_index()
    print(f'\n== {w}: wave rows base {len(wb)} tip {len(wt)} | cols tip {list(wt.columns)}')
    assert not any(c.endswith(('_x', '_y')) for c in wt.columns)
    _s = wt['i'].astype(str); is_cs = _s.str.startswith('cs-') | _s.str.fullmatch(r'\d+')
    panel_v = set(wt.loc[~is_cs, 'v'].astype(str)); cs_v = set(wt.loc[is_cs, 'v'].astype(str))
    print(f'  households: CS {int(is_cs.sum())}, panel {int((~is_cs).sum())} | EAs: CS {len(cs_v)}, panel {len(panel_v)}, shared {len(panel_v & cs_v)}')
    print(f'  household rows with Latitude: base {int(wb.Latitude.notna().sum())} tip {int(wt.Latitude.notna().sum())}; '
          f'panel households with Latitude tip {int(wt.loc[~is_cs, "Latitude"].notna().sum())} of {int((~is_cs).sum())}')
    cb = cfb.xs(w, level='t'); ct = cft.xs(w, level='t')
    cb.index = cb.index.astype(str); ct.index = ct.index.astype(str)
    pv = sorted(panel_v - cs_v); sv = sorted(panel_v & cs_v); cv = sorted(cs_v - panel_v)
    for name, vs in [('panel-only EAs', pv), ('shared EAs', sv), ('CS-only EAs', cv)]:
        print(f'  {name} ({len(vs)}): with GPS base {int(cb.loc[vs, "Latitude"].notna().sum())} -> tip {int(ct.loc[vs, "Latitude"].notna().sum())}')
    print(f'  all EAs ({len(ct)}): missing GPS base {int(cb.Latitude.isna().sum())} -> tip {int(ct.Latitude.isna().sum())}')
    # CS clusters unchanged?
    eq = cb.loc[cv].equals(ct.loc[cv])
    print(f'  CS-only clusters identical (all 5 cols): {eq}')
    for col in ['Region', 'District', 'Rural']:
        d = (cb[col].astype(str) != ct[col].astype(str)).sum()
        print(f'    {col}: clusters changed {int(d)}')
    if sv:
        print(f'  shared EA row identical: {cb.loc[sv].equals(ct.loc[sv])}')
    # contested cells per column (household-grain, before collapse)
    for lab, wf in [('base', wb), ('tip', wt)]:
        g = wf.groupby('v')[['Region', 'District', 'Rural', 'Latitude', 'Longitude']].nunique()
        print(f'  contested EAs ({lab}):', {c: int((g[c] > 1).sum()) for c in g.columns})
# other waves unchanged
other = [w for w in cft.index.get_level_values('t').unique() if w not in ('2016-17', '2019-20')]
print('\nother waves identical:', {w: cfb.xs(w, level='t').equals(cft.xs(w, level='t')) for w in other})
for f in ['sample.parquet', 'housing.parquet']:
    a, b = P(B, f), P(T, f)
    print(f, 'shape', a.shape, b.shape, '| DataFrame.equals', a.equals(b))
gb = pickle.load(open(f'{B}/grain_reports.pkl', 'rb')); gt = pickle.load(open(f'{T}/grain_reports.pkl', 'rb'))
def summ(rs):
    out = {}
    for r in rs:
        k = (r.get('table'), r.get('wave'), r.get('site'))
        out[k] = {x: r.get(x) for x in ('rows', 'dropped', 'destroyed', 'conflicting_groups', 'nan_key_rows')}
    return out
sb, st = summ(gb), summ(gt)
print('\ngrain reports (table, wave, site): base -> tip')
for k in sorted(set(sb) | set(st), key=str):
    print(' ', k, sb.get(k), '->', st.get(k))
# which clusters changed any served value, by half
for w in ['2016-17', '2019-20']:
    wt = P(T, f'wave_{w}.parquet').reset_index(); _s = wt['i'].astype(str)
    is_cs = _s.str.startswith('cs-') | _s.str.fullmatch(r'\d+')
    pv, cv = set(wt.loc[~is_cs, 'v'].astype(str)), set(wt.loc[is_cs, 'v'].astype(str))
    cb = cfb.xs(w, level='t'); ct = cft.xs(w, level='t'); cb.index = cb.index.astype(str); ct.index = ct.index.astype(str)
    for col in ['Region', 'District', 'Rural']:
        ch = set(cb.index[(cb[col].astype(str) != ct[col].astype(str)).values])
        print(f'{w} {col} changed: panel-only {len(ch & (pv - cv))}, shared {len(ch & pv & cv)}, CS-only {len(ch & (cv - pv))}')
    gps_changed = set(cb.index[((cb.Latitude != ct.Latitude) & cb.Latitude.notna()).values])
    print(f'{w} clusters whose existing GPS changed: {len(gps_changed)} (CS-only {len(gps_changed & (cv - pv))}, shared {len(gps_changed & pv & cv)})')
