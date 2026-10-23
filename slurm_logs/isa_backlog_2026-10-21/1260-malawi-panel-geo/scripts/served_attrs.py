"""Panel-only EAs: does the served Region/District/Rural agree with the EA's stayers? COUNTS ONLY."""
import sys
import pandas as pd
B, R1, R2 = sys.argv[1:4]
for w in ['2016-17', '2019-20']:
    wf = pd.read_parquet(f'{R2}/wave_{w}.parquet').reset_index(); s = wf.i.astype(str)
    is_cs = s.str.startswith('cs-') | s.str.fullmatch(r'\d+')
    pv, cv = set(wf.loc[~is_cs, 'v'].astype(str)), set(wf.loc[is_cs, 'v'].astype(str))
    po = sorted(pv - cv)
    st = wf[~is_cs & wf.Latitude.notna()].copy()
    for c in ['Region', 'District', 'Rural']:
        st[c] = st[c].astype(str).str.lower()
    nuniq = st.groupby('v')[['Region', 'District', 'Rural']].nunique()
    print(f'== {w}: panel-only EAs whose stayers disagree among themselves:', {c: int((nuniq[c] > 1).sum()) for c in nuniq})
    mode = st.groupby('v')[['Region', 'District', 'Rural']].agg(lambda x: x.mode().iloc[0]); mode.index = mode.index.astype(str)
    for lab, d in [('base', B), ('r1', R1), ('r2', R2)]:
        cf = pd.read_parquet(f'{d}/cluster_features.parquet').xs(w, level='t'); cf.index = cf.index.astype(str)
        x = cf.loc[po]
        agree = {c: int((x[c].astype(str).str.lower() == mode.loc[po, c]).sum()) for c in ['Region', 'District', 'Rural']}
        print(f'   {lab}: served value equals the stayers\' modal value, of {len(po)}:', agree)
# the round-1 Rural flip EA
cb = pd.read_parquet(f'{B}/cluster_features.parquet').xs('2016-17', level='t')
c1 = pd.read_parquet(f'{R1}/cluster_features.parquet').xs('2016-17', level='t')
c2 = pd.read_parquet(f'{R2}/cluster_features.parquet').xs('2016-17', level='t')
flip = cb.index[(cb.Rural.astype(str) != c1.Rural.astype(str)).values]
wf = pd.read_parquet(f'{R2}/wave_2016-17.parquet').reset_index()
for v in flip:
    e = wf[wf.v == v]
    stv = e[e.Latitude.notna()]
    print('round-1 flip EA: served Rural base', cb.loc[v, 'Rural'], '| r1', c1.loc[v, 'Rural'], '| r2', c2.loc[v, 'Rural'],
          '| households', len(e), 'stayers', len(stv), '| stayers Rural', stv.Rural.str.lower().value_counts().to_dict(),
          '| all Rural', e.Rural.str.lower().value_counts().to_dict())
ch = cb.index[(cb.Rural.astype(str) != c2.Rural.astype(str)).values]
print('2016-17 Rural base -> r2 changes:', [(cb.loc[v, 'Rural'], c2.loc[v, 'Rural']) for v in ch])
