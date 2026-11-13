"""Served Region/District/Rural vs the TRUE stayers' (source dist<=1) modal value, per run. COUNTS ONLY."""
import sys
import pandas as pd
cov_p = {'2016-17': sys.argv[1], '2019-20': sys.argv[2]}
key = {'2016-17': 'y3_hhid', '2019-20': 'y4_hhid'}
runs = list(zip(sys.argv[3::2], sys.argv[4::2]))
for w in ['2016-17', '2019-20']:
    cov = pd.read_stata(cov_p[w], convert_categoricals=False)
    stay_ids = set(cov.loc[cov.dist_to_IHS3location <= 1, key[w]].astype(str).str.strip())
    wf = pd.read_parquet(f'{runs[-1][1]}/wave_{w}.parquet').reset_index()
    st = wf[wf.i.astype(str).isin(stay_ids)].copy()
    for c in ['Region', 'District', 'Rural']:
        st[c] = st[c].astype(str).str.lower()
    mode = st.groupby(st.v.astype(str))[['Region', 'District', 'Rural']].agg(lambda x: x.mode().iloc[0])
    cs_v = set(wf.loc[~wf.i.astype(str).str.fullmatch(r'\d{4}-\d{3}'), 'v'].astype(str))
    po = [v for v in mode.index if v not in cs_v]
    out = []
    for lab, d in runs:
        cf = pd.read_parquet(f'{d}/cluster_features.parquet').xs(w, level='t'); cf.index = cf.index.astype(str)
        out.append(f"{lab} " + str({c: int((cf.loc[po, c].astype(str).str.lower() == mode.loc[po, c]).sum()) for c in ['Region', 'District', 'Rural']}))
    print(f'{w}: served = true stayers\' modal value, of {len(po)} panel-only EAs ->', ' | '.join(out))
