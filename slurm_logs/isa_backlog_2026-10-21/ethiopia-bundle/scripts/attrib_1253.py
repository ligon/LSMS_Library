"""Attribute every #1253 cell change (wave parquets, base -> branch) to sub-fix (a), (b) or (c)
by the raw §11 lines of the row's (holder, crop).  usage: attrib_1253.py <checkout> <base_dd> <new_dd> <out.json>"""
import os, sys, json, warnings
checkout, bdd, ndd, out = sys.argv[1:5]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, format_id
R = f'{checkout}/lsms_library/countries/Ethiopia'
sys.path.insert(0, f'{R}/_'); import ethiopia as E
cm = E._eth_crop_label_map()
W = {'2011-12': ('sect11_ph_w1.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04_a', None),
     '2013-14': ('sect11_ph_w2.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', None),
     '2015-16': ('sect11_ph_w3.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', 'ph_s11q03_b'),
     '2018-19': ('sect11_ph_w4.dta', 's11q01', 's11q07', 's11q11a', 's11q12', 's11q11b'),
     '2021-22': ('sect11_ph_w5.dta', 's11q01b', 's11q07', 's11q11a', 's11q12', 's11q11b')}
res = {}
for w, (f, cc, fl, q, v, u) in W.items():
    s = get_dataframe(f'{R}/{w}/Data/{f}', convert_categoricals=False)
    L = pd.DataFrame({'holder': s['holder_id'].apply(format_id).values,
                      'code': pd.to_numeric(s[cc], errors='coerce').astype('Int64').values,
                      'flag': pd.to_numeric(s[fl], errors='coerce').astype('Int64').values,
                      'q': pd.to_numeric(s[q], errors='coerce').values, 'v': pd.to_numeric(s[v], errors='coerce').values,
                      'ublank': pd.to_numeric(s[u], errors='coerce').isna().values if u else np.zeros(len(s), bool)}).dropna(subset=['holder', 'code'])
    L['j'] = L['code'].map(lambda c: cm.get(int(c)))
    L['no'] = (L['flag'] == 2).fillna(False)
    L['nopos'] = L['no'] & ((L['q'] > 0) | (L['v'] > 0))
    gl = L.groupby(['holder', 'j'])
    G = pd.DataFrame({'ncode': gl['code'].nunique(), 'any_ublank': gl['ublank'].any(), 'any_nopos': gl['nopos'].any(),
                      'v_allblank_noNo': gl.apply(lambda d: bool((d.v.isna() & ~d.no).all())),
                      'q_allblank_noNo': gl.apply(lambda d: bool((d.q.isna() & ~d.no).all()))})
    b = pd.read_parquet(f'{bdd}/Ethiopia/{w}/_/crop_production.parquet').sort_index()
    n = pd.read_parquet(f'{ndd}/Ethiopia/{w}/_/crop_production.parquet').sort_index()
    assert b.index.equals(n.index)
    rr = {}
    for col in ('Value_sold', 'Quantity_sold'):
        x = b[col].astype('float64').to_numpy(); y = n[col].astype('float64').to_numpy()
        ch = ~((x == y) | (np.isnan(x) & np.isnan(y)))
        idx = b.index[ch].to_frame(index=False)
        idx['holder'] = idx['plot_id'].str.split('_').str[0]
        m = idx.merge(G.reset_index(), on=['holder', 'j'], how='left')
        def why(r):
            if r['ncode'] != 1: return 'unattributable (folded label)'
            if r['any_nopos']: return '(b)'
            if col == 'Quantity_sold' and u and r['any_ublank']: return '(c)'
            if (col == 'Value_sold' and r['v_allblank_noNo']) or (col == 'Quantity_sold' and r['q_allblank_noNo']): return '(a)'
            return 'other'
        m['why'] = m.apply(why, axis=1) if len(m) else []
        trans = pd.Series([f'{a}->{c}' for a, c in zip(np.where(np.isnan(x[ch]), 'NaN', x[ch].astype(str)), np.where(np.isnan(y[ch]), 'NaN', y[ch].astype(str)))])
        rr[col] = {k: int(v) for k, v in pd.Series(m['why'].values + ' ' + trans.values).value_counts().items()} if len(m) else {}
    rr['wave_parquet_rows'] = [len(b), len(n)]
    res[w] = rr
json.dump(res, open(out, 'w'), indent=1); print(json.dumps(res, indent=1))
