"""Raw oracle for GH #1253: recompute the §11 (holder, crop) sale aggregate from the
raw files under the OLD and NEW rules, independently of ethiopia.py's §11 code,
and check the wave parquets of a build against the NEW rule.
usage: oracle_1253.py <checkout> <datadir> <out.json>"""
import os, sys, json, warnings
checkout, datadir, out = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, format_id
R = f'{checkout}/lsms_library/countries/Ethiopia'
W = {'2011-12': ('sect11_ph_w1.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04_a', None, 'sect9_ph_w1.dta', 'crop_code', None),
     '2013-14': ('sect11_ph_w2.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', None, 'sect9_ph_w2.dta', 'crop_code', 'ph_s9q04_b'),
     '2015-16': ('sect11_ph_w3.dta', 'crop_code', 'ph_s11q01', 'ph_s11q03_a', 'ph_s11q04', 'ph_s11q03_b', 'sect9_ph_w3.dta', 'crop_code', 'ph_s9q04_b'),
     '2018-19': ('sect11_ph_w4.dta', 's11q01', 's11q07', 's11q11a', 's11q12', 's11q11b', 'sect9_ph_w4.dta', 's9q00b', 's9q05b'),
     '2021-22': ('sect11_ph_w5.dta', 's11q01b', 's11q07', 's11q11a', 's11q12', 's11q11b', 'sect9_ph_w5.dta', 's9q00b', 's9q05b')}
res = {}
for w, (f11, cc, fl, q, v, u, f9, cc9, u9) in W.items():
    s = get_dataframe(f'{R}/{w}/Data/{f11}', convert_categoricals=False)
    h = get_dataframe(f'{R}/{w}/Data/{f9}', convert_categoricals=False)
    L = pd.DataFrame({
        'holder': s['holder_id'].apply(format_id).values,
        'code': pd.to_numeric(s[cc], errors='coerce').astype('Int64').values,
        'flag': pd.to_numeric(s[fl], errors='coerce').astype('Int64').values,
        'q': pd.to_numeric(s[q], errors='coerce').values,
        'v': pd.to_numeric(s[v], errors='coerce').values,
        'unit_blank': (pd.to_numeric(s[u], errors='coerce').isna().values if u else np.zeros(len(s), bool)),
    }).dropna(subset=['holder', 'code'])
    no = (L['flag'] == 2).fillna(False).to_numpy(bool)
    pos = ((L['q'] > 0) | (L['v'] > 0)).to_numpy(bool)
    # NEW per-line reading, written independently: blank cell on a No line with nothing positive -> 0
    L['q_new'] = np.where(no & ~pos & L['q'].isna(), 0.0, L['q'])
    L['v_new'] = np.where(no & ~pos & L['v'].isna(), 0.0, L['v'])
    # OLD: No overwrites both with 0
    L['q_old'] = np.where(no, 0.0, L['q']); L['v_old'] = np.where(no, 0.0, L['v'])
    # single-plot (holder, crop) from raw §9
    plot = (h['holder_id'].apply(format_id).astype(str) + '_' + h['parcel_id'].apply(format_id).astype(str) + '_' + h['field_id'].apply(format_id).astype(str))
    H = pd.DataFrame({'holder': h['holder_id'].apply(format_id).values, 'code': pd.to_numeric(h[cc9], errors='coerce').astype('Int64').values, 'plot': plot.values}).dropna(subset=['code'])
    npl = H.groupby(['holder', 'code'])['plot'].nunique()
    single = set(npl[npl == 1].index)
    g = L.groupby(['holder', 'code'])
    G = pd.DataFrame({
        'lines': g.size(),
        'all_v_blank': g['v'].apply(lambda x: x.isna().all()),
        'v_old': g['v_old'].sum(), 'v_new': g['v_new'].sum(min_count=1),
        'any_no_pos': g.apply(lambda d: bool(((d['flag'] == 2).fillna(False) & ((d['q'] > 0) | (d['v'] > 0))).any())),
        'all_no_blankunit': g.apply(lambda d: bool(((d['flag'] == 2).fillna(False) & d['unit_blank']).all())),
    })
    G['single'] = [k in single for k in G.index]
    S = G[G.single]
    r = {
        'sect11_lines': int(len(L)), 'holder_crop_groups': int(len(G)), 'single_plot_groups': int(len(S)),
        '(a) groups whose Value_sold lines are all blank (old 0 -> new NaN), single-plot': int((S.all_v_blank & (S.v_old == 0) & S.v_new.isna()).sum()),
        '(a) same, any plot count': int((G.all_v_blank & (G.v_old == 0) & G.v_new.isna()).sum()),
        '(b) groups with a No line carrying a positive cell, single-plot': int(S.any_no_pos.sum()),
        '(b) No lines carrying a positive cell (raw)': int((no & pos).sum()),
        '(c) single-plot groups that are all-No with a blank sale unit (coded-unit waves only)': int(S.all_no_blankunit.sum()) if u else 0,
    }
    # Check the build's wave parquet against the NEW rule on single-plot groups
    wp = pd.read_parquet(f'{datadir}/Ethiopia/{w}/_/crop_production.parquet').reset_index()
    wp['holder'] = wp['plot_id'].str.split('_').str[0]
    # code is not on the parquet; check Value_sold per (holder, j) where unique
    sys.path.insert(0, f'{R}/_'); import ethiopia as E
    cm = E._eth_crop_label_map()
    S2 = S.reset_index(); S2['j'] = S2['code'].map(lambda c: cm.get(int(c)))
    # one served value per (holder, j): only compare where (holder, j) is unique among single-plot groups
    S2 = S2[~S2.duplicated(['holder', 'j'], keep=False)]
    # a (holder, j) where several crop codes fold onto one label (KALE/CABBAGE/
    # SPINACH -> 'Leafy Greens') is not identifiable from the parquet, which
    # carries j and not the code: excluded from the comparison, and counted.
    allj = pd.concat([G.reset_index()[['holder', 'code']], H[['holder', 'code']]]).drop_duplicates()
    allj['j'] = allj['code'].map(lambda c: cm.get(int(c)))
    nc = allj.groupby(['holder', 'j'])['code'].nunique()
    folded = set(nc[nc > 1].index)
    r_folded = int(pd.MultiIndex.from_frame(S2[['holder', 'j']]).isin(list(folded)).sum()) if folded else 0
    S2 = S2[~pd.MultiIndex.from_frame(S2[['holder', 'j']]).isin(list(folded))] if folded else S2
    m = wp.merge(S2[['holder', 'j', 'v_new']], on=['holder', 'j'], how='inner')
    if 'Derivation' in m.columns:
        m = m[m['Derivation'].isna()]   # §12-filled rows are not §11's
    vb = m['Value_sold'].astype('float64').to_numpy(); ve = m['v_new'].astype('float64').to_numpy()
    agree = (vb == ve) | (np.isnan(vb) & np.isnan(ve))
    r['oracle_groups_excluded_folded_label'] = r_folded
    r['oracle_rows_compared'] = int(len(m)); r['oracle_Value_sold_mismatch'] = int((~agree).sum())
    if os.environ.get('DEBUG') and (~agree).any():
        mm = m[~agree]
        print('DEBUG', w, mm[['j','u','Quantity','Quantity_sold','Value_sold','v_new']].to_string())
        k = mm[['holder','j']].drop_duplicates()
        print('DEBUG lines', L.assign(j=L['code'].map(lambda c: cm.get(int(c)))).merge(k, on=['holder','j'])[['j','code','flag','q','v']].to_string())
    # (c): an all-"No" single-plot group in a coded-unit wave has no sale unit, so
    # its served Quantity_sold must be NaN (it was 0 on Kg harvest rows).
    if u:
        Sc = S[S.all_no_blankunit].reset_index()
        Sc['j'] = Sc['code'].map(lambda c: cm.get(int(c)))
        Sc = Sc[~pd.MultiIndex.from_frame(Sc[['holder', 'j']]).isin(list(folded))] if folded else Sc
        mc = wp.merge(Sc[['holder', 'j']].drop_duplicates(), on=['holder', 'j'], how='inner')
        if 'Derivation' in mc.columns:
            mc = mc[mc['Derivation'].isna()]
        r['(c) rows of all-No blank-unit groups'] = int(len(mc))
        r['(c) of which Kg harvest rows'] = int((mc['u'] == 'Kg').sum())
        r['(c) of which Quantity_sold not NaN (must be 0)'] = int(mc['Quantity_sold'].notna().sum())
        r['(c) of which Value_sold == 0'] = int((mc['Value_sold'] == 0).sum())
    res[w] = r
json.dump(res, open(out, 'w'), indent=1)
print(json.dumps(res, indent=1))
