"""Compare base vs cand frames (aggregates only).

usage: python compare.py <framesdir> <resultsdir>
"""
import json
import sys
from pathlib import Path

import pandas as pd

frames, results = Path(sys.argv[1]), Path(sys.argv[2])
PP_Q = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']
out = {}


def load(side, name):
    p = frames / f'{side}.{name}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def frame_equal(a, b):
    try:
        pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())
        return {'equal': True, 'rows': int(len(a))}
    except AssertionError as e:
        rec = {'equal': False, 'msg': str(e)[:800], 'rows_base': int(len(a)), 'rows_cand': int(len(b))}
        try:
            ia, ib = set(a.index), set(b.index)
            rec['base_only'] = len(ia - ib)
            rec['cand_only'] = len(ib - ia)
            common = sorted(ia & ib) if len(ia & ib) < 2_000_000 else None
            if common is not None and a.index.is_unique and b.index.is_unique:
                aa, bb = a.loc[common], b.loc[common, a.columns.intersection(b.columns)]
                aa = aa[bb.columns]
                ne = ~((aa == bb) | (aa.isna() & bb.isna()))
                rec['value_diff_cells_by_col'] = {str(k): int(v) for k, v in ne.sum().items() if v}
            rec['dtypes_base'] = {str(k): str(v) for k, v in a.dtypes.items()}
            rec['dtypes_cand'] = {str(k): str(v) for k, v in b.dtypes.items()}
        except Exception as e2:  # noqa: BLE001
            rec['diag_error'] = str(e2)[:300]
        return rec


for name in ('sample', 'household_roster', 'food_acquired', 'plot_features', 'crop_production',
             'community_prices_aggregate'):
    a, b = load('base', name), load('cand', name)
    if a is None or b is None:
        out[name] = {'missing': [s for s, x in (('base', a), ('cand', b)) if x is None]}
        continue
    if name == 'community_prices_aggregate':
        b = b[~b.index.get_level_values('t').isin(PP_Q)]
        b = b.drop(columns=[c for c in b.columns if c not in a.columns])
    out[name] = frame_equal(a, b)


def ph_diff(a, b, label):
    """PH rows: base vs cand, keyed, with relabel detection on index-minus-u."""
    b = b[~b.index.get_level_values('t').isin(PP_Q)]
    rec = {'cand_extra_columns': [c for c in b.columns if c not in a.columns],
           'cand_size_nonnull_on_ph': int(b['Size'].notna().sum()) if 'Size' in b else None}
    b = b[[c for c in a.columns if c in b.columns]]
    rec['dtypes_base'] = {str(k): str(v) for k, v in a.dtypes.items()}
    rec['dtypes_cand'] = {str(k): str(v) for k, v in b.dtypes.items()}
    ra, rb = a.reset_index(), b.reset_index()
    idx = list(a.index.names)
    m = ra.merge(rb, on=idx, how='outer', indicator=True, suffixes=('_b', '_c'))
    rec['rows_base'], rec['rows_cand'] = int(len(ra)), int(len(rb))
    rec['keyed'] = {str(k): int(v) for k, v in m['_merge'].value_counts().items()}
    both = m[m['_merge'].eq('both')]
    vd = {}
    for col in a.columns:
        x, y = both[f'{col}_b'], both[f'{col}_c']
        ne = ~((x == y) | (x.isna() & y.isna()))
        vd[col] = int(ne.sum())
    rec['common_value_diffs'] = vd
    lo = m[m['_merge'].eq('left_only')]
    ro = m[m['_merge'].eq('right_only')]
    rec['base_only_by_t_u'] = {f'{t}|{u}': int(n) for (t, u), n in lo.groupby(['t', 'u']).size().items()}
    rec['cand_only_by_t_u'] = {f'{t}|{u}': int(n) for (t, u), n in ro.groupby(['t', 'u']).size().items()}
    key = [x for x in idx if x != 'u']
    if 'country' in key:
        pass
    pair = lo.merge(ro, on=key, suffixes=('_base', '_cand'))
    rec['relabel_pairs'] = {f'{t}|{ub}->{uc}': int(n) for (t, ub, uc), n in
                            pair.groupby(['t', 'u_base', 'u_cand']).size().items()}
    if len(pair):
        pb, pc = pair['Price_b_base'], pair['Price_c_cand']
        rec['relabel_price_equal'] = int(((pb == pc) | (pb.isna() & pc.isna())).sum())
    rec['unpaired_base_only'] = int(len(lo) - lo.merge(ro[key], on=key).shape[0])
    rec['unpaired_cand_only'] = int(len(ro) - ro.merge(lo[key], on=key).shape[0])
    return rec


a, b = load('base', 'community_prices'), load('cand', 'community_prices')
if a is not None and b is not None:
    out['community_prices_ph'] = ph_diff(a, b, 'cp')
    out['community_prices_pp_rows_cand'] = int(b.index.get_level_values('t').isin(PP_Q).sum())
    out['community_prices_pp_rows_base'] = int(a.index.get_level_values('t').isin(PP_Q).sum())

a, b = load('base', 'feature_community_prices'), load('cand', 'feature_community_prices')
if a is not None and b is not None:
    rec = {}
    for ctry in ('Tanzania', 'Uganda'):
        xa = a.xs(ctry, level='country', drop_level=False) if ctry in a.index.get_level_values('country') else None
        xb = b.xs(ctry, level='country', drop_level=False) if ctry in b.index.get_level_values('country') else None
        if xa is None or xb is None:
            rec[ctry] = {'present_base': xa is not None, 'present_cand': xb is not None}
            continue
        extra = [c for c in xb.columns if c not in xa.columns]
        rec[ctry] = frame_equal(xa, xb[[c for c in xa.columns]])
        rec[ctry]['cand_extra_columns'] = extra
        if 'Size' in xb:
            rec[ctry]['cand_size_nonnull'] = int(xb['Size'].notna().sum())
    na = a.xs('Nigeria', level='country') if 'Nigeria' in a.index.get_level_values('country') else None
    nb = b.xs('Nigeria', level='country') if 'Nigeria' in b.index.get_level_values('country') else None
    rec['Nigeria_present'] = {'base': na is not None, 'cand': nb is not None}
    if na is not None and nb is not None:
        rec['Nigeria_ph'] = ph_diff(na, nb, 'feature')
        rec['Nigeria_pp_rows_cand'] = int(nb.index.get_level_values('t').isin(PP_Q).sum())
    rec['index_names_base'] = list(a.index.names)
    rec['index_names_cand'] = list(b.index.names)
    rec['columns_cand'] = list(b.columns)
    out['feature'] = rec

for side in ('base', 'cand'):
    p = results / f'{side}.build.json'
    if p.exists():
        d = json.loads(p.read_text())
        out.setdefault('hashes', {})[side] = (d.get('hashes') or {}).get('result')
if 'hashes' in out and all(out['hashes'].get(s) for s in ('base', 'cand')):
    hb, hc = out['hashes']['base'], out['hashes']['cand']
    moved = {}
    for ctry in hb:
        if ctry == 'btf':
            moved['btf'] = {str(k): hb['btf'][k] != hc['btf'].get(k) for k in hb['btf']}
            continue
        tabs = set(hb[ctry]) | set(hc.get(ctry, {}))
        moved[ctry] = {'tables': len(tabs),
                       'moved': sorted(t for t in tabs if hb[ctry].get(t) != hc.get(ctry, {}).get(t))}
    out['hash_moved'] = moved
    del out['hashes']

(results / 'compare.json').write_text(json.dumps(out, indent=1, default=str))
print(json.dumps(out, indent=1, default=str)[:20000])
