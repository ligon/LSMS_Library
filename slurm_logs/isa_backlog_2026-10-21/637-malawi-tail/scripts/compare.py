"""Compare base vs branch cold builds; aggregates only (JSON)."""
import json, pickle, sys
from collections import Counter
import numpy as np, pandas as pd
O = '/local/job40390255/data-private/637-mw/out'
res = {}

def load(lbl, name):
    return pd.read_parquet(f'{O}/{lbl}/{name}.parquet')

def num(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')

def cell_diff(a, b, col):
    x, y = a[col], b[col]
    if pd.api.types.is_numeric_dtype(x) or pd.api.types.is_numeric_dtype(y):
        xa, ya = num(x), num(y)
        same = (xa.isna() & ya.isna()) | np.isclose(xa.fillna(0), ya.fillna(0), rtol=0, atol=1e-9) & (xa.isna() == ya.isna())
    else:
        xs, ys = x.astype(object), y.astype(object)
        same = (pd.isna(xs) & pd.isna(ys)) | (xs == ys).fillna(False)
    return ~same

for tab in ['crop_production', 'plot_inputs', 'plot_features']:
    A, B = load('base', tab), load('branch', tab)
    out = {'rows': [len(A), len(B)], 'index_equal': bool(A.index.equals(B.index)),
           'columns_equal': list(A.columns) == list(B.columns),
           'dtypes_equal': bool((A.dtypes == B.dtypes).all())}
    if out['index_equal']:
        try:
            pd.testing.assert_frame_equal(A, B)
            out['frame_equal'] = True
        except AssertionError:
            out['frame_equal'] = False
        perwave = {}
        for w in sorted(A.index.get_level_values('t').unique()):
            m = A.index.get_level_values('t') == w
            a, b = A[m], B[m]
            d = {}
            for c in A.columns:
                nd = int(cell_diff(a, b, c).sum())
                if nd:
                    d[c] = nd
            wd = {'rows': int(m.sum()), 'cells_changed': d}
            if 'Quantity' in A.columns:
                qa, qb = num(a['Quantity']), num(b['Quantity'])
                wd['Quantity_sum'] = [float(qa.sum()), float(qb.sum())]
                wd['Quantity_na'] = [int(qa.isna().sum()), int(qb.isna().sum())]
                wd['Quantity_zero'] = [int((qa == 0).sum()), int((qb == 0).sum())]
                wd['zero_to_na'] = int(((qa == 0) & qb.isna()).sum())
                wd['na_to_value'] = int((qa.isna() & qb.notna()).sum())
                ch = cell_diff(a, b, 'Quantity') & qa.notna() & qb.notna()
                wd['value_changed_rows'] = int(ch.sum())
                wd['value_changed_delta'] = float((qb[ch] - qa[ch]).sum())
            if tab == 'plot_inputs':
                inp = a.index.get_level_values('input')
                s = inp == 'Seed'
                wd['seed_rows'] = int(s.sum())
                wd['seed_Quantity_sum'] = [float(num(a.loc[s, 'Quantity']).sum()), float(num(b.loc[s, 'Quantity']).sum())]
                wd['seed_Quantity_changed_rows'] = int(cell_diff(a[s], b[s], 'Quantity').sum())
                wd['Quantity_purchased_sum'] = [float(num(a['Quantity_purchased']).sum()), float(num(b['Quantity_purchased']).sum())]
                wd['Quantity_purchased_changed_rows'] = int(cell_diff(a, b, 'Quantity_purchased').sum())
                wd['nonseed_frame_equal'] = bool(a[~s].equals(b[~s]))
                byinp = {}
                for c in A.columns:
                    msk = cell_diff(a, b, c)
                    if msk.any():
                        byinp[c] = dict(Counter(inp[msk.values]))
                wd['changed_by_input'] = byinp
            if tab == 'crop_production':
                for c in ['Value_sold', 'Quantity_sold']:
                    wd[c + '_sum'] = [float(num(a[c]).sum()), float(num(b[c]).sum())]
                cols_other = [c for c in A.columns if c != 'Quantity']
                wd['other_columns_equal'] = bool(a[cols_other].equals(b[cols_other]))
            perwave[w] = wd
        out['per_wave'] = perwave
    res[tab] = out

down = {}
for k in ['seed_kg', 'nitrogen_kg', 'harvest_kg', 'harvest_kg_shipped', 'yield_kg']:
    A, B = load('base', 'down_' + k), load('branch', 'down_' + k)
    col = A.columns[0]
    dd = {'rows': [len(A), len(B)], 'index_equal': bool(A.index.equals(B.index))}
    try:
        pd.testing.assert_frame_equal(A, B)
        dd['frame_equal'] = True
    except AssertionError:
        dd['frame_equal'] = False
    pw = {}
    for w in sorted(set(A.index.get_level_values('t')) | set(B.index.get_level_values('t'))):
        a = A[A.index.get_level_values('t') == w][col]
        b = B[B.index.get_level_values('t') == w][col]
        j = pd.concat([a.rename('a'), b.rename('b')], axis=1)
        changed = ~((j.a.isna() & j.b.isna()) | np.isclose(num(j.a).fillna(0), num(j.b).fillna(0), rtol=1e-12, atol=1e-9) & (j.a.isna() == j.b.isna()))
        pw[w] = {'rows': [len(a), len(b)], 'sum': [float(num(a).sum()), float(num(b).sum())],
                 'only_base': int(j.b.isna().sum() - b.reindex(j.index).isna().sum() + (j.b.isna() & j.a.notna()).sum()) if False else int(len(j.index.difference(b.index))),
                 'only_branch': int(len(j.index.difference(a.index))), 'changed_rows': int(changed.sum())}
    dd['per_wave'] = pw
    for side in ('base', 'branch'):
        at = pickle.load(open(f'{O}/{side}/down_{k}.attrs.pkl', 'rb'))
        dd[f'attrs_keys_{side}'] = sorted(at)
        for key in ('kg_factor_sources', 'nutrient_coverage'):
            if key in at:
                dd[f'{key}_{side}'] = at[key] if not isinstance(at[key], pd.DataFrame) else at[key].to_dict()
    down[k] = dd
res['downstream'] = down

rep = {}
for side in ('base', 'branch'):
    r = pickle.load(open(f'{O}/{side}/reports.pkl', 'rb'))
    w = json.load(open(f'{O}/{side}/warnings.json'))
    summ = {'warnings_by_category': dict(Counter((x['table'], x['category']) for x in w).most_common())}
    summ['warnings_by_category'] = {f'{a}|{b}': n for (a, b), n in Counter((x['table'], x['category']) for x in w).items()}
    q = r['quantity']
    summ['siteQ'] = [{'table': x.get('table'), 'column': x.get('column'), 'n_offenders': x.get('n_offenders', len(x.get('offenders', [])))} for x in q]
    summ['grain'] = [{'table': x.get('table'), 'wave': x.get('wave'), 'destroyed': x.get('rows_destroyed', x.get('destroyed'))} for x in r['grain']]
    summ['null_index'] = [{'table': x.get('table'), 'levels': {k: v for k, v in (x.get('counts') or {}).items()} if isinstance(x.get('counts'), dict) else str(x)[:300]} for x in r['null_index']]
    summ['null_read'] = [{k: (v if isinstance(v, (int, float, str)) else str(v)[:200]) for k, v in x.items() if k in ('table', 'wave', 'column', 'columns', 'site', 'kind')} for x in r['null_read']]
    rep[side] = summ
res['reports'] = rep
json.dump(res, open(sys.argv[1], 'w'), indent=1, default=str)
print(json.dumps({t: {k: v for k, v in res[t].items() if k != 'per_wave'} for t in ['crop_production', 'plot_inputs', 'plot_features']}, indent=0, default=str))
