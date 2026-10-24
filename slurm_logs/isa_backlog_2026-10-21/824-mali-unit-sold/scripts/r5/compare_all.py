"""Base vs branch, every Mali table, from two build_all.py output dirs.
Adapted from the red-team review (mali-sales-review/scripts/compare.py, the
"all" part).  Aggregates to stdout.  usage: compare_all.py <base dir> <branch dir>"""
import sys, os, json
import numpy as np, pandas as pd
ab, ar = sys.argv[1:3]
sb = json.load(open(f'{ab}/summary.json')); sr = json.load(open(f'{ar}/summary.json'))
out = {'tables_base': len(sb), 'tables_branch': len(sr), 'same table list': sorted(sb) == sorted(sr)}
for t in sorted(set(sb) | set(sr)):
    if 'error' in sb.get(t, {}) or 'error' in sr.get(t, {}):
        out[t] = {'base': sb.get(t, {}).get('error'), 'branch': sr.get(t, {}).get('error')}; continue
    a, b = pd.read_parquet(f'{ab}/{t}.parquet'), pd.read_parquet(f'{ar}/{t}.parquet')
    key = [k for k in sb[t]['index'] if k in a.columns and k in b.columns]
    r = {'shape': [list(a.shape), list(b.shape)], 'columns equal': list(a.columns) == list(b.columns)}
    a = a.set_index(key).sort_index(); b = b.set_index(key).sort_index()
    r['index equal'] = bool(a.index.equals(b.index))
    if not r['index equal']:
        common = a.index.intersection(b.index); a, b = a.loc[common], b.loc[common]
        r['common rows'] = len(common)
    kind = None
    if t == 'crop_production':
        kind = pd.Series(a.index.get_level_values('t').astype(str) + '/' +
                         np.where(a['perennial'].fillna(False).astype(bool), 'tree', 'field'))
    diffs = {}
    for c in a.columns:
        if c not in b.columns: continue
        x, y = a[c], b[c]
        try:
            xf, yf = pd.to_numeric(x, errors='raise').astype('float64'), pd.to_numeric(y, errors='raise').astype('float64')
            eq = (xf == yf) | (xf.isna() & yf.isna())
        except Exception:
            xs, ys = x.astype('string'), y.astype('string'); eq = (xs == ys).fillna(False) | (xs.isna() & ys.isna())
        eq = eq.astype(bool).to_numpy()
        n = int((~eq).sum())
        if n:
            diffs[c] = {'n': n} | ({'by': kind[~eq].value_counts().to_dict()} if kind is not None else {})
    r['diffs'] = diffs
    try:
        wa, wb = open(f'{ab}/{t}.warnings.txt').read(), open(f'{ar}/{t}.warnings.txt').read()
        r['warnings identical'] = wa == wb
    except Exception as e:
        r['warnings'] = str(e)
    try:
        r['attrs identical'] = open(f'{ab}/{t}.attrs.json').read() == open(f'{ar}/{t}.attrs.json').read()
    except Exception:
        pass
    out[t] = r
print(json.dumps(out, indent=1, default=str))
