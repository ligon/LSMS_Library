"""Compare base vs head frames restricted to base's waves.

usage: compare.py FRAMES COUNTRY[,COUNTRY...] OUT.json
"""
import json, os, sys
import numpy as np
import pandas as pd

frames, countries, out = sys.argv[1:4]
result = {}


def norm(df):
    df = df.copy()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.CategoricalDtype):
            df[c] = df[c].astype('string')
    return df.sort_index()


for country in countries.split(','):
    bm = json.load(open(os.path.join(frames, 'base', country, '_meta.json')))
    hm = json.load(open(os.path.join(frames, 'head', country, '_meta.json')))
    old = [str(w) for w in bm['waves']]
    res = {'base_waves': old, 'head_waves': hm['waves'],
           'scheme_added': sorted(set(hm['scheme']) - set(bm['scheme'])),
           'scheme_removed': sorted(set(bm['scheme']) - set(hm['scheme'])), 'tables': {}}
    for name in sorted(set(bm['tables']) | set(hm['tables'])):
      try:
        b, h = bm['tables'].get(name), hm['tables'].get(name)
        r = {}
        if b is None or h is None:
            r['status'] = 'only-' + ('head' if b is None else 'base')
            res['tables'][name] = r
            continue
        r['base_ok'], r['head_ok'] = b['ok'], h['ok']
        r['base_warnings_only'] = sorted(set(b['warnings']) - set(h['warnings']))[:20]
        r['head_warnings_only'] = sorted(set(h['warnings']) - set(b['warnings']))[:40]
        if not (b['ok'] and h['ok']):
            r['status'] = 'error'
            r['base_error'] = b.get('error'); r['head_error'] = h.get('error')
            res['tables'][name] = r
            continue
        B = pd.read_parquet(os.path.join(frames, 'base', country, f'{name}.parquet'))
        H = pd.read_parquet(os.path.join(frames, 'head', country, f'{name}.parquet'))
        r['head_rows_by_wave'] = (H.index.get_level_values('t').astype(str).value_counts().to_dict()
                                  if 't' in H.index.names else None)
        if 't' in H.index.names and 't' in B.index.names:
            present = set(B.index.get_level_values('t').astype(str))
            r['base_t_present'] = sorted(present)
            H = H[H.index.get_level_values('t').astype(str).isin(present)]
        r['index_names_equal'] = list(B.index.names) == list(H.index.names)
        r['rows'] = [len(B), len(H)]
        extra = [c for c in H.columns if c not in B.columns]
        missing = [c for c in B.columns if c not in H.columns]
        r['extra_cols'] = {c: int(H[c].notna().sum()) for c in extra}
        r['missing_cols'] = missing
        common = [c for c in B.columns if c in H.columns]
        r['dtype_diffs'] = {c: [str(B[c].dtype), str(H[c].dtype)] for c in common
                            if str(B[c].dtype) != str(H[c].dtype)}
        if not r['index_names_equal']:
            r['status'] = 'index-names-differ'; r['index_names'] = [list(B.index.names), list(H.index.names)]
            res['tables'][name] = r; continue
        Bn, Hn = norm(B[common]), norm(H[common])
        try:
            pd.testing.assert_frame_equal(Bn, Hn, check_dtype=False, check_index_type=False,
                                          check_categorical=False)
            r['status'] = 'equal' if not missing else 'equal-common-cols'
        except AssertionError as e:
            r['status'] = 'DIFF'
            r['assert'] = str(e)[:800]
            bi, hi = Bn.index, Hn.index
            r['only_base_rows'] = int((~bi.isin(hi)).sum())
            r['only_head_rows'] = int((~hi.isin(bi)).sum())
            if bi.is_unique and hi.is_unique:
                both = bi.intersection(hi)
                bb, hh = Bn.loc[both], Hn.loc[both]
                cd = {}
                for c in common:
                    x, y = bb[c], hh[c]
                    try:
                        neq = ~((x == y).fillna(False) | (x.isna() & y.isna()))
                        if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
                            neq = neq & ~np.isclose(x.astype(float), y.astype(float), rtol=1e-9, equal_nan=True)
                    except Exception:
                        neq = x.astype(str) != y.astype(str)
                    n = int(neq.sum())
                    if n:
                        ex = [[str(k), str(x[k]), str(y[k])] for k in list(neq[neq].index[:3])]
                        cd[c] = {'n': n, 'examples': ex}
                r['cell_diffs'] = cd
            for c in ('Quantity', 'Expenditure', 'Price'):
                if c in common and pd.api.types.is_numeric_dtype(B[c]):
                    r.setdefault('sums', {})[c] = [float(B[c].sum()), float(H[c].sum())]
        res['tables'][name] = r
        print(country, name, r['status'], r.get('rows'), r.get('extra_cols'), flush=True)
      except Exception as exc:
        res['tables'][name] = {'status': 'COMPARE-ERROR', 'error': f'{type(exc).__name__}: {exc}'[:500]}
    result[country] = res
    json.dump(result, open(out, 'w'), indent=1, default=str)
json.dump(result, open(out, 'w'), indent=1, default=str)
print('COMPARE DONE')
