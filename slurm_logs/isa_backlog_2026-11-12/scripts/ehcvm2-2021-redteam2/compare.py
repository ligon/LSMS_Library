"""Compare prev (6538bc8b8) vs head (9b5585cf2) frames: older waves, and 2021-22 summaries.

usage: compare.py FRAMES COUNTRY[,COUNTRY...] OUT.json
Aggregates only: no row examples are written.
"""
import json, os, re, sys
import numpy as np
import pandas as pd

frames, countries, out = sys.argv[1:4]
NEW = '2021-22'
DECODE = {'659': 'Main', '660': 'Quartier', '662': 'Tête'}
PAT = re.compile(r'^(659|660|662)(?:\.0)?(?=$|\s|\()')
result = {}


def decode_label(x):
    if not isinstance(x, str):
        return x
    return PAT.sub(lambda m: DECODE[m.group(1)], x)


def norm(df):
    df = df.copy()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.CategoricalDtype):
            df[c] = df[c].astype('string')
    return df.sort_index()


def tslice(df, old):
    if 't' not in df.index.names:
        return df
    t = df.index.get_level_values('t').astype(str)
    return df[t != NEW] if old else df[t == NEW]


def frame_cmp(B, H):
    r = {}
    r['rows'] = [len(B), len(H)]
    common = [c for c in B.columns if c in H.columns]
    r['extra_cols_nonnull'] = {c: int(H[c].notna().sum()) for c in H.columns if c not in B.columns}
    r['missing_cols'] = [c for c in B.columns if c not in H.columns]
    if list(B.index.names) != list(H.index.names):
        r['status'] = 'index-names-differ'
        return r
    Bn, Hn = norm(B[common]), norm(H[common])
    try:
        pd.testing.assert_frame_equal(Bn, Hn, check_dtype=False, check_index_type=False,
                                      check_categorical=False, check_exact=False, rtol=1e-9)
        r['status'] = 'equal'
        return r
    except AssertionError as e:
        r['status'] = 'DIFF'
        r['assert_head'] = str(e).split('\n')[0][:300]
    bi, hi = Bn.index, Hn.index
    r['only_prev_rows'] = int((~bi.isin(hi)).sum())
    r['only_head_rows'] = int((~hi.isin(bi)).sum())
    if bi.is_unique and hi.is_unique:
        both = bi.intersection(hi)
        bb, hh = Bn.loc[both], Hn.loc[both]
        cd = {}
        for c in common:
            x, y = bb[c], hh[c]
            try:
                if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
                    xf, yf = x.astype(float).to_numpy(), y.astype(float).to_numpy()
                    neq = ~np.isclose(xf, yf, rtol=1e-9, atol=0, equal_nan=True)
                    if neq.any():
                        with np.errstate(divide='ignore', invalid='ignore'):
                            ratio = yf[neq] / xf[neq]
                        ratio = ratio[np.isfinite(ratio)]
                        cd[c] = {'n': int(neq.sum()), 'prev_sum': float(np.nansum(xf)),
                                 'head_sum': float(np.nansum(yf)),
                                 'ratio_q': [float(q) for q in np.quantile(ratio, [.1, .5, .9])] if len(ratio) else None,
                                 'na_flip': int((np.isnan(xf) != np.isnan(yf)).sum())}
                else:
                    neq = ~((x.astype('string') == y.astype('string')).fillna(False) | (x.isna() & y.isna()))
                    if neq.any():
                        cd[c] = {'n': int(neq.sum())}
            except Exception as exc:
                cd[c] = {'error': str(exc)[:200]}
        r['cell_diffs'] = cd
    else:
        r['dup_index'] = [int(bi.duplicated().sum()), int(hi.duplicated().sum())]
    return r


for country in countries.split(','):
    bm = json.load(open(os.path.join(frames, 'prev', country, '_meta.json')))
    hm = json.load(open(os.path.join(frames, 'head', country, '_meta.json')))
    res = {'prev_waves': bm['waves'], 'head_waves': hm['waves'],
           'scheme_added': sorted(set(hm['scheme']) - set(bm['scheme'])),
           'scheme_removed': sorted(set(bm['scheme']) - set(hm['scheme'])), 'tables': {}}
    for name in sorted(set(bm['tables']) | set(hm['tables'])):
        try:
            b, h = bm['tables'].get(name), hm['tables'].get(name)
            r = {}
            if b is None or h is None:
                res['tables'][name] = {'status': 'only-' + ('head' if b is None else 'prev')}
                continue
            r['prev_ok'], r['head_ok'] = b['ok'], h['ok']
            r['warnings_only_prev'] = sorted(set(b['warnings']) - set(h['warnings']))[:10]
            r['warnings_only_head'] = sorted(set(h['warnings']) - set(b['warnings']))[:20]
            if not (b['ok'] and h['ok']):
                r['status'] = 'error'
                r['prev_error'] = b.get('error'); r['head_error'] = h.get('error')
                res['tables'][name] = r
                continue
            B = pd.read_parquet(os.path.join(frames, 'prev', country, f'{name}.parquet'))
            H = pd.read_parquet(os.path.join(frames, 'head', country, f'{name}.parquet'))
            r['old'] = frame_cmp(tslice(B, True), tslice(H, True))
            if country == 'Togo' and 'u' in B.index.names and r['old']['status'] != 'equal':
                Bo = tslice(B, True)
                u = Bo.index.get_level_values('u')
                newu = pd.Index([decode_label(x) for x in u], name='u')
                r['old_u_rows_decoded'] = int((newu.astype(str) != u.astype(str)).sum())
                Bd = Bo.copy()
                arrays = [Bd.index.get_level_values(n) if n != 'u' else newu
                          for n in Bd.index.names]
                Bd.index = pd.MultiIndex.from_arrays(arrays, names=Bd.index.names)
                r['old_after_decode'] = frame_cmp(Bd, tslice(H, True))
            # 2021-22 summary
            Bn, Hn = tslice(B, False), tslice(H, False)
            s21 = {'rows': [len(Bn), len(Hn)]}
            for col in ('Quantity', 'Expenditure', 'Price', 'strata'):
                if col in Bn.columns and col in Hn.columns:
                    if pd.api.types.is_numeric_dtype(Bn[col]) and pd.api.types.is_numeric_dtype(Hn[col]):
                        s21[col] = {'sum': [float(Bn[col].sum()), float(Hn[col].sum())],
                                    'na': [int(Bn[col].isna().sum()), int(Hn[col].isna().sum())]}
                    else:
                        try:
                            eq = (Bn[col].astype('string').sort_index() == Hn[col].astype('string').sort_index())
                            s21[col] = {'equal_rows': int(eq.fillna(False).sum())}
                        except Exception as exc:
                            s21[col] = {'error': str(exc)[:200]}
            if 's' in Hn.index.names and 'Quantity' in Hn.columns:
                s21['by_s'] = {}
                for side, F in (('prev', Bn), ('head', Hn)):
                    g = F.groupby(level='s')
                    s21['by_s'][side] = {str(k): {'rows': int(len(v)),
                                                  'q_sum': float(v['Quantity'].sum()),
                                                  'q_na': int(v['Quantity'].isna().sum()),
                                                  'e_sum': float(v['Expenditure'].sum()) if 'Expenditure' in v else None,
                                                  'e_na': int(v['Expenditure'].isna().sum()) if 'Expenditure' in v else None}
                                         for k, v in g}
            r['new_2021'] = s21
            r['status'] = r['old']['status']
            res['tables'][name] = r
            print(country, name, 'old:', r['old']['status'], r['old'].get('rows'),
                  'decoded:', r.get('old_after_decode', {}).get('status'), 'new rows', s21['rows'], flush=True)
        except Exception as exc:
            res['tables'][name] = {'status': 'COMPARE-ERROR', 'error': f'{type(exc).__name__}: {exc}'[:500]}
            print(country, name, 'COMPARE-ERROR', exc, flush=True)
    result[country] = res
    json.dump(result, open(out, 'w'), indent=1, default=str)
json.dump(result, open(out, 'w'), indent=1, default=str)
print('COMPARE DONE')
