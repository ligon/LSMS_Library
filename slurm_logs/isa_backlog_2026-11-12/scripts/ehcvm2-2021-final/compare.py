"""Compare base and candidate cold builds of one country.

usage: python compare.py COUNTRY BASE_DIR CAND_DIR OUT_JSON [NEW_WAVE]
Non-derived tables: every wave but NEW_WAVE must be frame-equal to base on base's
columns (rows sorted by the declared index); new columns are reported separately
with their NA share on the older waves; dtype changes separately from value changes.
Derived food tables: kg movement on older waves.  NEW_WAVE: rows, duplicates, v,
weights, roster sanity, warnings.
"""
import json, os, sys
import numpy as np
import pandas as pd

country, bdir, cdir, out = sys.argv[1:5]
NEW = sys.argv[5] if len(sys.argv) > 5 else '2021-22'
bm = json.load(open(os.path.join(bdir, 'meta.json')))
cm = json.load(open(os.path.join(cdir, 'meta.json')))
DERIVED = {'food_expenditures', 'food_prices', 'food_quantities', 'household_characteristics'}
res = {'country': country, 'new_wave': NEW, 'tables': {}, 'base_waves': bm['waves'], 'cand_waves': cm['waves']}

def load(d, t):
    p = os.path.join(d, f'{t}.parquet')
    return pd.read_parquet(p) if os.path.exists(p) else None

def canon(df, keys):
    df = df.copy()
    for col in df.columns:
        if str(df[col].dtype) in ('object', 'string', 'str', 'string[python]', 'string[pyarrow]'):
            df[col] = df[col].astype('string')
    by = keys or list(df.columns)
    return df.sort_values(by, kind='mergesort', na_position='last').reset_index(drop=True)

for t in sorted(set(bm['tables']) | set(cm['tables'])):
    r = {}
    brec, crec = bm['tables'].get(t, {}), cm['tables'].get(t, {})
    r['base_error'] = brec.get('error'); r['cand_error'] = crec.get('error')
    r['base_rows'] = brec.get('rows'); r['cand_rows'] = crec.get('rows')
    r['cand_warnings'] = crec.get('warnings'); r['base_warnings'] = brec.get('warnings')
    r['cand_dup_index'] = crec.get('dup_index')
    B, C = load(bdir, t), load(cdir, t)
    if C is not None and 't' in C.columns:
        r['cand_rows_by_wave'] = {str(k): int(v) for k, v in C['t'].astype(str).value_counts().items()}
    if B is not None and 't' in B.columns:
        r['base_rows_by_wave'] = {str(k): int(v) for k, v in B['t'].astype(str).value_counts().items()}
    if B is not None and C is not None and 't' in C.columns and 't' in B.columns:
        keys = [k for k in (brec.get('index') or []) if k in B.columns and k in C.columns]
        Cold = C[C['t'].astype(str) != NEW]
        Bold = B[B['t'].astype(str) != NEW]
        r['base_has_new_wave_rows'] = int((B['t'].astype(str) == NEW).sum())
        newcols = [c for c in C.columns if c not in B.columns]
        gone = [c for c in B.columns if c not in C.columns]
        r['new_columns'] = {c: {'na_share_older': float(Cold[c].isna().mean()) if len(Cold) else None,
                                'dtype': str(C[c].dtype)} for c in newcols}
        r['dropped_columns'] = gone
        shared = [c for c in B.columns if c in C.columns]
        b2, c2 = canon(Bold[shared], keys), canon(Cold[shared], keys)
        r['older_rows'] = [len(b2), len(c2)]
        r['dtype_changes'] = {c: [str(Bold[c].dtype), str(Cold[c].dtype)] for c in shared
                              if str(Bold[c].dtype) != str(Cold[c].dtype)}
        if 'i' in shared:
            bi = {w: set(g['i'].astype(str)) for w, g in Bold.groupby(Bold['t'].astype(str))}
            ci = {w: set(g['i'].astype(str)) for w, g in Cold.groupby(Cold['t'].astype(str))}
            r['i_sets_equal'] = {w: bi.get(w) == ci.get(w) for w in sorted(set(bi) | set(ci))}
        if t in DERIVED:
            r['kind'] = 'derived'
        if len(b2) == len(c2):
            diffcols = {}
            for col in shared:
                a, b = b2[col], c2[col]
                if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
                    av = pd.to_numeric(a, errors='coerce').astype('float64').to_numpy()
                    bv = pd.to_numeric(b, errors='coerce').astype('float64').to_numpy()
                    neq = ~(np.isclose(av, bv, rtol=0, atol=0, equal_nan=True))
                else:
                    av, bv = a.astype('string'), b.astype('string')
                    neq = ~((av == bv).fillna(False) | (av.isna() & bv.isna())).to_numpy()
                if neq.any():
                    diffcols[col] = int(neq.sum())
            r['older_value_diffs'] = diffcols
            r['older_frame_equal_values'] = not diffcols
        else:
            r['older_frame_equal_values'] = False
            r['older_value_diffs'] = 'row count differs'
        # kg movement on older waves
        if t in ('food_quantities', 'food_prices') and keys:
            m = Bold.merge(Cold, on=keys, how='outer', suffixes=('_b', '_c'), indicator=True)
            col = [c for c in shared if c not in keys][0]
            both = m[m['_merge'] == 'both']
            bv = pd.to_numeric(both[f'{col}_b'], errors='coerce'); cv = pd.to_numeric(both[f'{col}_c'], errors='coerce')
            moved = ~np.isclose(bv, cv, equal_nan=True)
            ratio = (cv / bv)[moved & (bv != 0)]
            mv = {'column': col, 'rows_both': int(len(both)), 'only_base': int((m['_merge'] == 'left_only').sum()),
                  'only_cand': int((m['_merge'] == 'right_only').sum()), 'rows_moved': int(moved.sum()),
                  'median_ratio_moved': float(ratio.median()) if len(ratio) else None,
                  'p10': float(ratio.quantile(.1)) if len(ratio) else None,
                  'p90': float(ratio.quantile(.9)) if len(ratio) else None}
            if t == 'food_quantities' and 'u' in keys:
                for side, F in (('base', Bold), ('cand', Cold)):
                    q = pd.to_numeric(F[col], errors='coerce')
                    mv[f'kg_total_{side}_by_wave'] = {str(w): float(q[(F['u'] == 'kg') & (F['t'].astype(str) == w)].sum())
                                                     for w in sorted(F['t'].astype(str).unique())}
            if t == 'food_prices':
                for side, F in (('base', Bold), ('cand', Cold)):
                    q = pd.to_numeric(F[col], errors='coerce')
                    mv[f'price_sum_{side}'] = float(q.sum()); mv[f'price_median_{side}'] = float(q.median())
            r['kg_movement'] = mv
    if C is not None and 't' in C.columns:
        N = C[C['t'].astype(str) == NEW]
        if len(N):
            keys = [k for k in (crec.get('index') or []) if k in N.columns]
            n = {'rows': int(len(N)), 'dup_keys': int(N.duplicated(keys).sum()) if keys else None,
                 'v_present': ('v' in N.columns), 'v_na': int(N['v'].isna().sum()) if 'v' in N.columns else None,
                 'i_distinct': int(N['i'].nunique()) if 'i' in N.columns else None,
                 'columns_all_na': [c for c in N.columns if N[c].isna().all()]}
            if t == 'sample':
                for col in ('weight', 'panel_weight'):
                    if col in N.columns:
                        w = pd.to_numeric(N[col], errors='coerce')
                        n[col] = {'na': int(w.isna().sum()), 'nonpos': int((w <= 0).sum()), 'mean': float(w.mean()),
                                  'distinct': int(w.nunique())}
                if 'strata' in N.columns:
                    n['strata_distinct'] = int(N['strata'].nunique())
                    n['strata_values_head'] = sorted(map(str, N['strata'].dropna().unique()))[:40]
                    O = C[C['t'].astype(str) != NEW]
                    n['strata_shared_with_older'] = sorted(set(map(str, N['strata'].dropna())) & set(map(str, O['strata'].dropna())))
                    n['older_strata_distinct'] = {str(w): int(g['strata'].nunique()) for w, g in O.groupby(O['t'].astype(str))}
                if 'Rural' in N.columns:
                    n['Rural'] = {str(k): int(v) for k, v in N['Rural'].value_counts(dropna=False).items()}
            if t == 'household_roster':
                for col in ('Sex', 'Generation', 'Distance', 'Affinity', 'MonthsSpent'):
                    if col in N.columns:
                        n[col] = {str(k): int(v) for k, v in N[col].value_counts(dropna=False).head(12).items()}
                if 'Age' in N.columns:
                    a = pd.to_numeric(N['Age'], errors='coerce')
                    n['Age'] = {'na': int(a.isna().sum()), 'min': float(a.min()), 'max': float(a.max()),
                                'median': float(a.median()), 'over_100': int((a > 100).sum())}
            r['new_wave'] = n
    res['tables'][t] = r
res['per_wave_base'] = bm.get('per_wave', {}); res['per_wave_cand'] = cm.get('per_wave', {})
json.dump(res, open(out, 'w'), indent=1, default=str)
print('wrote', out)
