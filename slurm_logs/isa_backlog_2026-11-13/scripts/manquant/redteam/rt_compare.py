"""Compare base vs candidate builds from rt_build.py.  Run in the CANDIDATE env.

usage: python rt_compare.py <basedir> <canddir> <Country> <out.json>
"""
import json
import sys
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
B, K, C, OUTP = sys.argv[1:5]
R = {}


def load(d, n):
    try:
        return pd.read_pickle(f'{d}/{n}.pkl')
    except FileNotFoundError:
        return None


def norm(s):
    s = s.astype(object)
    return s.where(pd.notna(s), None)


def cell_diff(a, b):
    """Count cells differing (NA-aware, dtype-insensitive) per column, a/b aligned."""
    out = {}
    for col in sorted(set(a.columns) | set(b.columns)):
        if col not in a.columns or col not in b.columns:
            out[col] = 'only in ' + ('cand' if col in b.columns else 'base')
            continue
        x, y = norm(a[col]), norm(b[col])
        neq = ~((x.isna() & y.isna()) | (x == y).fillna(False))
        # numeric tolerance
        try:
            xf, yf = pd.to_numeric(a[col], errors='raise'), pd.to_numeric(b[col], errors='raise')
            neq = neq & ~np.isclose(xf.astype(float), yf.astype(float), equal_nan=True, rtol=0, atol=0)
        except Exception:
            pass
        n = int(neq.sum())
        if n:
            out[col] = n
    return out


def relabel_index(df, level, mapping):
    idx = df.index.to_frame(index=False)
    idx[level] = idx[level].astype(object).map(lambda v: mapping.get(v, v))
    out = df.copy()
    out.index = pd.MultiIndex.from_frame(idx)
    return out


def per_t(df, mask):
    t = pd.Series(df.index.get_level_values('t'), dtype=object)
    return {str(k): int(v) for k, v in t[np.asarray(mask)].value_counts().items()}


b, k = load(B, 'plot_inputs'), load(K, 'plot_inputs')
R['rows'] = {'base': len(b), 'cand': len(k)}
R['index_names'] = {'base': list(b.index.names), 'cand': list(k.index.names)}
ub, uk = pd.Series(b.index.get_level_values('u'), dtype=object), pd.Series(k.index.get_level_values('u'), dtype=object)
R['u_manquant'] = {'base': per_t(b, ub == 'Manquant'), 'cand': per_t(k, uk == 'Manquant')}
R['u_unknown'] = {'base': per_t(b, ub == 'Unknown'), 'cand': per_t(k, uk == 'Unknown')}
R['rows_by_t'] = {'base': per_t(b, np.ones(len(b), bool)), 'cand': per_t(k, np.ones(len(k), bool))}

crop_map = {}
if C == 'Senegal':
    crop_map = {'Maïs en grain': 'Maïs', 'Niébé/Haricots secs': 'Niébé'}
b2 = relabel_index(b, 'u', {'Manquant': 'Unknown'})
if crop_map:
    cb = pd.Series(b2.index.get_level_values('crop'), dtype=object)
    R['crop_relabelled_rows'] = {old: per_t(b2, cb == old) for old in crop_map}
    b2 = relabel_index(b2, 'crop', crop_map)
R['index_unique'] = {'base': bool(b.index.is_unique), 'cand': bool(k.index.is_unique)}
R['keys_equal_after_relabel'] = bool(set(b2.index) == set(k.index))
R['keys_only_base'] = [str(x) for x in list(set(b2.index) - set(k.index))[:10]]
R['keys_only_cand'] = [str(x) for x in list(set(k.index) - set(b2.index))[:10]]
# Align on key (both unique for Togo / Senegal; CIV 2021-22 has dupes -> positional after sort)
if b2.index.is_unique and k.index.is_unique:
    ka = k.reindex(b2.index)
    ba = b2
else:
    ba = b2.sort_index(kind='mergesort'); ka = k.sort_index(kind='mergesort')
    R['aligned_positionally'] = bool((ba.index == ka.index).all())
for wave in sorted(set(b2.index.get_level_values('t'))):
    m = np.asarray(ba.index.get_level_values('t') == wave)
    R.setdefault('cells_changed_by_t', {})[wave] = cell_diff(ba[m], ka[m])

if C == 'Togo':
    m18 = np.asarray(ba.index.get_level_values('t') == '2018')
    g = {}
    for old, new in [('Purchased', 'Received_as_gift'), ('Quantity_purchased', 'Quantity_received_as_gift')]:
        x, y = norm(ba.loc[m18, old]), norm(ka.loc[m18, new])
        g[f'base {old} == cand {new} (2018)'] = int(((x.isna() & y.isna()) | (x == y).fillna(False)).sum())
    g['n2018'] = int(m18.sum())
    pk = ka.loc[m18, 'Purchased'].astype('boolean')
    g['cand Purchased True/False/NA'] = [int((pk == True).sum()), int((pk == False).sum()), int(pk.isna().sum())]  # noqa: E712
    rg = ka.loc[m18, 'Received_as_gift'].astype('boolean')
    g['cand Received_as_gift True/False/NA'] = [int((rg == True).sum()), int((rg == False).sum()), int(rg.isna().sum())]  # noqa: E712
    ct = pd.crosstab(rg.fillna(False).astype(bool).rename('gift'), pk.fillna(False).astype(bool).rename('purch'))
    g['crosstab gift x purch'] = {f'{a}|{c}': int(ct.loc[a, c]) for a in ct.index for c in ct.columns}
    g['qty_purchased notna where Purchased True / False'] = [
        int(ka.loc[m18, 'Quantity_purchased'][pk.fillna(False).values].notna().sum()),
        int(ka.loc[m18, 'Quantity_purchased'][(pk == False).fillna(False).values].notna().sum())]  # noqa: E712
    for wave in ('2018', '2021-22'):
        mw = np.asarray(ka.index.get_level_values('t') == wave)
        g[f'u values {wave}'] = {str(a): int(v) for a, v in pd.Series(ka.index.get_level_values('u')[mw]).value_counts().items()}
        for col in ('Unit_purchased', 'Unit_received_as_gift'):
            g[f'{col} values {wave}'] = {str(a): int(v) for a, v in ka.loc[mw, col].value_counts(dropna=False).items()}
    # The two unit paths for the five shared labels and the 12-answer purchase card
    try:
        from lsms_library.ehcvm import served_unit_labels
        from lsms_library.local_tools import get_categorical_mapping, get_dataframe
        from lsms_library.paths import countries_root
        T = countries_root() / 'Togo'
        lab = get_dataframe(T / '2018' / 'Data1' / 's16b_me_tgo2018.dta', convert_categoricals=True)
        raw = sorted(set(lab['s16bq03b'].dropna().astype(str)) | set(lab['s16bq07b'].dropna().astype(str))
                     | set(lab['s16bq09b'].dropna().astype(str)))
        bm = get_categorical_mapping(tablename='u', idxvars='Original Label', dirs=[str(T / '2018' / '_') + '/', str(T / '_') + '/'],
                                     **{'Preferred Label': 'Preferred Label'})
        via_u = dict(zip(raw, served_unit_labels('Togo', [bm.get(x, x) for x in raw])))
        via_unit = dict(zip(raw, served_unit_labels('Togo', raw)))
        g['unit paths: raw -> (served u path, Unit_* path)'] = {x: [str(via_u[x]), str(via_unit[x])] for x in raw}
        g['unit paths disagree'] = [x for x in raw if via_u[x] != via_unit[x]]
        lab21 = get_dataframe(T / '2021-22' / 'Data' / 's16b_me_tgo2021.dta', convert_categoricals=True)
        raw21 = sorted(set(lab21['s16bq09b'].dropna().astype(str).str.strip()) | set(lab21['s16bq07b'].dropna().astype(str).str.strip()))
        g['2021-22 acquisition raw -> served'] = dict(zip(raw21, map(str, served_unit_labels('Togo', raw21))))
    except Exception as e:
        g['unit path error'] = f'{type(e).__name__}: {e}'
    # grappe 101 menage 9
    for nm, fr in (('base', b), ('cand', k)):
        sub = fr[fr.index.get_level_values('i') == '101009']
        sub = sub[sub.index.get_level_values('input') == 'Seed']
        g[f'101009 seed rows {nm}'] = sub.reset_index().astype(str).to_dict(orient='records')
    R['togo'] = g

if C == 'Senegal':
    out = {}
    for nm, d, fr in (('base', B, b), ('cand', K, k)):
        cp = load(d, 'crop_production')
        if cp is None:
            continue
        cpi = cp.reset_index()
        harvest = set(zip(cpi['t'].astype(str), cpi['i'].astype(str), cpi['crop'].astype(str)))
        pi = fr.reset_index()
        seeds = pi[pi['input'] == 'Seed']
        res = {}
        for (t, crop), sub in seeds.groupby(['t', 'crop']):
            hit = sum((str(a), str(b_), str(c)) in harvest for a, b_, c in zip(sub['t'], sub['i'], sub['crop']))
            res[f'{t}|{crop}'] = [len(sub), int(hit)]
        out[f'join {nm}'] = res
        out[f'crop_production crops {nm}'] = {str(a): int(v) for a, v in cpi['crop'].value_counts().head(40).items()}
    for t in ('crop_production', 'plot_features'):
        x, y = load(B, t), load(K, t)
        if x is None or y is None:
            out[f'{t} equal'] = 'missing'
            continue
        try:
            pd.testing.assert_frame_equal(x, y)
            out[f'{t} equal'] = [True, len(x)]
        except AssertionError as e:
            out[f'{t} equal'] = [False, str(e)[:400]]
    try:
        from lsms_library.local_tools import df_from_orgfile
        from lsms_library.paths import countries_root
        sc = df_from_orgfile(countries_root() / 'Senegal' / '_' / 'categorical_mapping.org', name='harmonize_seed_crop')
        out['seed_crop table hex'] = {r['Original Label']: [r['Preferred Label'], r['Preferred Label'].encode().hex()]
                                      for _, r in sc.iterrows()}
    except Exception as e:
        out['seed_crop table error'] = str(e)
    R['senegal'] = out

for fn in ('seed_kg', 'nitrogen_kg'):
    x, y = load(B, fn), load(K, fn)
    if x is None or y is None:
        R[fn] = 'missing'
        continue
    try:
        pd.testing.assert_frame_equal(x, y)
        R[fn] = ['equal', len(x), float(x.iloc[:, 0].sum())]
    except AssertionError as e:
        R[fn] = ['DIFF', str(e)[:400]]

# Country vs Feature (both sides)
for nm, d in (('base', B), ('cand', K)):
    fr, ft = load(d, 'plot_inputs'), load(d, 'feature_plot_inputs')
    if ft is None:
        R[f'feature_{nm}'] = 'missing'
        continue
    fi = ft.index.to_frame(index=False)
    drop = [n for n in ('country', 'plot_id', 'season', 'input_obs', 'currency') if n in fi.columns]
    rec_levels = {n: sorted(map(str, fi[n].unique()))[:5] for n in drop}
    fi = fi.drop(columns=drop).rename(columns={'j': 'crop'})
    f2 = ft.copy(); f2.index = pd.MultiIndex.from_frame(fi)
    f2 = f2[[c_ for c_ in f2.columns if c_ in fr.columns]]
    ci = fr.index.to_frame(index=False)
    f2 = f2.reorder_levels(list(ci.columns)) if set(ci.columns) == set(f2.index.names) else f2
    rec = {'feature_index': list(ft.index.names), 'country_index': list(fr.index.names),
           'rows': [len(fr), len(ft)], 'feature_unique': bool(ft.index.is_unique),
           'extra_feature_cols': [c_ for c_ in ft.columns if c_ not in fr.columns], 'dropped_levels': rec_levels}
    try:
        fs, cs = f2.sort_index(), fr.sort_index()
        rec['index_equal'] = bool(len(fs) == len(cs) and (fs.index == cs.index).all())
        if rec['index_equal']:
            rec['cells_diff'] = cell_diff(cs, fs)
            rec['dtype_diff'] = {c_: [str(cs[c_].dtype), str(fs[c_].dtype)] for c_ in cs.columns
                                 if c_ in fs.columns and str(cs[c_].dtype) != str(fs[c_].dtype)}
    except Exception as e:
        rec['error'] = f'{type(e).__name__}: {e}'
    R[f'feature_vs_country_{nm}'] = rec

with open(OUTP, 'w') as fh:
    json.dump(R, fh, indent=1, ensure_ascii=False, default=str)
print(json.dumps({k: v for k, v in R.items() if k in ('rows', 'u_manquant', 'u_unknown', 'keys_equal_after_relabel',
                                                       'cells_changed_by_t', 'seed_kg', 'nitrogen_kg')},
                 ensure_ascii=False, default=str))
