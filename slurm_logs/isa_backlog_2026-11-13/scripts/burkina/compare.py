"""Compare two measure.py runs (base, cand).  Counts only.

Usage: compare.py BASE_FRAMES CAND_FRAMES BASE_JSON CAND_JSON OUT_JSON
(cwd = the candidate clone; reads ehcvm_nsu_bfa2021.dta through get_dataframe for scoring)
"""
import json
import re
import sys
import unicodedata
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

BF, CF, BJ, CJ, OJ = sys.argv[1:6]
jb, jc = json.load(open(BJ)), json.load(open(CJ))
res = {'base': jb['head'], 'cand': jc['head'], 'tables': {}}


def save():
    with open(OJ, 'w') as fh:
        json.dump(res, fh, indent=1, default=str, sort_keys=True)


def norm_col(s):
    if pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s):
        return s.astype('Float64')
    return s.astype('object').where(s.notna(), None).map(lambda x: None if x is None else str(x))


def compare_frames(name, idx):
    try:
        fa = pd.read_parquet(f'{BF}/{name}.parquet')
    except Exception:  # noqa: BLE001
        fa = None
    try:
        fb = pd.read_parquet(f'{CF}/{name}.parquet')
    except Exception:  # noqa: BLE001
        fb = None
    if fa is None or fb is None:
        return {'missing': [fa is None, fb is None]}
    rec = {}
    if list(fa.columns) != list(fb.columns):
        rec['columns'] = [sorted(set(fa.columns) - set(fb.columns)), sorted(set(fb.columns) - set(fa.columns))]
    dt = {c: (str(fa[c].dtype), str(fb[c].dtype)) for c in set(fa.columns) & set(fb.columns)
          if str(fa[c].dtype) != str(fb[c].dtype)}
    if dt:
        rec['dtype'] = dt
    keys = [c for c in idx if c and c in fa.columns and c in fb.columns]

    def jk(d):
        if not keys:
            return pd.Series(range(len(d)), index=d.index).astype(str)
        o = None
        for c in keys:
            s_ = d[c].astype(object).where(d[c].notna(), '<NA>').map(str)
            o = s_ if o is None else o + '\x1f' + s_
        return o
    ka, kb = jk(fa), jk(fb)
    sa, sb = set(ka), set(kb)
    if len(fa) != len(fb) or sa != sb:
        rec['rows'] = [len(fa), len(fb)]
        rec['only_base'] = len(sa - sb)
        rec['only_cand'] = len(sb - sa)
        if 't' in fa and 't' in fb:
            ob = fa[ka.isin(sa - sb).to_numpy()]
            oc = fb[kb.isin(sb - sa).to_numpy()]
            rec['only_base_by_t'] = {str(k): int(v) for k, v in ob['t'].astype(str).value_counts().items()}
            rec['only_cand_by_t'] = {str(k): int(v) for k, v in oc['t'].astype(str).value_counts().items()}
    fa2 = fa.assign(_k=ka).drop_duplicates('_k').set_index('_k')
    fb2 = fb.assign(_k=kb).drop_duplicates('_k').set_index('_k')
    common = fa2.index.intersection(fb2.index)
    cd, cdt = {}, {}
    for c in fa.columns:
        if c not in fb.columns or c in keys:
            continue
        x, y = norm_col(fa2.loc[common, c]), norm_col(fb2.loc[common, c])
        xa, ya = x.isna().to_numpy(dtype=bool), y.isna().to_numpy(dtype=bool)
        if pd.api.types.is_float_dtype(x) and pd.api.types.is_float_dtype(y):
            xv = x.to_numpy(dtype='float64', na_value=np.nan)
            yv = y.to_numpy(dtype='float64', na_value=np.nan)
            ne = (xa != ya) | ((~xa) & (~ya) & ~np.isclose(xv, yv, rtol=1e-12, atol=0))
        else:
            xv, yv = x.to_numpy(dtype=object), y.to_numpy(dtype=object)
            ne = (xa != ya) | np.array([(not p and not q and u != w) for p, q, u, w in
                                        zip(xa, ya, xv, yv)], dtype=bool)
        if ne.any():
            cd[c] = int(ne.sum())
            if 't' in fa2:
                cdt[c] = {str(k): int(v) for k, v in
                          pd.Series(fa2.loc[common, 't'].astype(str).to_numpy()[ne]).value_counts().items()}
    if cd:
        rec['cells_differ_on_common'] = cd
        rec['cells_differ_by_t'] = cdt
    dupa, dupb = int(ka.duplicated().sum()), int(kb.duplicated().sum())
    if dupa or dupb:
        rec['dup_keys'] = [dupa, dupb]
    return rec


names = sorted(set(jb['calls']) | set(jc['calls']))
equal = []
for n in names:
    ca, cb = jb['calls'].get(n, {}), jc['calls'].get(n, {})
    if ca.get('err') or cb.get('err'):
        res['tables'][n] = {'err_base': (ca.get('err') or '')[:300], 'err_cand': (cb.get('err') or '')[:300]}
        continue
    idx = cb.get('index') or ca.get('index') or []
    r = compare_frames(n, idx)
    r['rows_by_t'] = [ca.get('rows_by_t'), cb.get('rows_by_t')]
    r['digest_equal'] = ca.get('digest') == cb.get('digest')
    if r['digest_equal'] and not any(k in r for k in ('columns', 'dtype', 'rows', 'cells_differ_on_common')):
        equal.append(n)
    res['tables'][n] = r
res['frame_equal'] = equal
save()

# ---- #1442 x #1509: kg factors, rungs, kg by wave and s, scored against the NSU weights
try:
    b = pd.read_parquet(f'{BF}/kg_factors.parquet')
    c = pd.read_parquet(f'{CF}/kg_factors.parquet')
    for d in (b, c):
        for k in ('t', 'v', 'i', 'j', 'u', 's'):
            d[k] = d[k].astype(str)
        d['visit'] = d['visit'].astype(int)
    keys = ['t', 'v', 'i', 'j', 'u', 's', 'visit']
    kgs = {}
    for lab, d in (('base', b), ('cand', c)):
        q = pd.to_numeric(d['Quantity'], errors='coerce') * pd.to_numeric(d['kg_per_unit'], errors='coerce')
        kgs[lab] = {
            'kg_by_t': {k: round(float(v), 1) for k, v in q.groupby(d['t']).sum().items()},
            'kg_by_t_s': {f'{a}/{s}': round(float(v), 1) for (a, s), v in q.groupby([d['t'], d['s']]).sum().items()},
            'source_by_t': {f'{a}/{s}': int(v) for (a, s), v in d.groupby(['t', 'KgFactorSource']).size().items()},
            'rows': int(len(d)),
        }
    res['kg'] = kgs
    m = b.merge(c, on=keys, how='outer', suffixes=('_b', '_c'), indicator=True)
    res['kg_merge'] = {str(k): int(v) for k, v in m['_merge'].value_counts().items()}
    both = m[m['_merge'] == 'both'].copy()
    rb = pd.to_numeric(both['kg_per_unit_b'], errors='coerce')
    rc = pd.to_numeric(both['kg_per_unit_c'], errors='coerce')
    moved = ~((rb.isna() & rc.isna()) | np.isclose(rb, rc, rtol=1e-9, equal_nan=False))
    both['moved'] = moved.to_numpy()
    res['kg_rows_moved'] = int(moved.sum())
    res['kg_rows_moved_by_t_s'] = {f'{a}/{s}': int(v) for (a, s), v in both[both['moved']].groupby(['t', 's']).size().items()}
    tr = both.groupby(['KgFactorSource_b', 'KgFactorSource_c']).size()
    res['rung_transitions'] = {f'{a}->{z}': int(v) for (a, z), v in tr.items() if a != z}
    pur = both['KgFactorSource_b'].isin(['survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'unit']) & \
        both['KgFactorSource_c'].isin(['survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'unit'])
    res['purchase_rung_rows_factor_moved'] = int((pur & both['moved']).sum())
    res['purchase_rung_rows_factor_moved_by_t_s'] = {f'{a}/{s}': int(v) for (a, s), v in
                                                     both[pur & both['moved']].groupby(['t', 's']).size().items()}
    ratio = (rc / rb)[both['moved'] & rb.notna() & rc.notna()]
    if len(ratio):
        res['moved_ratio_quantiles'] = {str(q): round(float(v), 4) for q, v in ratio.quantile([.01, .1, .5, .9, .99]).items()}
    res['kg_attrs_cand_evidence'] = jc.get('kg_attrs', {}).get('kg_factor_evidence')
    res['kg_attrs_base_evidence'] = jb.get('kg_attrs', {}).get('kg_factor_evidence')
    save()

    # Score against the survey's own national unit weights (outside reference; label join)
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    from lsms_library.ehcvm import size_label
    from lsms_library.country import Country, _categorical_key_column
    from lsms_library.transformations import _unit_size_base

    def norm(s):
        if s is None or (not isinstance(s, str) and pd.isna(s)):
            return None
        s = str(s)
        try:
            s = s.encode('latin-1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        s = re.sub(r'^\s*\d+\s*[.\-]\s*', '', s).strip().lower()
        s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
        return re.sub(r'\s+', ' ', s)
    x = get_dataframe(str(countries_root() / 'Burkina_Faso' / '2021-22' / 'Data' / 'ehcvm_nsu_bfa2021.dta')).copy()
    maps = Country('Burkina_Faso').categorical_mapping
    hf = maps.get('harmonize_food')
    kk = _categorical_key_column(hf)
    j_map = {norm(p): q for p, q in zip(hf[kk].astype(str), hf['Preferred Label'].astype(str))}
    x['j0'] = [norm(j_map.get(norm(p), p)) for p in x['codpr'].astype('string')]
    x['u0'] = [norm(re.sub(r'\s+', ' ', str(s)).strip()) for s in x['uniteID'].astype('string')]
    x['z0'] = [(norm(size_label(s)) if size_label(s) else None) for s in x['tailleID'].astype('string')]
    x['kg'] = pd.to_numeric(x['poids_moyen'], errors='coerce') / 1000
    x = x[x['kg'] > 0]
    t1 = x.groupby(['j0', 'u0', 'z0'], dropna=False)['kg'].median().rename('truth').reset_index()
    t2 = x.groupby(['j0', 'u0'])['kg'].median().rename('truth2').reset_index()

    def split(u):
        bb = _unit_size_base(u) if isinstance(u, str) else None
        return (u, None) if bb is None else (bb, u[len(bb) + 2:-1])

    def add(M, u='u', j='j'):
        M = M.copy()
        pr = M[u].map(split)
        M['u0'] = pr.map(lambda p: norm(p[0]))
        M['z0'] = pr.map(lambda p: norm(p[1]) if p[1] else None)
        M['jn'] = M[j].map(norm)
        J = M.merge(t1, left_on=['jn', 'u0', 'z0'], right_on=['j0', 'u0', 'z0'], how='left').merge(
            t2, left_on=['jn', 'u0'], right_on=['j0', 'u0'], how='left')
        J['truth'] = J['truth'].where(J['z0'].notna(), J['truth2'])
        return J
    INF = {'item_unit', 'item_unit_tight', 'unit', 'item_unit_nonpurchase', 'unit_nonpurchase'}

    def fmt(e):
        return {'n': int(len(e)), 'medlog': round(float(e.median()), 3) if len(e) else None,
                'within2': round(100 * float((e < np.log(2)).mean()), 1) if len(e) else None}
    score = {}
    for lab, M in (('base', b), ('cand', c)):
        J = add(M)
        J = J[J['KgFactorSource'].isin(INF) & np.isfinite(pd.to_numeric(J['kg_per_unit'], errors='coerce'))
              & J['truth'].notna()]
        e = np.abs(np.log(pd.to_numeric(J['kg_per_unit']) / J['truth']))
        score[lab] = {'all': fmt(e), 'by_t': {w: fmt(e[J['t'] == w]) for w in sorted(J['t'].unique())},
                      'by_t_source': {f'{w}/{s}': fmt(e[(J['t'] == w) & (J['KgFactorSource'] == s)])
                                      for w in sorted(J['t'].unique()) for s in sorted(J['KgFactorSource'].unique())
                                      if ((J['t'] == w) & (J['KgFactorSource'] == s)).any()}}
    mv = both[both['moved']].rename(columns={'kg_per_unit_c': 'kc', 'kg_per_unit_b': 'kb'})
    if len(mv):
        J = add(mv)
        J = J[J['truth'].notna() & J['kb'].notna() & J['kc'].notna()]
        score['moved_rows_with_weight'] = {
            'base': fmt(np.abs(np.log(pd.to_numeric(J['kb']) / J['truth']))),
            'cand': fmt(np.abs(np.log(pd.to_numeric(J['kc']) / J['truth'])))}
    res['nsu_score'] = score
except Exception:  # noqa: BLE001
    import traceback
    res['kg_err'] = traceback.format_exc()[-2500:]
save()

# ---- #1442 read-time effects from the saved frames
try:
    eff = {}
    for lab, F in (('base', BF), ('cand', CF)):
        fa = pd.read_parquet(f'{F}/food_acquired.parquet')
        fa['t'] = fa['t'].astype(str)
        w = fa[fa['t'] == '2014']
        e = {'rows_by_s_visit': {f'{s}/{v}': int(n) for (s, v), n in w.groupby([w['s'].astype(str), w['visit']]).size().items()},
             'exp_by_s_visit': {f'{s}/{v}': round(float(x), 1) for (s, v), x in
                                w.groupby([w['s'].astype(str), w['visit']])['Expenditure'].sum().items()},
             'rows_by_t': {k: int(v) for k, v in fa['t'].value_counts().items()}}
        piv = w[w['Expenditure'] > 0].pivot_table(index=['i', 'j', 'visit'], columns='s', values='Expenditure', aggfunc='sum')
        for s2 in ('produced', 'inkind'):
            if s2 in piv and 'purchased' in piv:
                eq = piv[piv['purchased'] == piv[s2]]
                e[f'equal_money_purchased_{s2}'] = [int(len(eq)), round(float(eq[s2].sum()), 1)]
        for tab in ('food_expenditures', 'food_expenditures_total', 'food_expenditures_valued', 'food_prices',
                    'food_prices_unitvalue', 'food_prices_kgprice', 'food_quantities', 'food_quantities_units'):
            try:
                d = pd.read_parquet(f'{F}/{tab}.parquet')
                d['t'] = d['t'].astype(str)
                col = 'Expenditure' if 'Expenditure' in d else ('Price' if 'Price' in d else 'Quantity')
                rec = {'rows_by_t': {k: int(v) for k, v in d['t'].value_counts().items()},
                       'sum_by_t': {k: round(float(v), 1) for k, v in d.groupby('t')[col].sum().items()}}
                if 's' in d:
                    rec['rows_by_t_s'] = {f'{a}/{s}': int(v) for (a, s), v in d.groupby(['t', d['s'].astype(str)]).size().items()}
                    if col != 'Quantity':
                        rec['sum_by_t_s'] = {f'{a}/{s}': round(float(v), 1) for (a, s), v in
                                             d.groupby(['t', d['s'].astype(str)])[col].sum().items()}
                if tab.startswith('food_quantities') and 'u' in d:
                    kgr = d[d['u'].astype(str) == 'kg']
                    rec['kg_by_t'] = {k: round(float(v), 1) for k, v in kgr.groupby('t')['Quantity'].sum().items()}
                eff.setdefault(lab, {})[tab] = rec
            except Exception as ex:  # noqa: BLE001
                eff.setdefault(lab, {})[tab] = f'ERR {ex}'[:200]
        eff[lab]['food_acquired_2014'] = e
    res['effects'] = eff
    res['attrs'] = {lab: {k: J_['calls'].get(k, {}).get('attrs') for k in
                          ('food_prices', 'food_prices_unitvalue', 'food_prices_kgprice', 'food_quantities',
                           'food_quantities_units', 'food_expenditures_valued')}
                    for lab, J_ in (('base', jb), ('cand', jc))}
except Exception:  # noqa: BLE001
    import traceback
    res['effects_err'] = traceback.format_exc()[-2500:]
save()
print('compare done', len(equal), 'frame-equal of', len(names), flush=True)
