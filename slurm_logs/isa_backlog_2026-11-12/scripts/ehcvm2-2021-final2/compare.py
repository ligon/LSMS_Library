"""Compare base / prev / cand frames from build_food.py.  Aggregates only.

usage: python -I compare.py BASEDIR PREVDIR CANDDIR OUT_JSON
Older waves: cand must equal base for sample, food_acquired, food_expenditures,
food_quantities_units (Togo 2018 food_acquired/food_quantities_units after mapping base's
u through the 659/660/662 decode) and equal prev for the kg modes.  2021-22: cand vs prev.
"""
import json, os, re, sys, collections
import numpy as np
import pandas as pd

base_dir, prev_dir, cand_dir, out_path = sys.argv[1:5]
NEW = '2021-22'
DECODE = {'659': 'Main', '660': 'Quartier', '662': 'Tête'}
INDEX = {'sample': ['i', 't'], 'food_acquired': ['t', 'v', 'i', 'j', 'u', 's'],
         'food_expenditures': ['t', 'i', 'j', 's'], 'food_quantities': ['t', 'i', 'j', 'u', 's'],
         'food_quantities_units': ['t', 'i', 'j', 'u', 's'], 'food_prices': None}


def decode_u(u):
    if not isinstance(u, str):
        return u
    m = re.match(r'^(\d+)( \(.*\))?$', u)
    if m and m.group(1) in DECODE:
        return DECODE[m.group(1)] + (m.group(2) or '')
    return u


def load(d, country, name):
    p = os.path.join(d, country, f'{name}.parquet')
    if not os.path.exists(p):
        return None
    df = pd.read_parquet(p)
    idx = INDEX.get(name) or [c for c in df.columns if c in ('t', 'v', 'i', 'j', 'u', 's', 'm')]
    idx = [c for c in idx if c in df.columns]
    return df.set_index(idx).sort_index()


def equal(a, b, cols=None):
    if a is None or b is None:
        return 'missing'
    cols = cols or [c for c in a.columns if c in b.columns]
    if len(a) != len(b):
        return f'rows {len(a)} vs {len(b)}'
    try:
        pd.testing.assert_frame_equal(a[cols], b[cols], check_like=False, check_dtype=False, check_exact=False)
        return 'equal'
    except AssertionError as e:
        return 'differ: ' + str(e)[:300]


def waves_of(df):
    return sorted(set(df.index.get_level_values('t').astype(str)))


rep = {}
for country in ('Togo', 'Benin', 'CotedIvoire'):
    R = rep[country] = {'older': {}, 'new': {}}
    for name in INDEX:
        b, p, c = (load(d, country, name) for d in (base_dir, prev_dir, cand_dir))
        if c is None:
            R['older'][name] = 'cand missing'
            continue
        old_waves = [w for w in waves_of(b)] if b is not None else []
        bo = b[b.index.get_level_values('t').astype(str).isin(old_waves)] if b is not None else None
        co = c[c.index.get_level_values('t').astype(str).isin(old_waves)]
        po = p[p.index.get_level_values('t').astype(str).isin(old_waves)] if p is not None else None
        rec = {}
        if country == 'Togo' and name in ('food_acquired', 'food_quantities_units', 'food_quantities') and bo is not None:
            flat = bo.reset_index()
            if 'u' in flat.columns:
                moved = flat['u'].map(decode_u) != flat['u']
                rec['base_rows_u_decoded'] = int(moved.sum())
                rec['decoded_by_u_s'] = {f'{u0} -> {decode_u(u0)} | {s0}': int(n) for (u0, s0), n in
                                         flat[moved].groupby(['u', 's']).size().items()} if 's' in flat.columns else None
                rec['decoded_by_j'] = {str(k): int(v) for k, v in flat[moved]['j'].value_counts().items()}
                flat['u'] = flat['u'].map(decode_u)
                bo = flat.set_index(list(bo.index.names)).sort_index()
        rec['cand_vs_base'] = equal(co, bo)
        rec['cand_vs_prev'] = equal(co, po)
        R['older'][name] = rec
        # 2021-22
        cn = c[c.index.get_level_values('t').astype(str) == NEW]
        pn = p[p.index.get_level_values('t').astype(str) == NEW] if p is not None else None
        nr = {'rows_prev': None if pn is None else int(len(pn)), 'rows_cand': int(len(cn)),
              'cand_vs_prev': equal(cn, pn)}
        for col in ('Quantity', 'Expenditure', 'Price', 'weight', 'strata'):
            if col in cn.columns and pd.api.types.is_numeric_dtype(cn[col]):
                nr[f'{col}_sum'] = [None if pn is None else float(pn[col].sum()), float(cn[col].sum())]
                nr[f'{col}_na'] = [None if pn is None else int(pn[col].isna().sum()), int(cn[col].isna().sum())]
        if 's' in cn.index.names:
            nr['by_s'] = {s: [None if pn is None else int((pn.index.get_level_values('s') == s).sum()),
                              int((cn.index.get_level_values('s') == s).sum())] for s in ('purchased', 'produced', 'inkind')}
        if name == 'food_acquired':
            for lab, f in (('prev', pn), ('cand', cn)):
                if f is None:
                    continue
                pur = f[f.index.get_level_values('s') == 'purchased']
                nr[f'{lab}_purchased_rows'] = int(len(pur))
                nr[f'{lab}_purchased_expenditure_na'] = int(pur['Expenditure'].isna().sum())
                nr[f'{lab}_purchased_expenditure_na_share'] = round(float(pur['Expenditure'].isna().mean()), 4)
                nr[f'{lab}_purchased_quantity_na'] = int(pur['Quantity'].isna().sum())
                keys = collections.Counter(k for v in f['Derivation'].dropna().astype(str) for k in v.split('+'))
                nr[f'{lab}_keys'] = dict(keys)
                nr[f'{lab}_key_combos'] = {str(k): int(v) for k, v in f['Derivation'].value_counts(dropna=False).items()}
            # region totals (2021-22 region from cand sample strata minus the milieu word)
            s = load(cand_dir, country, 'sample')
            if s is not None and 'strata' in s.columns:
                st = s[s.index.get_level_values('t').astype(str) == NEW]['strata'].astype(str)
                region = st.str.rsplit(' ', n=1).str[0]
                region.index = st.index.get_level_values('i')
                for lab, f in (('prev', pn), ('cand', cn)):
                    if f is None:
                        continue
                    pur = f[f.index.get_level_values('s') == 'purchased']
                    rr = pd.Series(region.reindex(pur.index.get_level_values('i')).to_numpy(), index=pur.index)
                    g = pur.assign(region=rr.to_numpy()).groupby('region')
                    nr[f'{lab}_by_region'] = {k: {'purchased_rows': int(len(v)),
                                                  'expenditure': round(float(v['Expenditure'].sum()), 2),
                                                  'expenditure_na_share': round(float(v['Expenditure'].isna().mean()), 4),
                                                  'quantity_na': int(v['Quantity'].isna().sum())}
                                              for k, v in g}
            # Expenditure share of the rows the new keys mark, read off prev's values
            if pn is not None:
                for key in set(k for v in cn['Derivation'].dropna().astype(str) for k in v.split('+')):
                    if '7day' in key:
                        continue
                    m = cn['Derivation'].fillna('').astype(str).str.split('+').apply(lambda ks: key in ks)
                    idx = cn.index[m.to_numpy()]
                    inter = idx.intersection(pn.index)
                    pur_prev = pn[pn.index.get_level_values('s') == 'purchased']
                    e_prev = float(pur_prev['Expenditure'].sum())
                    e_rows = float(pn.loc[inter][pn.loc[inter].index.get_level_values('s') == 'purchased']['Expenditure'].sum())
                    nr[f'share|{key}'] = {'rows': int(m.sum()), 'rows_in_prev': int(len(inter)),
                                          'by_s': {str(k): int(v) for k, v in pd.Series(idx.get_level_values('s')).value_counts().items()},
                                          'prev_purchased_expenditure_of_rows': round(e_rows, 2),
                                          'share_of_prev_purchased_expenditure': round(e_rows / e_prev, 4) if e_prev else None,
                                          'cand_quantity_sum': float(cn.loc[m.to_numpy(), 'Quantity'].sum()),
                                          'cand_quantity_na': int(cn.loc[m.to_numpy(), 'Quantity'].isna().sum())}
        if name in ('food_quantities', 'food_prices') and 'u' in cn.index.names:
            for lab, f in (('prev', pn), ('cand', cn)):
                if f is None:
                    continue
                kg = f[f.index.get_level_values('u') == 'kg']
                nr[f'{lab}_kg_rows'] = int(len(kg))
                nr[f'{lab}_kg_total'] = round(float(kg.iloc[:, 0].sum()), 2)
        R['new'][name] = nr
    # kg factors: per (j, u) cell, prev vs cand, by wave
    kp, kc = (os.path.join(d, country, 'kgf.parquet') for d in (prev_dir, cand_dir))
    if os.path.exists(kp) and os.path.exists(kc):
        a, b = pd.read_parquet(kp), pd.read_parquet(kc)
        if country == 'Togo':
            a['u'] = a['u'].map(decode_u)
        cell = lambda f: f.groupby(['j', 'u'])['kg_per_unit'].median()
        ca, cb = cell(a), cell(b)
        both = ca.index.intersection(cb.index)
        moved = both[~np.isclose(ca.loc[both].fillna(-1), cb.loc[both].fillna(-1), rtol=1e-9)]
        R['kg_cells'] = {'cells_prev': int(len(ca)), 'cells_cand': int(len(cb)), 'common': int(len(both)),
                         'moved': int(len(moved)),
                         'moved_examples': [f'{j} | {u}: {ca[(j, u)]:.4g} -> {cb[(j, u)]:.4g}' for j, u in list(moved)[:25]],
                         'moved_units': {str(k): int(v) for k, v in pd.Series([u for _, u in moved]).value_counts().head(20).items()}}
        for lab, f in (('prev', a), ('cand', b)):
            R['kg_cells'][f'sources_{lab}'] = {f'{t}|{s}': int(n) for (t, s), n in f.groupby(['t', 'KgFactorSource']).size().items()}
        # older-wave rows: per-row factor equality
        ia = a.set_index(['t', 'v', 'i', 'j', 'u', 's'])['kg_per_unit']
        ib = b.set_index(['t', 'v', 'i', 'j', 'u', 's'])['kg_per_unit']
        old = [w for w in sorted(set(a['t'].astype(str))) if w != NEW]
        ia_o = ia[ia.index.get_level_values('t').astype(str).isin(old)]
        ib_o = ib[ib.index.get_level_values('t').astype(str).isin(old)]
        common = ia_o.index.intersection(ib_o.index)
        diff = ~np.isclose(ia_o.loc[common].fillna(-1).to_numpy(), ib_o.loc[common].fillna(-1).to_numpy(), rtol=1e-9)
        R['kg_rows_older'] = {'rows_prev': int(len(ia_o)), 'rows_cand': int(len(ib_o)), 'common': int(len(common)),
                              'factor_moved': int(diff.sum()),
                              'moved_by_u': {str(k): int(v) for k, v in pd.Series(common[diff].get_level_values('u')).value_counts().head(20).items()}}
    # older waves: kilograms per wave and price ratios, base / prev / cand
    def decoded(f):
        if country != 'Togo' or f is None or 'u' not in f.index.names:
            return f
        flat = f.reset_index()
        flat['u'] = flat['u'].map(decode_u)
        return flat.set_index(list(f.index.names)).sort_index()

    fq = {lab: decoded(load(d, country, 'food_quantities')) for lab, d in (('base', base_dir), ('prev', prev_dir), ('cand', cand_dir))}
    fp = {lab: decoded(load(d, country, 'food_prices')) for lab, d in (('base', base_dir), ('prev', prev_dir), ('cand', cand_dir))}
    R['older_kg'] = {}
    for w in waves_of(fq['base']):
        row = {}
        for lab, f in fq.items():
            x = f[(f.index.get_level_values('t').astype(str) == w) & (f.index.get_level_values('u') == 'kg')]
            row[f'{lab}_kg_rows'] = int(len(x))
            row[f'{lab}_kg_total'] = round(float(pd.to_numeric(x['Quantity'], errors='coerce').sum()), 2)
        a = fq['prev'][fq['prev'].index.get_level_values('t').astype(str) == w]['Quantity']
        b = fq['cand'][fq['cand'].index.get_level_values('t').astype(str) == w]['Quantity']
        common = a.index.intersection(b.index)
        r = (b.loc[common] / a.loc[common]).replace([np.inf, -np.inf], np.nan).dropna()
        moved = r[~np.isclose(r, 1.0, rtol=1e-9)]
        row['prev_cand_rows_common'] = int(len(common))
        row['prev_cand_rows_moved'] = int(len(moved))
        row['prev_cand_moved_ratio_q10_50_90'] = [round(float(x), 4) for x in moved.quantile([0.1, 0.5, 0.9])] if len(moved) else None
        row['rows_only_prev'] = int(len(a.index.difference(b.index)))
        row['rows_only_cand'] = int(len(b.index.difference(a.index)))
        pa = fp['prev'][fp['prev'].index.get_level_values('t').astype(str) == w]['Price']
        pb = fp['cand'][fp['cand'].index.get_level_values('t').astype(str) == w]['Price']
        pc = pa.index.intersection(pb.index)
        pr = (pb.loc[pc] / pa.loc[pc]).replace([np.inf, -np.inf], np.nan).dropna()
        pm = pr[~np.isclose(pr, 1.0, rtol=1e-9)]
        row['price_rows_common'] = int(len(pc))
        row['price_rows_moved'] = int(len(pm))
        row['price_moved_ratio_q10_50_90'] = [round(float(x), 4) for x in pm.quantile([0.1, 0.5, 0.9])] if len(pm) else None
        row['price_sum_prev_cand'] = [round(float(pa.sum()), 2), round(float(pb.sum()), 2)]
        R['older_kg'][w] = row
json.dump(rep, open(out_path, 'w'), indent=1, default=str, ensure_ascii=False)
print('DONE')
