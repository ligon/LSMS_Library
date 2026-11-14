"""Compare base vs candidate Malawi frames saved by build_all.py.

usage: python compare.py FRAMES_DIR OUT_JSON
Aggregate counts only; no household identifiers are written.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

F = Path(sys.argv[1])
OUT = Path(sys.argv[2])
NEW = '2024-25'
B, C = F / 'base', F / 'cand'
sb = json.loads((B / 'summary.json').read_text())
sc = json.loads((C / 'summary.json').read_text())
DERIVED = {'food_expenditures', 'food_quantities', 'food_prices'}
res = {'base_head': sb['head'], 'cand_head': sc['head'], 'tables': {},
       'panel': {}, 'derived': {}, 'feature': {}}


def load(d, name):
    p = d / f'{name}.parquet'
    if p.exists():
        return pd.read_parquet(p)
    p = d / f'{name}.pkl'
    return pd.read_pickle(p) if p.exists() else None


def drop_new(df):
    if df is None or 't' not in df.index.names:
        return df
    return df[df.index.get_level_values('t') != NEW]


def frame_equal(b, c):
    """(equal, detail).  Exact: values, NA masks, dtypes, index (sorted)."""
    b = b.sort_index()
    c = c.sort_index()
    try:
        pd.testing.assert_frame_equal(b, c, check_exact=True, check_like=False)
        return True, 'assert_frame_equal(check_exact=True) passed'
    except AssertionError as exc:
        return False, str(exc)[:1500]


for t in sc['tables']:
    rb, rc = sb['tables'].get(t, {}), sc['tables'][t]
    rec = {'base_ok': rb.get('ok'), 'cand_ok': rc.get('ok'),
           'base_rows': rb.get('rows'), 'cand_rows': rc.get('rows'),
           'base_rows_per_wave': rb.get('rows_per_wave'),
           'cand_rows_per_wave': rc.get('rows_per_wave'),
           'base_dup_index': rb.get('dup_index'), 'cand_dup_index': rc.get('dup_index'),
           'base_warnings': rb.get('warnings', {}).get('counts'),
           'cand_warnings': rc.get('warnings', {}).get('counts'),
           'base_reports': rb.get('reports'), 'cand_reports': rc.get('reports'),
           'base_error': rb.get('error_class'), 'cand_error': rc.get('error_class'),
           'cand_dtypes_vs_base_equal': rb.get('dtypes') == rc.get('dtypes'),
           'cand_index_dtypes_vs_base_equal': rb.get('index_dtypes') == rc.get('index_dtypes')}
    b, c = load(B, t), load(C, t)
    if b is not None and c is not None:
        c_old = drop_new(c)
        b_old = drop_new(b)
        rec['base_has_2024'] = bool(len(b) - len(b_old))
        extra = [x for x in c_old.columns if x not in b_old.columns]
        missing = [x for x in b_old.columns if x not in c_old.columns]
        rec['cand_extra_columns'] = extra
        rec['cand_missing_columns'] = missing
        if extra:
            rec['cand_extra_columns_nonnull_old_waves'] = {
                x: int(c_old[x].notna().sum()) for x in extra}
        common = [x for x in b_old.columns if x in c_old.columns]
        eq, detail = frame_equal(b_old[common], c_old[common])
        rec['old_waves_frame_equal'] = eq
        if not eq:
            rec['old_waves_detail'] = detail
            # quantify: index set equality and per-column mismatch counts
            bi, ci = b_old.index, c_old.index
            rec['old_index_equal_as_sets'] = bool(set(bi) == set(ci)) if len(bi) < 3_000_000 else None
            inter = bi.intersection(ci)
            rec['old_rows_common'] = int(len(inter))
            rec['old_rows_only_base'] = int(len(bi.difference(ci)))
            rec['old_rows_only_cand'] = int(len(ci.difference(bi)))
            if len(inter) and not bi.duplicated().any() and not ci.duplicated().any():
                bb, cc = b_old.loc[inter, common], c_old.loc[inter, common]
                mm = {}
                for col in common:
                    x, y = bb[col], cc[col]
                    try:
                        diff = ~((x == y) | (x.isna() & y.isna()))
                        diff = diff.fillna(True)
                    except Exception:
                        diff = x.astype(str) != y.astype(str)
                    if diff.any():
                        mm[col] = int(diff.sum())
                rec['old_common_rows_mismatch_by_column'] = mm
        if 'i' in b_old.index.names:
            bi = set(b_old.index.get_level_values('i'))
            ci = set(c_old.index.get_level_values('i'))
            rec['old_household_ids_equal'] = bi == ci
            rec['old_household_ids_counts'] = [len(bi), len(ci)]
        if NEW in (rc.get('rows_per_wave') or {}):
            c_new = c[c.index.get_level_values('t') == NEW]
            r = {'rows': int(len(c_new)), 'dup_index': int(c_new.index.duplicated().sum())}
            if 'i' in c_new.index.names:
                ii = pd.Index(c_new.index.get_level_values('i')).astype(str)
                r['households'] = int(ii.nunique())
                r['households_cs24'] = int(pd.Index(ii.unique()).str.startswith('cs-24-').sum())
            r['v_present'] = ('v' in c_new.index.names) or ('v' in c_new.columns)
            if 'v' in c_new.columns:
                r['v_null'] = int(c_new['v'].isna().sum())
            elif 'v' in c_new.index.names:
                r['v_null'] = int(pd.isna(c_new.index.get_level_values('v')).sum())
            for col in ('weight', 'panel_weight', 'ihps_weight', 'ihps_hhid', 'Quantity',
                        'Expenditure', 'Quantity_kg', 'Derivation'):
                if col in c_new.columns:
                    r[f'{col}_nonnull'] = int(c_new[col].notna().sum())
            if 'weight' in c_new.columns:
                r['weight_mean'] = float(c_new['weight'].mean())
            if 'panel_weight' in c_new.columns:
                r['panel_weight_mean'] = float(c_new['panel_weight'].mean())
            if 'Derivation' in c_new.columns:
                r['Derivation_counts'] = {str(k): int(v) for k, v in
                                          c_new['Derivation'].value_counts(dropna=False).items()}
            rec['new_wave'] = r
    res['tables'][t] = rec

# derived per wave
for t in ('food_quantities', 'food_prices'):
    res['derived'][t] = {}
    for w in sc['derived_per_wave'].get(t, {}):
        bw = sb['derived_per_wave'].get(t, {}).get(w, {})
        cw = sc['derived_per_wave'][t][w]
        res['derived'][t][w] = {
            'base_rows': bw.get('rows'), 'cand_rows': cw.get('rows'),
            'base_total_kg': bw.get('total_kg'), 'cand_total_kg': cw.get('total_kg'),
            'base_kg_rows': bw.get('kg_rows'), 'cand_kg_rows': cw.get('kg_rows'),
            'base_kg_factor_sources': bw.get('kg_factor_sources'),
            'cand_kg_factor_sources': cw.get('kg_factor_sources'),
            'base_baseline_gate': bw.get('kg_factor_baseline_gate'),
            'cand_baseline_gate': cw.get('kg_factor_baseline_gate'),
            'base_error': bw.get('error_class'), 'cand_error': cw.get('error_class'),
            'base_warnings': bw.get('warnings', {}).get('counts'),
            'cand_warnings': cw.get('warnings', {}).get('counts')}

# value movement on common rows for derived kg tables (old waves)
for t in ('food_quantities', 'food_prices'):
    b, c = drop_new(load(B, t)), drop_new(load(C, t))
    if b is None or c is None:
        continue
    col = b.columns[0]
    inter = b.index.intersection(c.index)
    mv = {'value_column': str(col), 'rows_only_base': int(len(b.index.difference(c.index))),
          'rows_only_cand': int(len(c.index.difference(b.index))), 'rows_common': int(len(inter))}
    if len(inter) and not b.index.duplicated().any() and not c.index.duplicated().any():
        x, y = b.loc[inter, col].astype(float), c.loc[inter, col].astype(float)
        changed = ~(np.isclose(x, y, rtol=0, atol=0, equal_nan=True))
        tt = inter.get_level_values('t')
        mv['changed_common_rows_by_wave'] = {str(w): int(changed[tt == w].sum()) for w in sorted(set(tt))}
        ratio = (y / x)[changed & (x > 0) & (y > 0)]
        if len(ratio):
            mv['changed_ratio_quantiles'] = {str(q): float(ratio.quantile(q)) for q in (0.01, 0.1, 0.5, 0.9, 0.99)}
    if 'u' in b.index.names:
        for side, d in (('base', b), ('cand', c)):
            u = d.index.get_level_values('u')
            tt = d.index.get_level_values('t')
            mv[f'{side}_kg_rows_by_wave'] = {str(w): int(((u == 'kg') & (tt == w)).sum()) for w in sorted(set(tt))}
    res['derived'][t + '_movement'] = mv

# food_expenditures old waves (no kg factor: expected equal)
b, c = drop_new(load(B, 'food_expenditures')), drop_new(load(C, 'food_expenditures'))
if b is not None and c is not None:
    eq, detail = frame_equal(b, c)
    res['derived']['food_expenditures_old_waves_frame_equal'] = eq
    if not eq:
        res['derived']['food_expenditures_old_waves_detail'] = detail

# panel dicts
for prop in ('panel_ids', 'updated_ids'):
    pb, pc = B / f'{prop}.json', C / f'{prop}.json'
    if pb.exists() and pc.exists():
        db, dc = json.loads(pb.read_text()), json.loads(pc.read_text())
        if prop == 'updated_ids':
            res['panel'][prop] = {'base_waves': {k: len(v) for k, v in db.items()},
                                  'cand_waves': {k: len(v) for k, v in dc.items()},
                                  'earlier_waves_identical': all(db[w] == dc.get(w) for w in db)}
        else:
            res['panel'][prop] = {'base': len(db), 'cand': len(dc),
                                  'base_entries_unchanged': all(dc.get(k) == v for k, v in db.items())}

# Feature: Malawi rows equal Country frames
for feat in ('food_acquired', 'sample'):
    fp = load(C, f'feature_{feat}')
    cp = load(C, feat)
    rec = {'feature_ok': sc.get('feature', {}).get(feat, {}).get('ok'),
           'rows_per_country': sc.get('feature', {}).get(feat, {}).get('rows_per_country'),
           'feature_warnings': sc.get('feature', {}).get(feat, {}).get('warnings', {}).get('counts')}
    if fp is not None and cp is not None:
        rec['feature_dup_index'] = int(fp.index.duplicated().sum())
        m = fp.xs('Malawi', level='country')
        rec['feature_malawi_index'] = list(map(str, m.index.names))
        rec['country_index'] = list(map(str, cp.index.names))
        mr, cr = m.reset_index(), cp.reset_index()
        cols = [x for x in cr.columns if x in mr.columns]
        rec['country_columns_missing_in_feature'] = [x for x in cr.columns if x not in mr.columns]
        rec['feature_columns_not_in_country'] = [x for x in mr.columns if x not in cr.columns]
        if rec['feature_columns_not_in_country']:
            rec['feature_extra_nonnull'] = {x: int(mr[x].notna().sum())
                                            for x in rec['feature_columns_not_in_country']}
        mr = mr[cols].sort_values(cols, kind='stable', na_position='last').reset_index(drop=True)
        cr = cr[cols].sort_values(cols, kind='stable', na_position='last').reset_index(drop=True)
        rec['rows'] = [int(len(mr)), int(len(cr))]
        try:
            pd.testing.assert_frame_equal(mr, cr, check_exact=True, check_dtype=False,
                                          check_categorical=False)
            rec['malawi_rows_equal_country'] = True
        except AssertionError as exc:
            rec['malawi_rows_equal_country'] = False
            rec['detail'] = str(exc)[:1500]
            # retry dtype-agnostic via string compare
            try:
                eqs = (mr.astype(str).values == cr.astype(str).values).all()
                rec['malawi_rows_equal_country_as_strings'] = bool(eqs)
            except Exception as e2:
                rec['string_compare_error'] = str(e2)[:300]
    res['feature'][feat] = rec

OUT.write_text(json.dumps(res, indent=1, default=str))
print(json.dumps({t: {k: v for k, v in r.items() if k in (
    'base_rows', 'cand_rows', 'old_waves_frame_equal', 'old_household_ids_equal',
    'cand_extra_columns', 'base_error', 'cand_error')} for t, r in res['tables'].items()},
    indent=1, default=str))
