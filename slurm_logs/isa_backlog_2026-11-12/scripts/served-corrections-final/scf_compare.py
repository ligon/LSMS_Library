"""Base vs candidate frames, the #1708 revaluation metrics, and the inputs joins.

Usage: python scf_compare.py <frames_base> <frames_cand>
Runs on the CANDIDATE code (its Niger module and registry).  Aggregates only ->
results/compare.json, results/revaluation.json, results/inputs_joins.json.
"""
import collections
import json
import os
import sys
import traceback
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scf_common import RESULTS, guard, load_wave_module  # noqa: E402

B, C = sys.argv[1], sys.argv[2]
guard()
warnings.simplefilter('ignore')
import lsms_library as ll  # noqa: E402

FLAG = 'Niger::food_acquired::computed-last-purchase-quantity'
NEW = 'Niger::food_acquired::flagged-purchase-local-median'


def load(d, tag):
    p = os.path.join(d, tag + '.pkl')
    return pd.read_pickle(p) if os.path.exists(p) else None


def eqcol(a, b):
    an, bn = pd.isna(a).to_numpy(), pd.isna(b).to_numpy()
    try:
        av = pd.to_numeric(a, errors='raise').to_numpy(dtype=float)
        bv = pd.to_numeric(b, errors='raise').to_numpy(dtype=float)
        same = av == bv
    except (ValueError, TypeError):
        same = a.astype(object).astype(str).to_numpy() == b.astype(object).astype(str).to_numpy()
    return (an & bn) | (~an & ~bn & same)


def keyframe(df):
    k = df.index.to_frame(index=False)
    for col in k.columns:
        k[col] = k[col].astype(object).where(k[col].notna(), '__NA__').astype(str)
    k['_dup'] = k.groupby(list(k.columns)).cumcount()
    return k


def merged(b, c):
    kb, kc = keyframe(b), keyframe(c)
    vb = b.reset_index(drop=True).astype(object)
    vc = c.reset_index(drop=True).astype(object)
    vb.columns = [f'{x}__b' for x in vb.columns]
    vc.columns = [f'{x}__c' for x in vc.columns]
    keys = list(kb.columns)
    return pd.concat([kb, vb], axis=1).merge(pd.concat([kc, vc], axis=1), on=keys, how='outer',
                                             indicator=True), keys


def diff(b, c):
    m, keys = merged(b, c)
    o = {'rows': {'base': len(b), 'cand': len(c)},
         'left_only': int((m['_merge'] == 'left_only').sum()),
         'right_only': int((m['_merge'] == 'right_only').sum()),
         'dup_ordinals': int((m['_dup'].astype(int) > 0).sum())}
    both = m[m['_merge'] == 'both']
    anyc = np.zeros(len(both), dtype=bool)
    chg = {}
    for col in sorted(set(b.columns) | set(c.columns)):
        if col not in b.columns or col not in c.columns:
            chg[col] = 'one side only'
            continue
        e = eqcol(both[f'{col}__b'], both[f'{col}__c'])
        if (~e).any():
            chg[col] = int((~e).sum())
            anyc |= ~e
    o['changed_cells'] = chg
    o['rows_changed'] = int(anyc.sum())
    if 't' in keys:
        for name, sel in (('left_only_by_t', m['_merge'] == 'left_only'),
                          ('right_only_by_t', m['_merge'] == 'right_only')):
            o[name] = {str(k): int(v) for k, v in m.loc[sel, 't'].value_counts().items()}
        o['changed_by_t'] = {str(k): int(v) for k, v in both.loc[anyc, 't'].value_counts().items()}
    if 't' in b.index.names:
        per = {}
        tb, tc = b.index.get_level_values('t').astype(str), c.index.get_level_values('t').astype(str)
        for w in sorted(set(tb) | set(tc)):
            try:
                pd.testing.assert_frame_equal(b[tb == w], c[tc == w], check_exact=True)
                per[w] = True
            except AssertionError as e:
                per[w] = str(e).split('\n')[0][:160]
        o['per_wave_assert_frame_equal'] = per
    return o, m


OUT, REV, JOIN = {}, {}, {}
tags = sorted({f[:-4] for f in os.listdir(B) if f.endswith('.pkl')} | {f[:-4] for f in os.listdir(C) if f.endswith('.pkl')})
for tag in tags:
    b, c = load(B, tag), load(C, tag)
    if b is None or c is None:
        OUT[tag] = {'missing': {'base': b is None, 'cand': c is None}}
        continue
    try:
        OUT[tag], _ = diff(b, c)
    except Exception as e:  # noqa: BLE001
        OUT[tag] = {'error': f'{type(e).__name__}: {e}'}
    print(tag, json.dumps(OUT[tag])[:600], flush=True)
json.dump(OUT, open(os.path.join(RESULTS, 'compare.json'), 'w'), indent=1, default=str)

# ---------------------------------------------------------------- Niger #1708 revaluation
try:
    b = load(B, 'Niger__food_acquired')
    c = load(C, 'Niger__food_acquired')
    t = c.index.get_level_values('t').astype(str)
    s = c.index.get_level_values('s').astype(str)
    der = c['Derivation'].astype('string').fillna('')
    has_flag = der.str.contains(FLAG, regex=False).to_numpy()
    has_new = der.str.contains(NEW, regex=False).to_numpy()
    REV['rows_with_flag'] = int(has_flag.sum())
    REV['rows_with_new_key'] = int(has_new.sum())
    REV['flag_equals_new'] = bool((has_flag == has_new).all())
    # the index is unchanged; align base to candidate
    assert b.index.equals(c.index), 'index differs'
    REV['index_equal'] = True
    other_cols = [x for x in c.columns if x not in ('Expenditure', 'Derivation')]
    REV['other_columns_equal'] = {x: bool(eqcol(b[x], c[x]).all()) for x in other_cols}
    e_b = pd.to_numeric(b['Expenditure'], errors='coerce').to_numpy(dtype=float)
    e_c = pd.to_numeric(c['Expenditure'], errors='coerce').to_numpy(dtype=float)
    changed = ~eqcol(b['Expenditure'], c['Expenditure'])
    REV['expenditure_changed_rows'] = int(changed.sum())
    REV['expenditure_changed_outside_new_key'] = int((changed & ~has_new).sum())
    d_b = b['Derivation'].astype('string').fillna('')
    d_c = der
    dchg = (d_b != d_c).to_numpy()
    REV['derivation_changed_rows'] = int(dchg.sum())
    REV['derivation_changed_outside_flag'] = int((dchg & ~has_flag).sum())
    w = (t == '2018-19') & (s == 'purchased')
    tot = {}
    for name, e in (('before', e_b), ('after', e_c)):
        tot[name] = {'wave_purchase_total': float(np.nansum(e[w])),
                     'flagged_rows_total': float(np.nansum(e[w & has_flag])),
                     'flagged_rows_valued': int(np.isfinite(e[w & has_flag]).sum())}
        tot[name]['flagged_share_of_wave'] = tot[name]['flagged_rows_total'] / tot[name]['wave_purchase_total']
    REV['totals_2018_19'] = tot
    REV['other_waves_expenditure_equal'] = bool(eqcol(b['Expenditure'][t != '2018-19'], c['Expenditure'][t != '2018-19']).all())

    # levels and the function, on the candidate's derivation_inputs
    nig = load_wave_module('Niger', '2018-19')
    country = ll.Country('Niger')
    inputs = country.derivation_inputs(NEW, wave='2018-19')
    REV['inputs_rows'] = int(len(inputs))
    REV['inputs_flagged'] = int(inputs['flagged'].sum())
    REV['inputs_columns'] = list(map(str, inputs.columns))
    fn = country.derivations()[NEW].resolve()
    out = fn(inputs)
    REV['function_rows'] = int(len(out))
    prev = pd.Series(e_b[has_new], index=c.index[has_new]).groupby(level=['t', 'i', 'j']).sum(min_count=1)
    prev_valued = prev.reindex(out.index)
    pv = np.isfinite(prev_valued.to_numpy(dtype=float))
    lv = out['level'].astype('string').fillna('NA').to_numpy()
    REV['levels'] = {
        'all': dict(collections.Counter(lv)),
        'previously_valued': dict(collections.Counter(lv[pv])),
        'previously_NA': dict(collections.Counter(lv[~pv])),
    }
    # served reproduction
    served = c.loc[has_new, 'Expenditure'].groupby(level=['t', 'i', 'j']).sum(min_count=1)
    want = out['Expenditure'].groupby(level=['t', 'i', 'j']).sum(min_count=1)
    REV['function_reproduces_served'] = {
        'same_keys': bool(set(served.index) == set(want.index)),
        'max_abs_diff': float(np.nanmax(np.abs(served.sort_index().to_numpy(dtype=float)
                                               - want.reindex(served.sort_index().index).to_numpy(dtype=float)))),
        'na_pattern_equal': bool((served.sort_index().isna().to_numpy()
                                  == want.reindex(served.sort_index().index).isna().to_numpy()).all())}
    # sanity: revalued unit value vs the producer's own unit value s07bq08 / s07bq07a
    flagged_in = inputs[inputs['flagged'].to_numpy(dtype=bool)]
    with np.errstate(divide='ignore', invalid='ignore'):
        prod_uv = (pd.to_numeric(flagged_in['s07bq08'], errors='coerce')
                   / pd.to_numeric(flagged_in['s07bq07a'], errors='coerce'))
    ou = out.join(prod_uv.rename('producer_uv').groupby(level=['t', 'i', 'j']).first(), how='left')
    match_mask = pv & np.isfinite(ou['unit_value'].to_numpy(dtype=float))
    r = ou['unit_value'].to_numpy(dtype=float)[match_mask] / ou['producer_uv'].to_numpy(dtype=float)[match_mask]
    lr = np.abs(np.log(r))

    def qs(x):
        x = x[np.isfinite(x)]
        return {f'p{p}': float(np.percentile(x, p)) for p in (5, 10, 25, 50, 75, 90, 95)} | {'n': int(len(x))}
    REV['revalued_over_producer_unit_value'] = qs(r) | {
        'equal': float(np.mean(np.isclose(r, 1.0, rtol=1e-9))),
        'within_10pct': float(np.mean(lr < np.log(1.1))), 'within_25pct': float(np.mean(lr < np.log(1.25))),
        'within_2x': float(np.mean(lr < np.log(2))),
        'by_level': {lvl: qs(r[lv[match_mask] == lvl]) for lvl in ('cluster', 'region')}}
    # sensitivity: a region floor of 3, and the gate set False
    REV['sensitivity'] = {
        'region_floor_3_rows_NA': int(((out['level'] == 'region') & (out['region_reports'] < 3)).sum() + (out['level'].isna()).sum()),
        'region_rows_by_reports': {k: int(v) for k, v in pd.cut(out.loc[out['level'] == 'region', 'region_reports'], [0, 1, 2, 4, 9, 10**6], labels=['1', '2', '3-4', '5-9', '10+']).value_counts().sort_index().items()},
        'gate_false_rows': int(pv.sum()),
        'gate_false_wave_total': float(np.nansum(e_c[w]) - np.nansum(out['Expenditure'].to_numpy(dtype=float)[~pv])),
    }
    # held-out accuracy of thin region cells (the rule's region branch)
    g = inputs.reset_index(drop=True)
    loc = nig.local_median_unit_values(g)
    REV['region_loo_note'] = 'genuine reports whose cluster cell has < 3 OTHER reports, scored against the region median of the other reports'
    from lsms_library.ehcvm import _unit_key
    q7 = pd.to_numeric(g['s07bq07a'], errors='coerce').to_numpy(dtype=float)
    q8 = pd.to_numeric(g['s07bq08'], errors='coerce').to_numpy(dtype=float)
    lu = g['s07bq07b'].map(_unit_key)
    ls = g['s07bq07c'].map(_unit_key)
    gen = (~g['flagged'].to_numpy(dtype=bool) & (q7 > 0) & np.isfinite(q8) & (q8 > 0)
           & lu.notna().to_numpy() & ls.notna().to_numpy())
    pool = pd.DataFrame({'g': g['grappe'], 'r': g['s00q01'], 'p': g['s07bq01'], 'u': lu, 's': ls,
                         'uv': np.where(gen, q8 / np.where(q7 > 0, q7, np.nan), np.nan)})[gen].copy()
    pool['cl_n'] = pool.groupby(['g', 'p', 'u', 's'])['uv'].transform('size') - 1
    loo = np.full(len(pool), np.nan)
    rn = np.zeros(len(pool), dtype=int)
    vals = pool['uv'].to_numpy()
    for _, idx in pool.groupby(['r', 'p', 'u', 's']).indices.items():
        v = vals[idx]
        n = len(v)
        rn[idx] = n - 1
        if n < 2:
            continue
        for k, pos in enumerate(idx):
            loo[pos] = np.median(np.delete(v, k))
    pool['rg_loo'] = loo
    pool['rg_n'] = rn
    thin = pool['cl_n'] < 3
    res = {}
    for lab, lo, hi in (('1', 1, 1), ('2', 2, 2), ('3-4', 3, 4), ('5-9', 5, 9), ('10+', 10, 10**9)):
        sel = thin & (pool['rg_n'] >= lo) & (pool['rg_n'] <= hi) & np.isfinite(pool['rg_loo'])
        if sel.any():
            e = np.abs(np.log(pool.loc[sel, 'rg_loo'] / pool.loc[sel, 'uv']))
            res[lab] = {'records': int(sel.sum()), 'within_25pct': float((e < np.log(1.25)).mean()),
                        'within_2x': float((e < np.log(2)).mean()), 'median_abs_log': float(e.median())}
    REV['region_loo_by_other_reports'] = res
    REV['loc_level_counts_all_records'] = dict(collections.Counter(loc['level'].astype('string').fillna('NA')))

    # kilograms and prices in every Niger wave
    kg = {}
    for tag in ('Niger__food_quantities', 'Niger__food_quantities__units-units', 'Niger__food_prices',
                'Niger__food_prices__units-unitvalue', 'Niger__food_expenditures'):
        fb, fc = load(B, tag), load(C, tag)
        if fb is None or fc is None:
            continue
        col = fb.columns[0]
        tb, tc = fb.index.get_level_values('t').astype(str), fc.index.get_level_values('t').astype(str)
        kg[tag] = {'column': str(col), 'by_wave': {w: {'base_sum': float(pd.to_numeric(fb[col][tb == w], errors='coerce').sum()),
                                                        'cand_sum': float(pd.to_numeric(fc[col][tc == w], errors='coerce').sum()),
                                                        'rows_base': int((tb == w).sum()), 'rows_cand': int((tc == w).sum())}
                                                    for w in sorted(set(tb) | set(tc))}}
    REV['derived_tables_by_wave'] = kg
except Exception as e:  # noqa: BLE001
    REV['error'] = f'{type(e).__name__}: {e}'
    REV['tb'] = traceback.format_exc()[-3000:]
    print(REV['tb'], flush=True)
json.dump(REV, open(os.path.join(RESULTS, 'revaluation.json'), 'w'), indent=1, default=str)
print('revaluation', json.dumps({k: v for k, v in REV.items() if k not in ('tb',)}, default=str)[:3000], flush=True)

# ---------------------------------------------------------------- inputs joins, every new key
KEYS = [('Mali', 'Mali::food_acquired::rice-ration-block-withheld', '2018-19', 'Mali__food_acquired', ('t', 'i', 'j')),
        ('Senegal', 'Senegal::food_acquired::salt-per-capita-withheld', '2021-22', 'Senegal__food_acquired', ('t', 'i', 'j')),
        ('Senegal', 'Senegal::food_acquired::near-zero-quantity-withheld', '2018-19', 'Senegal__food_acquired', ('t', 'i', 'j')),
        ('Niger', FLAG, '2018-19', 'Niger__food_acquired', ('t', 'i', 'j')),
        ('Niger', NEW, '2018-19', 'Niger__food_acquired', ('t', 'i', 'j')),
        ('Uganda', 'Uganda::sample::ea-same-household', '2009-10', 'Uganda__sample', ('t', 'i'))]
for country, key, wave, tag, lv in KEYS:
    rec = {}
    try:
        c = ll.Country(country)
        rows = c.derivations()[key].rows
        rec['rows_field'] = rows if rows == 0 else str(rows)
        inp = c.derivation_inputs(key, wave=wave)
        rec['inputs_rows'] = int(len(inp))
        frame = load(C, tag)
        der = frame['Derivation'].astype('string').fillna('')
        carry = frame[der.str.contains(key, regex=False).to_numpy()]
        rec['served_rows_with_key'] = int(len(carry))
        sk = set(carry.reset_index().set_index(list(lv)).index)
        ik = set(inp.reset_index().set_index(list(lv)).index)
        rec['served_keys'] = len(sk)
        rec['served_keys_without_inputs'] = len(sk - ik)
        if rows == 0:
            base = load(B, tag)
            bk = set(base.reset_index().set_index(list(lv)).index)
            ck = set(frame.reset_index().set_index(list(lv)).index)
            rec['inputs_keys'] = len(ik)
            rec['inputs_keys_in_base'] = len(ik & bk)
            rec['inputs_keys_still_served'] = len(ik & ck)
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
    JOIN[key] = rec
    print(key, rec, flush=True)
json.dump(JOIN, open(os.path.join(RESULTS, 'inputs_joins.json'), 'w'), indent=1, default=str)
print('done compare', flush=True)
