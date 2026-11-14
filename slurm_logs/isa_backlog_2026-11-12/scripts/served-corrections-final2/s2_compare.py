"""Niger: development (base) vs the previous branch head (prev) vs this candidate (cand).

Usage: python s2_compare.py <frames_base> <frames_prev> <frames_cand>
Runs on the CANDIDATE code (its Niger module and registry), in the candidate's warm
private root.  Aggregates only -> results/compare.json and results/revaluation.json.
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
from s2_common import RESULTS, guard, load_wave_module  # noqa: E402

B, P, C = sys.argv[1], sys.argv[2], sys.argv[3]
guard()
warnings.simplefilter('ignore')
import lsms_library as ll  # noqa: E402

N876 = 'Niger::food_acquired::7day-purchase-at-last-purchase-unit-value'
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


def diff(b, c):
    kb, kc = keyframe(b), keyframe(c)
    vb = b.reset_index(drop=True).astype(object)
    vc = c.reset_index(drop=True).astype(object)
    vb.columns = [f'{x}__b' for x in vb.columns]
    vc.columns = [f'{x}__c' for x in vc.columns]
    keys = list(kb.columns)
    m = pd.concat([kb, vb], axis=1).merge(pd.concat([kc, vc], axis=1), on=keys, how='outer', indicator=True)
    o = {'rows': {'left': len(b), 'right': len(c)},
         'left_only': int((m['_merge'] == 'left_only').sum()),
         'right_only': int((m['_merge'] == 'right_only').sum())}
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
    per = {}
    tb, tc = b.index.get_level_values('t').astype(str), c.index.get_level_values('t').astype(str)
    for w in sorted(set(tb) | set(tc)):
        try:
            pd.testing.assert_frame_equal(b[tb == w], c[tc == w], check_exact=True)
            per[w] = True
        except AssertionError as e:
            per[w] = str(e).split('\n')[0][:160]
    o['per_wave_assert_frame_equal'] = per
    col = c.columns[0]
    o['sum_by_wave'] = {w: {'left': float(pd.to_numeric(b[col][tb == w], errors='coerce').sum()),
                            'right': float(pd.to_numeric(c[col][tc == w], errors='coerce').sum())}
                        for w in sorted(set(tb) | set(tc))} if len(c.columns) == 1 else None
    return o


OUT, REV = {}, {}
tags = sorted({f[:-4] for f in os.listdir(C) if f.endswith('.pkl')})
for tag in tags:
    rec = {}
    c = load(C, tag)
    for name, d in (('cand_vs_base', B), ('cand_vs_prev', P)):
        o = load(d, tag)
        if o is None:
            rec[name] = 'missing'
            continue
        try:
            rec[name] = diff(o, c)
        except Exception as e:  # noqa: BLE001
            rec[name] = {'error': f'{type(e).__name__}: {e}'}
    OUT[tag] = rec
    print(tag, json.dumps(rec, default=str)[:900], flush=True)
json.dump(OUT, open(os.path.join(RESULTS, 'compare.json'), 'w'), indent=1, default=str)


def keyset(series):
    return [frozenset(x.split('+')) if isinstance(x, str) and x else frozenset() for x in series.astype(object)]


try:
    b = load(B, 'Niger__food_acquired')
    p = load(P, 'Niger__food_acquired')
    c = load(C, 'Niger__food_acquired')
    REV['index_equal'] = {'base': bool(b.index.equals(c.index)), 'prev': bool(p.index.equals(c.index))}
    assert b.index.equals(c.index) and p.index.equals(c.index)
    t = c.index.get_level_values('t').astype(str)
    s = c.index.get_level_values('s').astype(str)
    w = (t == '2018-19') & (s == 'purchased')
    dc = c['Derivation'].astype('string').fillna('')
    has_flag = dc.str.contains(FLAG, regex=False).to_numpy()
    has_new = dc.str.contains(NEW, regex=False).to_numpy()
    e = {k: pd.to_numeric(f['Expenditure'], errors='coerce').to_numpy(dtype=float)
         for k, f in (('base', b), ('prev', p), ('cand', c))}
    REV['rows'] = {'flag': int(has_flag.sum()), 'new_key': int(has_new.sum()),
                   'new_key_outside_flag': int((has_new & ~has_flag).sum()),
                   'flag_outside_2018_19_purchased': int((has_flag & ~w).sum())}
    base_valued = np.isfinite(e['base'])
    REV['scope'] = {
        'new_key_equals_flag_and_base_valued': bool((has_new == (has_flag & base_valued)).all()),
        'flagged_base_valued': int((has_flag & base_valued).sum()),
        'flagged_base_NA': int((has_flag & ~base_valued).sum()),
        'flagged_base_zero': int((has_flag & (e['base'] == 0)).sum()),
    }
    left = has_flag & ~has_new
    kb, kc, kp = keyset(b['Derivation']), keyset(c['Derivation']), keyset(p['Derivation'])
    REV['left_NA_rows'] = {
        'rows': int(left.sum()),
        'cand_expenditure_all_NA': bool(np.isnan(e['cand'][left]).all()),
        'base_expenditure_all_NA': bool(np.isnan(e['base'][left]).all()),
        'derivation_is_base_plus_flag_only': bool(all(kc[i] == kb[i] | {FLAG} for i in np.flatnonzero(left))),
        'prev_had_new_key': int(sum(NEW in kp[i] for i in np.flatnonzero(left))),
        'prev_valued': int(np.isfinite(e['prev'][left]).sum()),
    }
    REV['new_rows_derivation_is_base_plus_flag_plus_new'] = bool(
        all(kc[i] == kb[i] | {FLAG, NEW} for i in np.flatnonzero(has_new)))
    other = ~(has_flag | has_new)
    REV['unflagged_rows_derivation_equal_base'] = bool(all(kc[i] == kb[i] for i in np.flatnonzero(other)))
    REV['other_columns_equal_base'] = {x: bool(eqcol(b[x], c[x]).all()) for x in c.columns
                                       if x not in ('Expenditure', 'Derivation')}
    chg_b = ~eqcol(b['Expenditure'], c['Expenditure'])
    chg_p = ~eqcol(p['Expenditure'], c['Expenditure'])
    REV['expenditure_changed'] = {
        'vs_base_rows': int(chg_b.sum()), 'vs_base_outside_new_key': int((chg_b & ~has_new).sum()),
        'vs_prev_rows': int(chg_p.sum()), 'vs_prev_outside_flag': int((chg_p & ~has_flag).sum()),
        'vs_prev_on_left_NA_rows': int((chg_p & left).sum()),
    }
    # among the revalued rows, against the previous head (region floor 1)
    cf, pf = np.isfinite(e['cand']), np.isfinite(e['prev'])
    sel = has_new
    REV['revalued_vs_prev'] = {
        'both_valued': int((sel & cf & pf).sum()),
        'both_valued_identical': int((sel & cf & pf & (e['cand'] == e['prev'])).sum()),
        'cand_NA_prev_valued': int((sel & ~cf & pf).sum()),
        'cand_valued_prev_NA': int((sel & cf & ~pf).sum()),
        'both_NA': int((sel & ~cf & ~pf).sum()),
    }
    tot = {}
    for name in ('base', 'prev', 'cand'):
        x = e[name]
        tot[name] = {'wave_purchase_total': float(np.nansum(x[w])),
                     'flagged_rows_total': float(np.nansum(x[w & has_flag])),
                     'flagged_rows_valued': int(np.isfinite(x[w & has_flag]).sum()),
                     'flagged_rows_NA': int(np.isnan(x[w & has_flag]).sum())}
        tot[name]['flagged_share_of_wave'] = tot[name]['flagged_rows_total'] / tot[name]['wave_purchase_total']
    REV['totals_2018_19'] = tot
    # served ratio cand / base on rows valued both before and after = revalued unit value
    # over the producer's own s07bq08 / s07bq07a (same quantity on both sides)
    both = has_new & cf & base_valued & (e['base'] > 0)
    r = e['cand'][both] / e['base'][both]

    def qs(x):
        x = x[np.isfinite(x)]
        lr = np.abs(np.log(x))
        return {f'p{q}': float(np.percentile(x, q)) for q in (5, 10, 25, 50, 75, 90, 95)} | {
            'n': int(len(x)), 'equal': float(np.mean(np.isclose(x, 1.0, rtol=1e-9))),
            'within_10pct': float(np.mean(lr < np.log(1.1))), 'within_25pct': float(np.mean(lr < np.log(1.25))),
            'within_2x': float(np.mean(lr < np.log(2)))}
    REV['served_ratio_cand_over_base'] = qs(r)

    # the registered function, on derivation_inputs
    nig = load_wave_module('Niger', '2018-19')
    country = ll.Country('Niger')
    inputs = country.derivation_inputs(NEW, wave='2018-19')
    REV['inputs'] = {'rows': int(len(inputs)), 'flagged': int(inputs['flagged'].sum()),
                     'columns': list(map(str, inputs.columns))}
    fn = country.derivations()[NEW].resolve()
    out = fn(inputs)
    lv = out['level'].astype('string').fillna('NA').to_numpy()
    REV['function'] = {'rows': int(len(out)), 'levels': dict(collections.Counter(lv)),
                       'NA_by_region_reports': {str(k): int(v) for k, v in
                                                out.loc[lv == 'NA', 'region_reports'].value_counts().sort_index().items()},
                       'NA_by_cluster_reports': {str(k): int(v) for k, v in
                                                 out.loc[lv == 'NA', 'cluster_reports'].value_counts().sort_index().items()}}
    served = c.loc[has_new, 'Expenditure'].groupby(level=['t', 'i', 'j']).sum(min_count=1).sort_index()
    want = out['Expenditure'].groupby(level=['t', 'i', 'j']).sum(min_count=1).reindex(served.index)
    sv, wv = served.to_numpy(dtype=float), want.to_numpy(dtype=float)
    REV['function_reproduces_served'] = {
        'served_keys': int(len(served)),
        'same_keys': bool(set(served.index) == set(out['Expenditure'].groupby(level=['t', 'i', 'j']).sum(min_count=1).index)),
        'na_pattern_equal': bool((np.isnan(sv) == np.isnan(wv)).all()),
        'max_abs_diff': float(np.nanmax(np.abs(sv - wv))) if np.isfinite(sv).any() else None,
        'max_rel_diff': float(np.nanmax(np.abs(sv - wv) / np.abs(wv))) if np.isfinite(sv).any() else None}
    # inputs join: every served (t, i, j) carrying the key finds an input record
    ik = set(inputs.index)
    sk = set(c[has_new].reset_index().set_index(['t', 'i', 'j']).index)
    fk = set(c[has_flag].reset_index().set_index(['t', 'i', 'j']).index)
    REV['inputs_join'] = {'new_key_served_keys': len(sk), 'without_inputs': len(sk - ik),
                          'flag_served_keys': len(fk)}
    # positional, on the function's own frame: revalued unit value / producer unit value, by level
    g = inputs.reset_index(drop=True)
    hit = nig._revalued_records(g)
    loc = nig.local_median_unit_values(g)
    with np.errstate(divide='ignore', invalid='ignore'):
        prod = (pd.to_numeric(g['s07bq08'], errors='coerce') / pd.to_numeric(g['s07bq07a'], errors='coerce')).to_numpy(dtype=float)
    uv = loc['unit_value'].to_numpy(dtype=float)
    lvl = loc['level'].astype('string').fillna('NA').to_numpy()
    ok = hit & np.isfinite(uv) & np.isfinite(prod) & (prod > 0)
    rr = uv[ok] / prod[ok]
    REV['unit_value_over_producer'] = qs(rr) | {
        'by_level': {L: qs(rr[lvl[ok] == L]) for L in ('cluster', 'region')}}
    REV['positional_hit_rows'] = int(hit.sum())
    # what the region floor of 3 took away inside this scope (would be valued at floor 1)
    rg = loc['region_reports'].to_numpy()
    cl = loc['cluster_reports'].to_numpy()
    REV['floor_effect_in_scope'] = {
        'NA_with_region_reports_1_or_2': int((hit & (lvl == 'NA') & (rg >= 1) & (rg <= 2)).sum()),
        'NA_with_no_region_report': int((hit & (lvl == 'NA') & (rg == 0)).sum()),
        'cluster_rows': int((hit & (lvl == 'cluster')).sum()),
        'region_rows': int((hit & (lvl == 'region')).sum()),
        'region_rows_by_reports': {k: int(v) for k, v in pd.cut(pd.Series(rg[hit & (lvl == 'region')]),
                                   [2, 4, 9, 10**6], labels=['3-4', '5-9', '10+']).value_counts().sort_index().items()},
        'cluster_rows_with_cluster_reports_ge3': int((hit & (lvl == 'cluster') & (cl >= 3)).sum()),
    }
    # cells containing a non-integer s07bq08 among the genuine reports (the residual)
    q8 = pd.to_numeric(g['s07bq08'], errors='coerce').to_numpy(dtype=float)
    q7 = pd.to_numeric(g['s07bq07a'], errors='coerce').to_numpy(dtype=float)
    gen = (~g['flagged'].to_numpy(dtype=bool) & (q7 > 0) & np.isfinite(q8) & (q8 > 0))
    REV['genuine_non_integer_s07bq08'] = int((gen & (np.abs(q8 - np.round(q8)) > 1e-9)).sum())
except Exception as ex:  # noqa: BLE001
    REV['error'] = f'{type(ex).__name__}: {ex}'
    REV['tb'] = traceback.format_exc()[-3000:]
    print(REV['tb'], flush=True)
json.dump(REV, open(os.path.join(RESULTS, 'revaluation.json'), 'w'), indent=1, default=str)
print('revaluation', json.dumps({k: v for k, v in REV.items() if k != 'tb'}, default=str)[:4000], flush=True)
print('done compare', flush=True)
