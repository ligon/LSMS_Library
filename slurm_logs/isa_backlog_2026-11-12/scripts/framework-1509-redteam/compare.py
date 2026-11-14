"""Compare the base and candidate arms.  usage: compare.py OUTROOT  (reads OUTROOT/{base,cand})"""
import os
import sys
import json
import traceback

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

ROOT = sys.argv[1]
B, C = f'{ROOT}/base', f'{ROOT}/cand'
NP, UNP = 'item_unit_nonpurchase', 'unit_nonpurchase'
res = {}


def rd(arm, name, what):
    p = f'{ROOT}/{arm}/{name}_{what}.parquet'
    return pd.read_parquet(p) if os.path.exists(p) else None


def js(arm, name):
    p = f'{ROOT}/{arm}/{name}.json'
    return json.load(open(p)) if os.path.exists(p) else None


def norm(x):
    return T._normalise_join_key(np.asarray(x, dtype=object))


def frame_eq(a, b, idx):
    a = a.set_index(idx).sort_index()
    b = b.set_index(idx).sort_index()
    try:
        pd.testing.assert_frame_equal(a, b, check_dtype=False)
        return True, None
    except AssertionError as e:
        return False, str(e)[:800]


def sec(rec, key, fn):
    try:
        rec[key] = fn()
    except Exception:  # noqa: BLE001
        rec[key] = {'error': traceback.format_exc()[-1500:]}


names = sorted(f[:-5] for f in os.listdir(C) if f.endswith('.json')
               and f[:-5] in {g[:-5] for g in os.listdir(B)} and f != 'feature.json')
for name in names:
    rec = {}
    jb, jc = js('base', name), js('cand', name)
    fab, fac = rd('base', name, 'fa'), rd('cand', name, 'fa')
    kb, kc = rd('base', name, 'kgf'), rd('cand', name, 'kgf')

    def s_fa():
        try:
            pd.testing.assert_frame_equal(fab, fac, check_dtype=False)
            return {'equal': True, 'rows': len(fab)}
        except AssertionError as e:
            return {'equal': False, 'diff': str(e)[:800]}
    sec(rec, 'fa_equal', s_fa)
    fa = fac
    idx = [c for c in ('t', 'v', 'i', 'j', 'u', 's', 'visit') if c in fa.columns]
    s = fa['s'].astype(object).to_numpy()
    u = fa['u'].astype(object).to_numpy()
    j = fa['j'].astype(object).to_numpy()
    t = fa['t'].astype(str).to_numpy()
    E = pd.to_numeric(fa.get('Expenditure'), errors='coerce').to_numpy(dtype=float) \
        if 'Expenditure' in fa else np.full(len(fa), np.nan)
    Q = pd.to_numeric(fa.get('Quantity'), errors='coerce').to_numpy(dtype=float)
    cur = np.asarray(T._is_currency_denominated(pd.Index(u)), dtype=bool)
    sent = T._unit_sentinel_mask(fa.set_index(idx))
    purch = (s == 'purchased')
    priced = np.isfinite(E) & (E > 0) & np.isfinite(Q) & (Q > 0) & ~cur
    nj, nu = norm(j), norm(u)
    cellkey = pd.Series(nj).astype(str) + '\x1f' + pd.Series(nu).astype(str)
    cells_priced = set(cellkey[priced & purch])
    cells_any = set(cellkey[purch])
    in_priced = cellkey.isin(cells_priced).to_numpy()
    in_any = cellkey.isin(cells_any).to_numpy()
    rec['priced_by_s'] = {str(k): int(v) for k, v in
                          pd.Series(s[priced]).value_counts(dropna=False).items()}

    def s_kgf():
        bs, cs = kb['KgFactorSource'].to_numpy(), kc['KgFactorSource'].to_numpy()
        bf, cf = kb['kg_per_unit'].to_numpy(float), kc['kg_per_unit'].to_numpy(float)
        out = {}
        ct = pd.crosstab(pd.Series(bs, name='base'), pd.Series(cs, name='cand'))
        out['transitions'] = {f'{a}->{b}': int(ct.loc[a, b]) for a in ct.index
                              for b in ct.columns if ct.loc[a, b] and a != b}
        same = (np.isnan(bf) & np.isnan(cf)) | np.isclose(bf, cf, rtol=1e-9, atol=0)
        out['rows_factor_changed'] = int((~same).sum())
        out['rows_factor_changed_by_cand_src'] = {
            str(k): int(v) for k, v in pd.Series(cs[~same]).value_counts().items()}
        out['rows_factor_changed_same_src'] = int((~same & (bs == cs)).sum())
        fb = np.isin(cs, (NP, UNP))
        for lay in (NP, UNP):
            m = cs == lay
            r = cf[m] / bf[m]
            out[lay] = {
                'rows': int(m.sum()),
                'by_t': {str(k): int(v) for k, v in pd.Series(t[m]).value_counts().sort_index().items()},
                'by_s': {str(k): int(v) for k, v in pd.Series(s[m]).value_counts().items()},
                'base_src': {str(k): int(v) for k, v in pd.Series(bs[m]).value_counts().items()},
                'cells': int(cellkey[m].nunique()),
                'labels': sorted(set(pd.Series(u[m]).astype(str))),
                'ratio_to_base_median': float(np.nanmedian(r)) if m.any() else None,
                'ratio_min': float(np.nanmin(r)) if m.any() and np.isfinite(r).any() else None,
                'ratio_max': float(np.nanmax(r)) if m.any() and np.isfinite(r).any() else None,
                'exact_eq_base_1e-9': int(np.isclose(cf[m], bf[m], rtol=1e-9).sum()),
                'within_2x': int(((r <= 2) & (r >= 0.5)).sum()),
                'base_nan': int(np.isnan(bf[m]).sum()),
                'size_pooled': int(kc['kg_size_pooled'].to_numpy(bool)[m].sum()),
            }
        # Q2, independently: a fallback row in a cell holding a priced purchase.
        out['Q2_fallback_rows_in_priced_purchase_cell'] = int((fb & in_priced).sum())
        out['Q2_fallback_rows_in_cell_with_any_purchase_row'] = int((fb & in_any).sum())
        out['Q2_fallback_rows_in_any_but_not_priced'] = int((fb & in_any & ~in_priced).sum())
        if 'kg_cell_has_purchase' in kc:
            kp = kc['kg_cell_has_purchase'].to_numpy(bool)
            pooled = kc['kg_size_pooled'].to_numpy(bool) | False
            out['cell_has_purchase_agrees_with_mine'] = int((kp == in_priced).sum())
            out['cell_has_purchase_disagrees'] = int((kp != in_priced).sum())
            out['cell_has_purchase_disagrees_unpooled'] = int(((kp != in_priced) & ~pooled).sum())
        # gated-out purchase cells: they must not reach a fallback
        out['rows_in_priced_cell_served_none'] = int((in_priced & (cs == 'none')).sum())
        out['rows_in_priced_cell_base_inferred_cand_none'] = int(
            (in_priced & (cs == 'none') & np.isin(bs, ('item_unit', 'item_unit_tight', 'unit'))).sum())
        # disclosure arithmetic
        a = jc['kgf_attrs']
        part = a['kg_factor_sources']
        ev = a['kg_factor_evidence']
        fbk = ev['fallback']
        nb = fbk['none_by_reason']
        mine_none = cs == 'none'
        out['ledger'] = {
            'partition_sum_eq_rows': sum(part.values()) == len(fa),
            'partition_eq_my_counts': part == {k: int((cs == k).sum()) for k in part},
            'priced_rows_read': [ev['priced_rows_read'], int((priced & purch).sum())],
            'priced_rows_excluded': [ev['priced_rows_excluded'],
                                     {str(k): int(v) for k, v in pd.Series(
                                         s[priced & ~purch]).value_counts(dropna=False).items()}],
            'fallback_priced_rows_read': [fbk['priced_rows_read'],
                                          {str(k): int(v) for k, v in pd.Series(
                                              s[priced & ~purch]).value_counts(dropna=False).items()}],
            'fallback_rows_served': [fbk['rows_served'], int((cs == NP).sum())],
            'unit_rung_rows_served': [fbk['unit_rung']['rows_served'], int((cs == UNP).sum())],
            'none_by_reason': nb,
            'none_by_reason_sum_eq_none': sum(nb.values()) == int(mine_none.sum()),
            'my_none_currency': int((mine_none & cur).sum()),
            'my_none_missing': int((mine_none & ~cur & sent).sum()),
            'rows_refused_spread': [fbk['rows_refused_spread'],
                                    fbk['refused_spread_to_unit_nonpurchase'],
                                    fbk['refused_spread_to_none']],
            'ran': fbk['ran'],
        }
        return out
    sec(rec, 'kgf', s_kgf)

    def s_counts():
        out = {}
        for lab in ('fq_kgs', 'fq_units', 'food_prices_default',
                    'food_prices_kgprice', 'food_prices_unitvalue',
                    'food_prices_unitprice'):
            a, b = (jb.get(lab) or {}), (jc.get(lab) or {})
            out[lab] = {'rows': [a.get('rows'), b.get('rows')],
                        'rows_by_t': [a.get('rows_by_t'), b.get('rows_by_t')]}
            if lab == 'fq_kgs':
                kb_, kc_ = a.get('kg_total'), b.get('kg_total')
                out[lab]['kg_total'] = [kb_, kc_, (kc_ / kb_ - 1) * 100 if kb_ else None]
                out[lab]['kg_by_t'] = {w: [a['kg_by_t'].get(w), b['kg_by_t'].get(w)]
                                       for w in sorted(set(a.get('kg_by_t', {})) | set(b.get('kg_by_t', {})))}
                out[lab]['attrs_keys_cand'] = sorted((b.get('attrs') or {}).keys())
            if lab.startswith('food_prices') or lab == 'fq_units':
                out[lab]['value_unknown_rows'] = [[a.get('u_value_rows'), a.get('u_unknown_rows')],
                                                  [b.get('u_value_rows'), b.get('u_unknown_rows')]]
                out[lab]['price_eq_1_on_value'] = [a.get('price_eq_1_on_value'), b.get('price_eq_1_on_value')]
                out[lab]['attrs_cand'] = b.get('attrs')
                out[lab]['attrs_keys_base'] = sorted((a.get('attrs') or {}).keys())
        out['calls_errors'] = {'base': {k: v['error'] for k, v in jb['calls'].items() if v['error']},
                               'cand': {k: v['error'] for k, v in jc['calls'].items() if v['error']}}
        out['calls_seconds'] = {k: [jb['calls'].get(k, {}).get('seconds'), v['seconds']]
                                for k, v in jc['calls'].items()}
        out['warnings_cand'] = {k: v['warnings'] for k, v in jc['calls'].items() if v['warnings']}
        out['subset_cand'] = jc.get('subset')
        out['subset_base'] = {w: {k: v for k, v in r.items() if k.endswith('equal')}
                              for w, r in (jb.get('subset') or {}).items()}
        out['unitvalue_1987_88'] = [jb.get('unitvalue_1987_88'), jc.get('unitvalue_1987_88')]
        return out
    sec(rec, 'counts', s_counts)

    def s_unitvalue():
        a, b = rd('base', name, 'food_prices_unitvalue'), rd('cand', name, 'food_prices_unitvalue')
        if a is None or b is None:
            return None
        i2 = [c for c in a.columns if c != 'Price']
        au = a['u'].astype(object).to_numpy()
        drop = (np.asarray(T._is_currency_denominated(pd.Index(au)), dtype=bool)
                | T._unit_sentinel_mask(a.set_index(i2)))
        eq, d = frame_eq(a[~drop], b, i2)
        tally = (jc.get('food_prices_unitvalue') or {}).get('attrs', {}).get('price_rows_dropped')
        exp_cur = int((cur & np.isfinite(E) & (E > 0)).sum())
        exp_unk = int((sent & ~cur & np.isfinite(E) & (E > 0) & np.isfinite(Q) & (Q > 0)).sum())
        return {'cand_eq_base_minus_label_rows': eq, 'diff': d,
                'base_rows_dropped_groups': int(drop.sum()),
                'tally_currency': tally and tally.get('currency'),
                'my_currency_expected_rows': exp_cur,
                'tally_unknown': tally and tally.get('unknown'),
                'my_unknown_priced_rows': exp_unk,
                'cand_value_rows': int(np.asarray(T._is_currency_denominated(
                    pd.Index(b['u'].astype(object))), dtype=bool).sum()),
                'cand_price_eq_1': int((b['Price'] == 1.0).sum()),
                'cand_rows_by_t': {str(k): int(v) for k, v in b['t'].astype(str).value_counts().sort_index().items()}}
    sec(rec, 'unitvalue', s_unitvalue)

    def s_units():
        a, b = rd('base', name, 'fq_units'), rd('cand', name, 'fq_units')
        if a is None or b is None:
            return None
        i2 = [c for c in a.columns if c != 'Quantity']
        drop = T._unit_sentinel_mask(a.set_index(i2))
        eq, d = frame_eq(a[~drop], b, i2)
        tally = (jc.get('fq_units') or {}).get('attrs', {}).get('quantity_rows_dropped')
        q0 = np.where(Q == 0, np.nan, Q)
        return {'cand_eq_base_minus_unknown': eq, 'diff': d,
                'base_unknown_groups': int(drop.sum()),
                'tally': tally,
                'my_unknown_rows_with_qty': int((sent & np.isfinite(q0)).sum())}
    sec(rec, 'units', s_units)

    def s_prices():
        out = {}
        for lab in ('food_prices_default', 'food_prices_kgprice', 'fq_kgs'):
            a, b = rd('base', name, lab), rd('cand', name, lab)
            if a is None or b is None:
                continue
            val = 'Quantity' if lab == 'fq_kgs' else 'Price'
            i2 = [c for c in a.columns if c != val]
            eq, d = frame_eq(a, b, i2)
            out[lab] = {'equal': eq}
            if not eq:
                m = a.set_index(i2)[val].rename('b').to_frame().join(
                    b.set_index(i2)[val].rename('c'), how='outer')
                out[lab]['only_base'] = int(m['c'].isna().sum())
                out[lab]['only_cand'] = int(m['b'].isna().sum())
                both = m.dropna()
                out[lab]['both_changed'] = int((~np.isclose(both['b'], both['c'], rtol=1e-9)).sum())
        return out
    sec(rec, 'frames', s_prices)

    def s_nutrition():
        a, b = rd('base', name, 'nutrition'), rd('cand', name, 'nutrition')
        if a is None or b is None:
            return None
        num = [c for c in a.columns if pd.api.types.is_numeric_dtype(a[c])]
        idxc = [c for c in a.columns if c not in num]
        eq, d = frame_eq(a, b, idxc)
        out = {'equal': eq, 'rows': [len(a), len(b)], 'diff': d}
        if not eq and set(a.columns) == set(b.columns):
            m = a.set_index(idxc).join(b.set_index(idxc), how='outer', rsuffix='_c')
            for col in num[:40]:
                x, y = m[col].to_numpy(float), m[col + '_c'].to_numpy(float)
                ch = ~((np.isnan(x) & np.isnan(y)) | np.isclose(x, y, rtol=1e-9))
                if ch.any():
                    out.setdefault('cols_changed', {})[col] = {
                        'rows_changed': int(ch.sum()),
                        'sum_base': float(np.nansum(x)), 'sum_cand': float(np.nansum(y))}
        return out
    sec(rec, 'nutrition', s_nutrition)

    if name == 'Uganda':
        def s_uganda():
            cs = kc['KgFactorSource'].to_numpy()
            bs = kb['KgFactorSource'].to_numpy()
            m = np.isin(cs, (NP, UNP))
            rows = fa.loc[m, idx + ['Quantity', 'Expenditure']].copy()
            rows['cand_src'] = cs[m]
            rows['base_src'] = bs[m]
            rows['base_f'] = kb['kg_per_unit'].to_numpy()[m]
            rows['cand_f'] = kc['kg_per_unit'].to_numpy()[m]
            labs = sorted(set(pd.Series(u[m]).astype(str)))
            lab_info = {}
            for lb in labs:
                lm = pd.Series(u).astype(str).to_numpy() == lb
                lab_info[lb] = {
                    'rows': int(lm.sum()),
                    'purchased_rows': int((lm & purch).sum()),
                    'priced_purchased_rows': int((lm & purch & priced).sum()),
                    'priced_purchased_items': int(pd.Series(j[lm & purch & priced]).nunique()),
                    'cand_src_on_purchased': {str(k): int(v) for k, v in pd.Series(cs[lm & purch]).value_counts().items()},
                    'base_src_on_purchased': {str(k): int(v) for k, v in pd.Series(bs[lm & purch]).value_counts().items()},
                    'cand_src_all': {str(k): int(v) for k, v in pd.Series(cs[lm]).value_counts().items()},
                    'base_src_all': {str(k): int(v) for k, v in pd.Series(bs[lm]).value_counts().items()},
                    'cand_kg_unit_np_values': sorted({round(float(x), 6) for x in kc['kg_unit_nonpurchase'].to_numpy()[lm] if np.isfinite(x)}),
                }
            return {'rows': rows.astype(str).to_dict('records'), 'labels': lab_info}
        sec(rec, 'uganda_drill', s_uganda)
    res[name] = rec
    print('compared', name, flush=True)

# hash
def s_hash():
    hb = json.load(open(f'{ROOT}/hash/base.json'))
    hc = json.load(open(f'{ROOT}/hash/cand.json'))
    out = {}
    for k in ('tables', 'waves', 'btf'):
        keys = set(hb[k]) | set(hc[k])
        moved = sorted(x for x in keys if hb[k].get(x) != hc[k].get(x))
        out[k] = {'n': len(keys), 'moved': len(moved),
                  'moved_countries': sorted({x.split('/')[0] for x in moved}) if k != 'btf' else moved,
                  'moved_list': moved[:200],
                  'none_both': sum(1 for x in keys if hb[k].get(x) is None and hc[k].get(x) is None)}
    out['errors'] = [len(hb['errors']), len(hc['errors'])]
    out['errors_differ'] = sorted(set(hb['errors']) ^ set(hc['errors']))[:50]
    return out
r = {}
sec(r, 'hash', s_hash)
res['_hash'] = r['hash']
for arm in ('base', 'cand'):
    p = f'{ROOT}/{arm}/feature.json'
    if os.path.exists(p):
        res[f'_feature_{arm}'] = json.load(open(p))
with open(f'{ROOT}/compare.json', 'w') as f:
    json.dump(res, f, indent=1, default=str)
print('compare written')
