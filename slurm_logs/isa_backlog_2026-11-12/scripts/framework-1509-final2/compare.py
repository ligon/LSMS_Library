"""Compare the base / head / cand arms.  usage: compare.py OUTROOT
Reads OUTROOT/{base,head,cand}/<C>.json and <C>_rows.parquet; writes
OUTROOT/compare.json and OUTROOT/tables.md.  Pure pandas; no lsms_library."""
import json
import os
import sys
import traceback

import numpy as np
import pandas as pd

ROOT = sys.argv[1]
ARMS = ('base', 'head', 'cand')
NP_, UNP = 'item_unit_nonpurchase', 'unit_nonpurchase'


def js(arm, name):
    p = f'{ROOT}/{arm}/{name}.json'
    return json.load(open(p)) if os.path.exists(p) else None


def rows(arm, name):
    p = f'{ROOT}/{arm}/{name}_rows.parquet'
    return pd.read_parquet(p) if os.path.exists(p) else None


def g(d, *ks, default=None):
    for k in ks:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def pct(a, b):
    return None if not a else 100.0 * (b - a) / a


def kg(r):
    return r['Quantity'].to_numpy(dtype=float) * r['kg_per_unit'].to_numpy(dtype=float)


def transitions(a, b, label_a, label_b):
    sa = a['KgFactorSource'].to_numpy()
    sb = b['KgFactorSource'].to_numpy()
    fa_, fb_ = a['kg_per_unit'].to_numpy(dtype=float), b['kg_per_unit'].to_numpy(dtype=float)
    same_f = np.isclose(fa_, fb_, rtol=1e-9, atol=0, equal_nan=True)
    src_moved = sa != sb
    changed = src_moved | ~same_f
    ka, kb = np.nan_to_num(kg(a)), np.nan_to_num(kg(b))
    out = {'rows_changed_factor_or_source': int(changed.sum()),
           'rows_source_moved': int(src_moved.sum()),
           'rows_factor_changed': int((~same_f).sum()),
           'rows_factor_changed_gt2x': int((~same_f & (np.maximum(fa_ / fb_, fb_ / fa_) > 2)).sum()),
           'kg_a': float(ka.sum()), 'kg_b': float(kb.sum()),
           'kg_delta_on_changed': float((kb - ka)[changed].sum())}
    if src_moved.any():
        t = pd.DataFrame({'a': sa[src_moved], 'b': sb[src_moved],
                          'kg_a': ka[src_moved], 'kg_b': kb[src_moved],
                          't': a['t'].to_numpy()[src_moved]})
        grp = t.groupby(['a', 'b']).agg(rows=('kg_a', 'size'), kg_a=('kg_a', 'sum'),
                                         kg_b=('kg_b', 'sum')).reset_index()
        out['by_transition'] = [
            {f'{label_a}': r.a, f'{label_b}': r.b, 'rows': int(r.rows),
             'kg_a': float(r.kg_a), 'kg_b': float(r.kg_b)} for r in grp.itertuples()]
        out['by_wave'] = {str(k): int(v) for k, v in t['t'].value_counts().sort_index().items()}
        out['by_wave_s'] = {f'{w}|{s}': int(v) for (w, s), v in pd.Series(
            1, index=pd.MultiIndex.from_arrays([a['t'].to_numpy()[src_moved],
                                                a['s'].to_numpy()[src_moved]])
        ).groupby(level=[0, 1]).size().items()}
    return out, changed, src_moved


res = {}
md = ['# #1509 final2: base / head / cand', '']
names = sorted({f[:-5] for arm in ARMS for f in os.listdir(f'{ROOT}/{arm}')
                if f.endswith('.json')})
for name in names:
    rec = {}
    J = {arm: js(arm, name) for arm in ARMS}
    try:
        rec['fatal'] = {arm: bool(J[arm] and J[arm].get('fatal')) for arm in ARMS}
        rec['fa_hash_equal'] = len({g(J[a], 'fa_hash') for a in ARMS}) == 1
        rec['fa_rows'] = g(J['cand'], 'fa_rows')
        for key in ('fq_kgs', 'fq_units', 'food_prices_default', 'food_prices_kgprice',
                    'food_prices_unitvalue', 'food_prices_unitprice'):
            rec[key] = {arm: g(J[arm], key, 'rows') for arm in ARMS}
        rec['kg'] = {arm: g(J[arm], 'fq_kgs', 'kg_total') for arm in ARMS}
        rec['kg_pct_base_cand'] = pct(rec['kg']['base'], rec['kg']['cand'])
        rec['kg_pct_head_cand'] = pct(rec['kg']['head'], rec['kg']['cand'])
        rec['kg_by_t'] = {arm: g(J[arm], 'fq_kgs', 'kg_by_t') for arm in ARMS}
        rec['kgf_total_kg'] = {arm: g(J[arm], 'kgf_total_kg') for arm in ARMS}
        rec['sources'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_sources') for arm in ARMS}
        rec['sources_by_t'] = {arm: g(J[arm], 'kgf_sources_by_t') for arm in ARMS}
        rec['fallback'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_evidence', 'fallback')
                           for arm in ARMS}
        rec['baseline_gate'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_baseline_gate')
                                for arm in ARMS}
        rec['wave_spread'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_wave_spread')
                              for arm in ARMS}
        rec['size_pool'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_size_pool')
                            for arm in ARMS}
        rec['declared_none'] = {arm: g(J[arm], 'kgf_attrs', 'kg_factor_declared_none')
                                for arm in ARMS}
        rec['price_hash_equal'] = {
            key: {'base_cand': g(J['base'], key, 'price_hash') == g(J['cand'], key, 'price_hash'),
                  'head_cand': g(J['head'], key, 'price_hash') == g(J['cand'], key, 'price_hash')}
            for key in ('food_prices_default', 'food_prices_kgprice',
                        'food_prices_unitvalue', 'food_prices_unitprice')}
        rec['item_and_np_both_set'] = {arm: g(J[arm], 'item_and_np_both_set') for arm in ARMS}
        rec['subset'] = {arm: g(J[arm], 'subset') for arm in ARMS}
        rec['subset_all_equal'] = {
            arm: (None if not g(J[arm], 'subset') else all(
                v.get('equal') for w in J[arm]['subset'].values() for v in w.values()))
            for arm in ARMS}
        rec['nutrition'] = {arm: g(J[arm], 'nutrition') for arm in ARMS}
        if rec['nutrition']['cand']:
            tb = (rec['nutrition']['base'] or {}).get('totals') or {}
            th = (rec['nutrition']['head'] or {}).get('totals') or {}
            tc = rec['nutrition']['cand']['totals']
            rec['nutrition_pct'] = {k: {'base_cand': pct(tb.get(k), v),
                                        'head_cand': pct(th.get(k), v)} for k, v in tc.items()}
        rec['timing'] = {arm: {k: v.get('seconds') for k, v in (g(J[arm], 'calls') or {}).items()
                               if not k.startswith(('fq_kgs_wave', 'fp_default_wave'))}
                         for arm in ARMS}
        R = {arm: rows(arm, name) for arm in ARMS}
        if all(R[a] is not None for a in ARMS) and rec['fa_hash_equal']:
            b, h, c = R['base'], R['head'], R['cand']
            rec['head_cand'], changed_hc, moved_hc = transitions(h, c, 'head', 'cand')
            rec['base_cand'], _, _ = transitions(b, c, 'base', 'cand')
            rec['base_head'], _, _ = transitions(b, h, 'base', 'head')
            if 'kg_cell_has_purchase' in c.columns:
                purch = c['kg_cell_has_purchase'].astype(bool).to_numpy()
                rec['head_cand']['purchase_cell_rows_changed'] = int((changed_hc & purch).sum())
                np_rows = np.isin(c['KgFactorSource'].to_numpy(), (NP_, UNP))
                rec['cand_fallback_rows_on_purchase_cells'] = int((np_rows & purch).sum())
                # recount: priced purchase rows in each (j, u) cell, any wave
                e = c['Expenditure'].to_numpy(dtype=float)
                q = c['Quantity'].to_numpy(dtype=float)
                pr = (c['s'].to_numpy() == 'purchased') & np.isfinite(e) & (e > 0) \
                    & np.isfinite(q) & (q > 0)
                cell = pd.Series(c['j'].str.strip().str.lower() + '\x1f'
                                 + c['u'].str.strip().str.lower())
                cells_with_purchase = set(cell[pr])
                any_purchase_cells = set(cell[c['s'].to_numpy() == 'purchased'])
                rec['cand_fallback_rows_in_cells_with_any_purchased_row'] = int(
                    (np_rows & cell.isin(any_purchase_cells).to_numpy()).sum())
                rec['cand_fallback_rows_in_cells_with_priced_purchase_recount'] = int(
                    (np_rows & cell.isin(cells_with_purchase).to_numpy()).sum())
            if moved_hc.any():
                fb_ = b['kg_per_unit'].to_numpy(dtype=float)[moved_hc]
                fc_ = c['kg_per_unit'].to_numpy(dtype=float)[moved_hc]
                rec['head_cand']['moved_cand_factor_equals_base'] = float(
                    np.isclose(fb_, fc_, rtol=1e-9, atol=0, equal_nan=True).mean())
                rec['head_cand']['moved_kg_base'] = float(np.nansum(kg(b)[moved_hc]))
                rec['head_cand']['moved_kg_head'] = float(np.nansum(kg(h)[moved_hc]))
                rec['head_cand']['moved_kg_cand'] = float(np.nansum(kg(c)[moved_hc]))
                rec['head_cand']['moved_cells'] = int(pd.DataFrame(
                    {'j': c['j'].to_numpy()[moved_hc], 'u': c['u'].to_numpy()[moved_hc]}
                ).drop_duplicates().shape[0])
                rec['head_cand']['moved_ratio_cand_over_head_median'] = float(np.nanmedian(
                    fc_ / h['kg_per_unit'].to_numpy(dtype=float)[moved_hc]))
                top = pd.DataFrame({'u': c['u'].to_numpy()[moved_hc], 'one': 1}).groupby('u').size()
                rec['head_cand']['moved_by_u_top'] = {str(k): int(v) for k, v in
                                                      top.sort_values(ascending=False).head(12).items()}
    except Exception:  # noqa: BLE001
        rec['error'] = traceback.format_exc()[-3000:]
    res[name] = rec
    md.append(f'## {name}')
    md.append('')
    md.append(f"fa_hash equal across arms: {rec.get('fa_hash_equal')}; fa rows {rec.get('fa_rows')}")
    md.append('')
    md.append('| measure | base | head | cand |')
    md.append('|---|---|---|---|')
    for key in ('kg', 'fq_kgs', 'fq_units', 'food_prices_default', 'food_prices_kgprice',
                'food_prices_unitvalue', 'food_prices_unitprice'):
        v = rec.get(key) or {}
        md.append(f"| {key} | {v.get('base')} | {v.get('head')} | {v.get('cand')} |")
    md.append('')
    srcs = rec.get('sources') or {}
    layers = sorted({k for a in ARMS for k in (srcs.get(a) or {})})
    if layers:
        md.append('| rung | base | head | cand |')
        md.append('|---|---|---|---|')
        for L in layers:
            md.append(f"| {L} | {(srcs.get('base') or {}).get(L)} | "
                      f"{(srcs.get('head') or {}).get(L)} | {(srcs.get('cand') or {}).get(L)} |")
        md.append('')
    for key in ('head_cand', 'base_cand'):
        if key in rec:
            md.append(f'`{key}`: ' + json.dumps({k: v for k, v in rec[key].items()
                                                if k not in ('by_wave_s',)}, default=str))
            md.append('')
    if rec.get('nutrition_pct'):
        md.append('nutrition % (cand vs base / vs head): ' + json.dumps(
            {k: [None if v['base_cand'] is None else round(v['base_cand'], 3),
                 None if v['head_cand'] is None else round(v['head_cand'], 3)]
             for k, v in rec['nutrition_pct'].items()}))
        md.append('')
    if rec.get('error'):
        md.append('ERROR: ' + rec['error'])
        md.append('')
json.dump(res, open(f'{ROOT}/compare.json', 'w'), indent=1, default=str, sort_keys=True)
open(f'{ROOT}/tables.md', 'w').write('\n'.join(md) + '\n')
print('compare done', len(names))
