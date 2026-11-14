"""Compare pickled frames: base vs cand (every table), cand vs make (crop_production).

usage: compare.py <clone(cand)> <frames_dir> <out_json>
Run with the cand library on PYTHONPATH (transformations.py is identical on both sides).
"""
import glob
import os
import sys
import traceback

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import dump  # noqa: E402

clone, frdir, out_json = sys.argv[1:4]
NEW = 'Quantity_sold_kg'
NEW_KEYS = {'Niger::crop_production::sold-kg-estimate',
            'Niger::crop_production::dry-season-sold-kg-no-sale',
            'Mali::crop_production::sold-kg-estimate'}
res = {'base_vs_cand': {}, 'cand_vs_make': {}, 'stats': {}, 'transforms': {}}


def keyset(x):
    if x is None or (not isinstance(x, str) and pd.isna(x)):
        return frozenset()
    return frozenset(k for k in str(x).split('+') if k)


def attrs_diff(a, b, skip=()):
    out = {}
    for k in sorted(set(a.attrs) | set(b.attrs)):
        if k in skip:
            continue
        if repr(a.attrs.get(k)) != repr(b.attrs.get(k)):
            out[k] = 'differs'
    return out


def cmp_with_new_column(b, c):
    r = {}
    r['rows'] = [len(b), len(c)]
    r['index_equal'] = bool(b.index.equals(c.index))
    r['index_names_equal'] = list(b.index.names) == list(c.index.names)
    added = [x for x in c.columns if x not in b.columns]
    removed = [x for x in b.columns if x not in c.columns]
    r['added_cols'], r['removed_cols'] = added, removed
    r['col_order_ok'] = [x for x in c.columns if x != NEW] == list(b.columns)
    b2 = b.drop(columns=['Derivation'], errors='ignore')
    c2 = c.drop(columns=[NEW, 'Derivation'], errors='ignore')
    try:
        pd.testing.assert_frame_equal(b2, c2, check_exact=True)
        r['frame_equal_minus_new_and_derivation'] = True
    except AssertionError as e:
        r['frame_equal_minus_new_and_derivation'] = False
        r['frame_diff'] = str(e)[:800]
    if 'Derivation' in c.columns and r['index_equal']:
        bd = b['Derivation'] if 'Derivation' in b.columns else pd.Series(pd.NA, index=b.index)
        bs = bd.map(keyset)
        cs = c['Derivation'].map(keyset)
        removed_any = int(sum(1 for x, y in zip(bs, cs) if not x <= y))
        added = [y - x for x, y in zip(bs, cs)]
        added_counts = {}
        for a in added:
            for k in a:
                added_counts[k] = added_counts.get(k, 0) + 1
        r['derivation_rows_with_removed_key'] = removed_any
        r['derivation_added_key_counts'] = added_counts
        r['derivation_added_only_new_keys'] = set(added_counts) <= NEW_KEYS
        r['derivation_rows_changed'] = int(sum(1 for a in added if a))
    r['attrs_diff'] = attrs_diff(b, c)
    return r


def kg_stats(df, country):
    d = df.reset_index()
    out = {}
    if NEW not in d.columns:
        return {'has_column': False}
    kg = pd.to_numeric(d[NEW], errors='coerce').astype('float64')
    q = pd.to_numeric(d['Quantity_sold'], errors='coerce').astype('float64')
    v = pd.to_numeric(d['Value_sold'], errors='coerce').astype('float64')
    der = d['Derivation'].map(keyset) if 'Derivation' in d.columns else pd.Series([frozenset()] * len(d))
    sale = q.fillna(0).gt(0) | v.fillna(0).gt(0)
    farm = d['plot_id'].astype('string').eq('Farm-level').fillna(False)
    est = f'{country}::crop_production::sold-kg-estimate'
    nosale = 'Niger::crop_production::dry-season-sold-kg-no-sale'
    for t, g in d.groupby('t'):
        idx = g.index
        k = kg.loc[idx]
        e = der.loc[idx].map(lambda s: est in s).astype(bool)
        out[str(t)] = dict(
            rows=int(len(g)), kg_notna=int(k.notna().sum()), kg_pos=int(k.gt(0).sum()),
            kg_zero=int(k.eq(0).sum()), kg_sum=float(k.sum()), kg_max=float(k.max()) if k.notna().any() else None,
            kg_on_farm_level=int((k.notna() & farm.loc[idx]).sum()),
            kg_on_nonsale=int((k.notna() & ~sale.loc[idx]).sum()),
            kg_zero_beside_pos_q=int((k.eq(0) & q.loc[idx].gt(0)).sum()),
            kg_coded=int(k.isin([9999, 99999, 999999]).sum()),
            est_key_rows=int(e.sum()), est_key_eq_kg_notna=bool((e == k.notna()).all()),
            est_key_farm=int((e & farm.loc[idx]).sum()),
            nosale_key_rows=int(der.loc[idx].map(lambda s: nosale in s).sum()),
            sale_rows=int(sale.loc[idx].sum()))
        if 'KgFactor_sold' in d.columns:
            out[str(t)]['kgfactor_sold_notna'] = int(d.loc[idx, 'KgFactor_sold'].notna().sum())
    # (t,i,crop) carrying kg on more than one row
    cropcol = 'crop' if 'crop' in d.columns else 'j'
    dup = d[kg.notna()].groupby(['t', 'i', cropcol]).size()
    out['tic_with_kg_on_gt1_row_by_t'] = {str(k): int(v) for k, v in
                                          (dup[dup > 1].groupby(level='t').size()).items()}
    return out


for side_pair, key in ((('base', 'cand'), 'base_vs_cand'), (('cand', 'make'), 'cand_vs_make')):
    a, b = side_pair
    for pa in sorted(glob.glob(os.path.join(frdir, a, '*.pkl'))):
        name = os.path.basename(pa)[:-4]
        pb = os.path.join(frdir, b, name + '.pkl')
        if not os.path.exists(pb):
            continue
        try:
            fa, fb = pd.read_pickle(pa), pd.read_pickle(pb)
            if key == 'base_vs_cand' and (name.endswith('__crop_production')):
                r = cmp_with_new_column(fa, fb)
            else:
                r = {'rows': [len(fa), len(fb)]}
                try:
                    pd.testing.assert_frame_equal(fa, fb, check_exact=True)
                    r['equal'] = True
                except AssertionError as e:
                    r['equal'] = False
                    r['diff'] = str(e)[:800]
                r['attrs_diff'] = attrs_diff(fa, fb)
        except Exception as e:  # noqa: BLE001
            r = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1200:]}
        res[key][name] = r
        print(key, name, {k: v for k, v in r.items() if k not in ('frame_diff', 'diff', 'tb')}, flush=True)
    dump(res, out_json)

for country in ('Niger', 'Mali', 'Uganda'):
    p = os.path.join(frdir, 'cand', f'{country}__crop_production.pkl')
    if os.path.exists(p):
        res['stats'][country] = kg_stats(pd.read_pickle(p), country)
dump(res, out_json)

# harvest_kg / gross_crop_revenue, both sides, cand library
from lsms_library import transformations as T  # noqa: E402
for country in ('Niger', 'Mali', 'Uganda'):
    pa = os.path.join(frdir, 'base', f'{country}__crop_production.pkl')
    pb = os.path.join(frdir, 'cand', f'{country}__crop_production.pkl')
    if not (os.path.exists(pa) and os.path.exists(pb)):
        continue
    fa, fb = pd.read_pickle(pa), pd.read_pickle(pb)
    for fn in ('harvest_kg', 'gross_crop_revenue'):
        try:
            ra, rb = getattr(T, fn)(fa), getattr(T, fn)(fb)
            try:
                if isinstance(ra, pd.DataFrame):
                    pd.testing.assert_frame_equal(ra, rb, check_exact=True)
                else:
                    pd.testing.assert_series_equal(ra, rb, check_exact=True)
                ok = True
                diff = None
            except AssertionError as e:
                ok, diff = False, str(e)[:600]
            res['transforms'][f'{country}__{fn}'] = dict(equal=ok, diff=diff, rows=[len(ra), len(rb)])
        except Exception as e:  # noqa: BLE001
            res['transforms'][f'{country}__{fn}'] = dict(error=f'{type(e).__name__}: {str(e)[:300]}')
        print('transform', country, fn, res['transforms'][f'{country}__{fn}'], flush=True)

# Feature
pa = os.path.join(frdir, 'base', 'FEATURE__crop_production.pkl')
pb = os.path.join(frdir, 'cand', 'FEATURE__crop_production.pkl')
if os.path.exists(pa) and os.path.exists(pb):
    fa, fb = pd.read_pickle(pa), pd.read_pickle(pb)
    r = cmp_with_new_column(fa, fb)
    if NEW in fb.columns:
        cc = fb.index.get_level_values('country')
        r['uganda_kg_all_na'] = bool(fb.loc[cc == 'Uganda', NEW].isna().all())
        r['kg_dtype'] = str(fb[NEW].dtype)
    res['feature_base_vs_cand'] = r
    print('feature', {k: v for k, v in r.items() if k != 'frame_diff'}, flush=True)
dump(res, out_json)
print('COMPARE DONE', flush=True)
