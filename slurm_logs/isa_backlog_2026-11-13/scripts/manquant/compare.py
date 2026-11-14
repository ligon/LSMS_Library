"""Base vs candidate comparison on saved frames.  Usage: python compare.py <base_dir> <cand_dir> <out.json>"""
import json, sys
import numpy as np
import pandas as pd

bdir, cdir, outp = sys.argv[1:4]
SEN_RELABEL = {'Maïs en grain': 'Maïs', 'Niébé/Haricots secs': 'Niébé'}
out = {}

def load(d, name):
    return pd.read_parquet(f'{d}/{name}.parquet')

def cells_differ(a, b):
    """Count differing cells per column (NA == NA)."""
    res = {}
    for col in sorted(set(a.columns) | set(b.columns)):
        if col not in a.columns or col not in b.columns:
            res[col] = 'missing on one side'
            continue
        x, y = a[col], b[col]
        both_na = x.isna() & y.isna()
        try:
            eq = (x.astype(object) == y.astype(object)).fillna(False)
        except Exception:
            eq = pd.Series(False, index=x.index)
        diff = ~(eq | both_na)
        res[col] = int(diff.sum())
    return res

def frame_equal(a, b):
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-12, atol=0)
        return True
    except AssertionError as e:
        return str(e)[:600]

for c in ['Togo', 'Senegal', 'CotedIvoire']:
    rec = out[c] = {}
    b = load(bdir, f'{c}__plot_inputs')
    k = load(cdir, f'{c}__plot_inputs')
    rec['rows'] = {'base': len(b), 'cand': len(k)}
    bf = b.reset_index()
    kf = k.reset_index()
    rec['u_manquant'] = {'base': int((bf['u'] == 'Manquant').sum()), 'cand': int((kf['u'] == 'Manquant').sum())}
    rec['u_unknown'] = {'base': int((bf['u'] == 'Unknown').sum()), 'cand': int((kf['u'] == 'Unknown').sum())}
    # relabel the base the way the change should, then align on the key
    moved_u = bf['u'] == 'Manquant'
    bf.loc[moved_u, 'u'] = 'Unknown'
    moved_crop = pd.Series(False, index=bf.index)
    if c == 'Senegal':
        moved_crop = bf['crop'].isin(list(SEN_RELABEL))
        bf['crop'] = bf['crop'].replace(SEN_RELABEL)
    rec['relabelled_rows'] = {'u': int(moved_u.sum()), 'crop': int(moved_crop.sum()),
                              'crop_by_wave': {str(t): int(n) for t, n in bf.loc[moved_crop, 't'].value_counts().items()}}
    key = list(b.index.names)
    bi = bf.set_index(key).sort_index()
    ki = kf.set_index(key).sort_index()
    rec['same_keys_after_relabel'] = bool(bi.index.equals(ki.index))
    if not rec['same_keys_after_relabel']:
        rec['keys_only_base'] = len(bi.index.difference(ki.index))
        rec['keys_only_cand'] = len(ki.index.difference(bi.index))
        common = bi.index.intersection(ki.index)
        bi, ki = bi.loc[common], ki.loc[common]
    for t in sorted(set(bi.index.get_level_values('t'))):
        bb = bi[bi.index.get_level_values('t') == t]
        kk = ki[ki.index.get_level_values('t') == t]
        rec[f'cells_differ_{t}'] = cells_differ(bb, kk)
    # Togo 2018: the rewired columns, by value
    if c == 'Togo':
        k18 = kf[kf['t'] == '2018']; b18 = b.reset_index(); b18 = b18[b18['t'] == '2018']
        rec['togo2018'] = {
            'Purchased_base': {str(x): int(n) for x, n in b18['Purchased'].value_counts(dropna=False).items()},
            'Purchased_cand': {str(x): int(n) for x, n in k18['Purchased'].value_counts(dropna=False).items()},
            'Received_as_gift_cand': {str(x): int(n) for x, n in k18['Received_as_gift'].value_counts(dropna=False).items()},
            'Quantity_purchased_nonnull': {'base': int(b18['Quantity_purchased'].notna().sum()), 'cand': int(k18['Quantity_purchased'].notna().sum())},
            'Quantity_received_as_gift_nonnull_cand': int(k18['Quantity_received_as_gift'].notna().sum()),
            'Unit_purchased_cand': {str(x): int(n) for x, n in k18['Unit_purchased'].value_counts(dropna=False).items()},
            'Unit_received_as_gift_cand': {str(x): int(n) for x, n in k18['Unit_received_as_gift'].value_counts(dropna=False).items()},
            'u_cand_2018': {str(x): int(n) for x, n in k18['u'].value_counts(dropna=False).items()},
            'gift_equals_old_purchase': bool((k18.set_index(key)['Received_as_gift'].sort_index().astype('boolean').fillna(False).values
                                             == b18.assign(u=b18['u'].replace({'Manquant': 'Unknown'})).set_index(key)['Purchased'].sort_index().astype('boolean').fillna(False).values).all()),
            'gift_qty_equals_old_purchase_qty': bool(np.allclose(
                k18.set_index(key)['Quantity_received_as_gift'].sort_index().astype(float).fillna(-1).values,
                b18.assign(u=b18['u'].replace({'Manquant': 'Unknown'})).set_index(key)['Quantity_purchased'].sort_index().astype(float).fillna(-1).values)),
        }
        hh = kf[(kf['i'] == '101009') & (kf['input'] == 'Seed')]
        rec['togo_101009_seed_rows'] = hh.astype(str).to_dict('records')
    for fn in ('seed_kg', 'nitrogen_kg'):
        try:
            rec[f'{fn}_equal'] = frame_equal(load(bdir, f'{c}__{fn}'), load(cdir, f'{c}__{fn}'))
        except Exception as e:
            rec[f'{fn}_equal'] = f'ERR {e}'
    for extra in ('crop_production', 'plot_features'):
        try:
            rec[f'{extra}_equal'] = frame_equal(load(bdir, f'{c}__{extra}'), load(cdir, f'{c}__{extra}'))
        except FileNotFoundError:
            pass
    # Country vs Feature (candidate)
    try:
        f = load(cdir, f'{c}__feature_plot_inputs')
        ff = f.xs(c, level='country').reset_index()
        extra_levels = [l for l in ff.columns if l in ('plot_id', 'season', 'input_obs', 'currency')]
        rec['feature_added_levels'] = {l: sorted(map(str, ff[l].dropna().unique()))[:5] for l in extra_levels}
        ff = ff.drop(columns=extra_levels).rename(columns={'j': 'crop'})
        cc = kf.copy()
        kk = ['t', 'v', 'i', 'input', 'crop', 'u']
        ff = ff.set_index(kk).sort_index(); cc = cc.set_index(kk).sort_index()
        cols = [x for x in cc.columns if x in ff.columns]
        rec['feature_vs_country'] = {'rows': [len(cc), len(ff)], 'index_equal': bool(cc.index.equals(ff.index)),
                                     'cells_differ': cells_differ(cc[cols], ff[cols]) if cc.index.equals(ff.index) else 'index differs',
                                     'cols_only_country': [x for x in cc.columns if x not in ff.columns],
                                     'cols_only_feature': [x for x in ff.columns if x not in cc.columns]}
    except Exception as e:
        rec['feature_vs_country'] = f'ERR {type(e).__name__}: {e}'

json.dump(out, open(outp, 'w'), indent=1, ensure_ascii=False, default=str)
print(json.dumps(out, indent=1, ensure_ascii=False, default=str)[:30000])
