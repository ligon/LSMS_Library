"""Base (previous head) vs candidate for the four gift cells.  Usage: compare_gift.py <base_dir> <cand_dir> <out.json>"""
import json, sys
import numpy as np
import pandas as pd

bdir, cdir, outp = sys.argv[1:4]
CELLS = {'Senegal': ['2018-19'], 'CotedIvoire': ['2018-19'], 'Niger': ['2018-19', '2021-22']}
ACQ = ['Purchased', 'Quantity_purchased', 'Unit_purchased', 'Received_as_gift',
       'Quantity_received_as_gift', 'Unit_received_as_gift']
out = {}

def load(d, name):
    return pd.read_parquet(f'{d}/{name}.parquet')

def vc(s):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).items()}

def ndiff(a, b):
    both = a.isna() & b.isna()
    eq = (a.astype(object) == b.astype(object)).fillna(False)
    return int((~(eq | both)).sum())

def frame_equal(a, b):
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-12, atol=0)
        return True
    except AssertionError as e:
        return str(e)[:500]

for c, cells in CELLS.items():
    rec = out[c] = {}
    b = load(bdir, f'{c}__plot_inputs').sort_index()
    k = load(cdir, f'{c}__plot_inputs').sort_index()
    rec['rows'] = [len(b), len(k)]
    rec['index_equal'] = bool(b.index.equals(k.index))
    tb = b.index.get_level_values('t'); tk = k.index.get_level_values('t')
    rec['rows_by_wave'] = {'base': vc(pd.Series(tb)), 'cand': vc(pd.Series(tk))}
    for t in sorted(set(tb)):
        bb, kk = b[tb == t], k[tk == t]
        cols = sorted(set(bb.columns) | set(kk.columns))
        rec[f'cells_differ_{t}'] = {col: (ndiff(bb[col], kk[col]) if col in bb and col in kk else 'missing one side') for col in cols}
    for t in cells:
        bb, kk = b[tb == t], k[tk == t]
        u = set(map(str, kk.index.get_level_values('u').unique()))
        cell = rec[f'cell_{t}'] = {
            'Purchased_before': vc(bb['Purchased']), 'Purchased_after': vc(kk['Purchased']),
            'Quantity_purchased_nonnull': [int(bb['Quantity_purchased'].notna().sum()), int(kk['Quantity_purchased'].notna().sum())],
            'Received_as_gift_after': vc(kk['Received_as_gift']),
            'Quantity_received_as_gift_nonnull_after': int(kk['Quantity_received_as_gift'].notna().sum()),
            'gift_flag_equals_old_purchase_flag': bool((kk['Received_as_gift'].astype('boolean').fillna(False).to_numpy()
                                                        == bb['Purchased'].astype('boolean').fillna(False).to_numpy()).all()
                                                       and (kk['Received_as_gift'].isna().to_numpy() == bb['Purchased'].isna().to_numpy()).all()),
            'gift_qty_equals_old_purchase_qty': bool(np.array_equal(
                kk['Quantity_received_as_gift'].astype(float).fillna(-1.0).to_numpy(),
                bb['Quantity_purchased'].astype(float).fillna(-1.0).to_numpy())),
            'Unit_purchased_after': vc(kk['Unit_purchased']),
            'Unit_received_as_gift_after': vc(kk['Unit_received_as_gift']),
            'u_labels_after': sorted(u),
            'unit_labels_not_in_u': sorted((set(kk['Unit_purchased'].dropna().astype(str)) | set(kk['Unit_received_as_gift'].dropna().astype(str))) - u),
            'qty_purchased_without_unit': int((kk['Quantity_purchased'].notna() & kk['Unit_purchased'].isna()).sum()),
            'unit_purchased_without_qty': int((kk['Quantity_purchased'].isna() & kk['Unit_purchased'].notna()).sum()),
            'qty_gift_without_unit': int((kk['Quantity_received_as_gift'].notna() & kk['Unit_received_as_gift'].isna()).sum()),
            'unit_gift_without_qty': int((kk['Quantity_received_as_gift'].isna() & kk['Unit_received_as_gift'].notna()).sum()),
        }
    if c == 'Senegal':
        s21 = k[tk == '2021-22']
        rec['senegal_2021_unit_vocab'] = {'Unit_purchased': vc(s21['Unit_purchased']), 'Unit_received_as_gift': vc(s21['Unit_received_as_gift'])}
    for fn in ('seed_kg', 'nitrogen_kg'):
        rec[f'{fn}_equal'] = frame_equal(load(bdir, f'{c}__{fn}'), load(cdir, f'{c}__{fn}'))
    for extra in ('crop_production', 'plot_features'):
        try:
            rec[f'{extra}_equal'] = frame_equal(load(bdir, f'{c}__{extra}'), load(cdir, f'{c}__{extra}'))
        except FileNotFoundError:
            pass
    f = load(cdir, f'{c}__feature_plot_inputs').xs(c, level='country').reset_index()
    extra_levels = [l for l in ('plot_id', 'season', 'input_obs', 'currency') if l in f.columns]
    cc = k.reset_index()
    keys = ['t', 'v', 'i', 'input', 'crop', 'u'] + (['input_obs'] if 'input_obs' in cc.columns else [])
    f = f.drop(columns=[l for l in extra_levels if l not in keys]).rename(columns={'j': 'crop'}).set_index(keys).sort_index()
    cc = cc.set_index(keys).sort_index()
    cols = [x for x in ACQ + ['Quantity'] if x in cc.columns and x in f.columns]
    rec['feature_vs_country'] = {'rows': [len(cc), len(f)], 'index_equal': bool(cc.index.equals(f.index)),
                                 'cells_differ': {col: ndiff(cc[col], f[col]) for col in cols} if cc.index.equals(f.index) else 'index differs'}
json.dump(out, open(outp, 'w'), indent=1, ensure_ascii=False, default=str)
print(json.dumps(out, indent=1, ensure_ascii=False, default=str)[:40000])
