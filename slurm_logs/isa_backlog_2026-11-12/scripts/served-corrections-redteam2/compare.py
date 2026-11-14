"""3ae15b56c (prev) vs 06bac7429 (head) frame comparison.
Usage: python compare.py <prev_frames> <head_frames>   Aggregates only -> $RESULTS/compare.json."""
import json
import os
import sys
import traceback

import numpy as np
import pandas as pd

B, C = sys.argv[1], sys.argv[2]
OUT = {}
LK = 'Niger::food_acquired::flagged-purchase-local-median'
FK = 'Niger::food_acquired::computed-last-purchase-quantity'


def load(d, tag):
    p = os.path.join(d, tag + '.pkl')
    return pd.read_pickle(p) if os.path.exists(p) else None


def lv(df, name):
    return df.index.get_level_values(name).astype(str)


def eqcol(a, b):
    an, bn = pd.isna(a).to_numpy(), pd.isna(b).to_numpy()
    try:
        av = pd.to_numeric(a, errors='raise').to_numpy(dtype=float)
        bv = pd.to_numeric(b, errors='raise').to_numpy(dtype=float)
        same = (av == bv)
    except (ValueError, TypeError):
        same = (a.astype(object).astype(str).to_numpy() == b.astype(object).astype(str).to_numpy())
    return (an & bn) | (~an & ~bn & same)


def keystr(df):
    k = df.index.to_frame(index=False)
    for col in k.columns:
        k[col] = k[col].astype(object).where(k[col].notna(), '__NA__').astype(str)
    k['_dup'] = k.groupby(list(k.columns[:len(df.index.names)])).cumcount()
    return k


def merge_diff(b, c):
    kb, kc = keystr(b), keystr(c)
    vb = b.reset_index(drop=True).astype(object)
    vc = c.reset_index(drop=True).astype(object)
    vb.columns = [f'{x}__b' for x in vb.columns]
    vc.columns = [f'{x}__c' for x in vc.columns]
    m = pd.concat([kb, vb], axis=1).merge(pd.concat([kc, vc], axis=1), on=list(kb.columns), how='outer', indicator=True)
    o = {'left_only': int((m['_merge'] == 'left_only').sum()), 'right_only': int((m['_merge'] == 'right_only').sum()),
         'both': int((m['_merge'] == 'both').sum()), 'nan_key_rows_prev': int((kb == '__NA__').any(axis=1).sum()),
         'dup_ordinals_prev': int((kb['_dup'] > 0).sum())}
    both = m[m['_merge'] == 'both']
    chg, anyc = {}, np.zeros(len(both), dtype=bool)
    for col in b.columns:
        if col in c.columns:
            e = eqcol(both[f'{col}__b'], both[f'{col}__c'])
            if (~e).any():
                chg[col] = int((~e).sum())
                anyc |= ~e
    o['changed_cells'] = chg
    o['rows_changed'] = int(anyc.sum())
    if 't' in b.index.names:
        o['left_only_by_t'] = {str(k): int(v) for k, v in m.loc[m['_merge'] == 'left_only', 't'].value_counts().items()}
        o['right_only_by_t'] = {str(k): int(v) for k, v in m.loc[m['_merge'] == 'right_only', 't'].value_counts().items()}
        o['changed_by_t'] = {str(k): int(v) for k, v in both.loc[anyc, 't'].value_counts().items()}
    return o


def per_wave_equal(b, c):
    if 't' not in b.index.names:
        return None
    out = {}
    tb, tc = lv(b, 't'), lv(c, 't')
    for w in sorted(set(tb) | set(tc)):
        try:
            pd.testing.assert_frame_equal(b[tb == w], c[tc == w], check_exact=True)
            out[w] = True
        except AssertionError as e:
            out[w] = str(e).split('\n')[0][:200]
    return out


def sums_by_t(df):
    if 't' not in df.index.names:
        return None
    t = lv(df, 't')
    return {w: {c: round(float(pd.to_numeric(df.loc[t == w, c], errors='coerce').sum()), 3)
                for c in df.columns if pd.api.types.is_numeric_dtype(df[c])} for w in sorted(set(t))}


def diff(tag):
    b, c = load(B, tag), load(C, tag)
    o = {'tag': tag}
    if b is None or c is None:
        o['missing'] = {'prev': b is None, 'head': c is None}
        return o
    o['frame_equal_strict'] = bool(b.equals(c))
    o['shape'] = {'prev': list(b.shape), 'head': list(c.shape)}
    o['index_names_equal'] = list(b.index.names) == list(c.index.names)
    o['columns_equal'] = list(b.columns) == list(c.columns)
    o['dtypes_equal'] = all(str(b[k].dtype) == str(c[k].dtype) for k in b.columns if k in c.columns)
    try:
        o['per_wave_assert_frame_equal'] = per_wave_equal(b, c)
    except Exception as e:
        o['per_wave_assert_frame_equal'] = f'ERR {type(e).__name__}: {e}'
    if not o['frame_equal_strict']:
        try:
            o['merge_diff'] = merge_diff(b, c)
        except Exception as e:
            o['merge_diff'] = f'ERR {type(e).__name__}: {e}'
        o['sums_by_t'] = {'prev': sums_by_t(b), 'head': sums_by_t(c)}
    return o


def run(name, fn):
    try:
        OUT[name] = fn()
    except Exception as e:
        OUT[name] = {'ERROR': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
    print(name, json.dumps(OUT[name], default=str)[:4000], flush=True)
    json.dump(OUT, open(os.path.join(os.environ['RESULTS'], 'compare.json'), 'w'), indent=1, default=str)


TAGS = []
for c in ('Mali', 'Senegal', 'Niger'):
    TAGS += [f'{c}__{t}' for t in ('food_acquired', 'food_expenditures', 'food_prices', 'food_quantities')]
TAGS += ['Senegal__food_prices__units-unitvalue', 'Mali__food_quantities__units-units',
         'Niger__food_prices__units-unitvalue', 'Niger__food_quantities__units-units',
         'Uganda__sample', 'Uganda__cluster_features', 'Uganda__household_characteristics']
for tag in TAGS:
    run(tag, lambda tag=tag: diff(tag))


def niger_detail():
    o = {}
    b, c = load(B, 'Niger__food_acquired'), load(C, 'Niger__food_acquired')
    o['same_index_and_order'] = bool(b.index.equals(c.index))
    assert o['same_index_and_order'], 'index differs; positional comparison invalid'
    t, s = lv(b, 't'), lv(b, 's')
    for col in b.columns:
        e = eqcol(b[col], c[col])
        o[f'changed_{col}'] = int((~e).sum())
        o[f'changed_{col}_by_t_s'] = {f'{a}/{x}': int(n) for (a, x), n in
                                      pd.Series(1, index=pd.MultiIndex.from_arrays([t[~e], s[~e]])).groupby(level=[0, 1]).sum().items()} if (~e).any() else {}
    bd = b['Derivation'].astype(object).fillna('').astype(str)
    cd = c['Derivation'].astype(object).fillna('').astype(str)
    hasL = cd.str.contains(LK, regex=False).to_numpy()
    o['head_local_key_rows'] = int(hasL.sum())
    o['prev_local_key_rows'] = int(bd.str.contains(LK, regex=False).sum())
    o['flag_key_rows_prev'] = int(bd.str.contains(FK, regex=False).sum())
    o['flag_key_rows_head'] = int(cd.str.contains(FK, regex=False).sum())
    o['flag_key_rowset_identical'] = bool(np.array_equal(bd.str.contains(FK, regex=False).to_numpy(),
                                                         cd.str.contains(FK, regex=False).to_numpy()))
    dchg = (bd != cd).to_numpy()
    added = [set(y.split('+')) - set(x.split('+')) for x, y in zip(bd[dchg], cd[dchg])]
    removed = [set(x.split('+')) - set(y.split('+')) for x, y in zip(bd[dchg], cd[dchg])]
    o['derivation_changed_rows'] = int(dchg.sum())
    o['derivation_added_only_local_key'] = all(a == {LK} for a in added)
    o['derivation_nothing_removed'] = all(not (r - {''}) for r in removed)
    o['derivation_changed_equals_local_rows'] = bool(np.array_equal(dchg, hasL))
    eb, ec = b['Expenditure'].astype(float).to_numpy(), c['Expenditure'].astype(float).to_numpy()
    ech = ~eqcol(b['Expenditure'], c['Expenditure'])
    o['expenditure_changed_rows'] = int(ech.sum())
    o['expenditure_changed_outside_local_rows'] = int((ech & ~hasL).sum())
    o['na_prev_valued_head'] = int((np.isnan(eb) & ~np.isnan(ec)).sum())
    o['valued_prev_na_head'] = int((~np.isnan(eb) & np.isnan(ec)).sum())
    o['valued_prev_na_head_with_local_key'] = int((~np.isnan(eb) & np.isnan(ec) & hasL).sum())
    o['local_rows_na_prev'] = int((hasL & np.isnan(eb)).sum())
    both = hasL & ~np.isnan(eb) & ~np.isnan(ec)
    r = ec[both] / eb[both]
    o['ratio_head_over_prev_on_valued_both'] = {'n': int(both.sum()), 'median': float(np.median(r)),
                                                'exact_equal': int((ec[both] == eb[both]).sum()),
                                                'within_1e-9': int((np.abs(r - 1) < 1e-9).sum()),
                                                'p10': float(np.quantile(r, .1)), 'p90': float(np.quantile(r, .9))}
    p18 = (t == '2018-19') & (s == 'purchased')
    o['purchased_exp_2018_prev'] = round(float(np.nansum(eb[p18])), 2)
    o['purchased_exp_2018_head'] = round(float(np.nansum(ec[p18])), 2)
    o['purchased_exp_2018_change_pct'] = round(100 * (o['purchased_exp_2018_head'] / o['purchased_exp_2018_prev'] - 1), 3)
    fk = cd.str.contains(FK, regex=False).to_numpy()
    o['flag_rows_exp_prev'] = round(float(np.nansum(eb[fk])), 2)
    o['flag_rows_exp_head'] = round(float(np.nansum(ec[fk])), 2)
    o['flag_rows_valued_prev'] = int((fk & ~np.isnan(eb)).sum())
    o['flag_rows_valued_head'] = int((fk & ~np.isnan(ec)).sum())
    for col in ('Quantity', 'Quantity_kg'):
        if col in b.columns:
            o[f'{col}_all_equal'] = bool(eqcol(b[col], c[col]).all())
    o['per_wave_equal'] = per_wave_equal(b, c)
    # derived tables: rows gone / changed by t
    for tag in ('Niger__food_expenditures', 'Niger__food_prices', 'Niger__food_quantities',
                'Niger__food_prices__units-unitvalue', 'Niger__food_quantities__units-units'):
        bb, cc = load(B, tag), load(C, tag)
        if bb is None or cc is None:
            continue
        try:
            o[tag] = merge_diff(bb, cc)
        except Exception as e:
            o[tag] = f'ERR {type(e).__name__}: {e}'
    return o


run('niger_detail', niger_detail)
print('done compare', flush=True)
