"""Compare base and candidate frames.  AGGREGATES ONLY.

usage: python compare.py <framesdir> <out.json> <base-arm> <cand-arm>
"""
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

fd, out, B, N = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
R = {}


def load(arm, name):
    p = fd / f'{arm}.{name}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def neq(a, b):
    """Elementwise 'not equal', NA == NA."""
    both = a.isna() & b.isna()
    try:
        e = (a == b).fillna(False).astype(bool)
    except Exception:  # noqa: BLE001
        e = a.astype(str) == b.astype(str)
    return ~(e | both)


def delta(a, b, label, cols=None):
    r = {'rows': [int(len(a)), int(len(b))],
         'unique': [bool(a.index.is_unique), bool(b.index.is_unique)],
         'cols': [list(a.columns), list(b.columns)]}
    if cols is None:
        cols = [c for c in a.columns if c in b.columns]
    oa, ob = a.index.difference(b.index), b.index.difference(a.index)
    inter = a.index.intersection(b.index)
    x, y = a.loc[inter], b.loc[inter]
    changed = pd.Series(False, index=inter)
    by_col = {}
    for col in cols:
        d = neq(x[col], y[col])
        by_col[col] = int(d.sum())
        changed |= d.to_numpy()
    r['only_base'] = int(len(oa))
    r['only_cand'] = int(len(ob))
    r['changed'] = int(changed.sum())
    r['changed_by_col'] = by_col
    try:
        common = [c for c in a.columns if c in b.columns]
        pd.testing.assert_frame_equal(a[common], b[common], check_exact=True)
        r['frame_equal'] = True
    except AssertionError as e:
        r['frame_equal'] = False
        r['msg'] = str(e)[:600]
    if 't' in a.index.names:
        for nm, ix in (('only_base', oa), ('only_cand', ob), ('changed', inter[changed.to_numpy()])):
            if len(ix):
                r[f'{nm}_by_t'] = dict(Counter(map(str, ix.get_level_values('t'))))
                if 'j' in a.index.names:
                    r[f'{nm}_j_top'] = dict(Counter(map(str, ix.get_level_values('j'))).most_common(8))
                if 'u' in a.index.names:
                    r[f'{nm}_u_top'] = dict(Counter(map(str, ix.get_level_values('u'))).most_common(8))
                if 's' in a.index.names:
                    r[f'{nm}_s'] = dict(Counter(map(str, ix.get_level_values('s'))))
    return r, oa, ob, inter[changed.to_numpy()]


def keys3(df):
    f = df.reset_index()
    return pd.MultiIndex.from_frame(f[['t', 'i', 'j']].astype(str))


def tij(ix):
    f = ix.to_frame(index=False)
    return pd.MultiIndex.from_frame(f[['t', 'i', 'j']].astype(str))


def wave_slice(df, waves, keep=True):
    t = df.index.get_level_values('t').astype(str)
    m = t.isin(waves)
    return df[m if keep else ~m]


# ---------------------------------------------------------------- generic tables
for p in sorted(fd.glob(f'{B}.*.parquet')):
    name = p.name[len(B) + 1:-len('.parquet')]
    if '.inputs.' in name:
        continue
    a, b = load(B, name), load(N, name)
    if a is None or b is None:
        R[name] = {'missing': [a is None, b is None]}
        continue
    r, *_ = delta(a, b, name)
    R[name] = r
    print(name, json.dumps({k: v for k, v in r.items() if k not in ('cols', 'msg')}, default=str)[:900], flush=True)

S = {}
# ---------------------------------------------------------------- Mali
try:
    a, b = load(B, 'Mali.food_acquired'), load(N, 'Mali.food_acquired')
    inp = load(N, 'Mali.inputs.rice-ration-block-withheld')
    m = {'inputs_rows': int(len(inp)), 'inputs_vague_na': int(inp['vague'].isna().sum())}
    ik = set(tij(inp.index))
    is_blk = lambda df: (pd.Index(tij(df.index)).isin(ik) & (df.index.get_level_values('u').astype(str) == 'Kg')  # noqa: E731
                         & (df.index.get_level_values('s').astype(str) == 'purchased'))
    ba, bb = a[is_blk(a)], b[is_blk(b)]
    m['base_rows_on_block_keys'] = int(len(ba))
    m['cand_rows_on_block_keys'] = int(len(bb))
    d, oa, ob, ch = delta(a, b, 'Mali')
    m['only_base'] = int(len(oa)); m['only_cand'] = int(len(ob)); m['changed'] = int(len(ch))
    m['only_base_all_block'] = bool(pd.Index(tij(oa)).isin(ik).all())
    m['changed_all_block'] = bool(pd.Index(tij(ch)).isin(ik).all()) if len(ch) else None
    if len(ch):
        m['changed_quantity_base_minus_cand'] = round(float((a.loc[ch, 'Quantity'] - b.loc[ch, 'Quantity']).sum()), 4)
        m['changed_cols'] = {c: int(neq(a.loc[ch, c], b.loc[ch, c]).sum()) for c in a.columns if c in b.columns}
    rice = lambda df: df[(df.index.get_level_values('t').astype(str) == '2018-19')  # noqa: E731
                         & (df.index.get_level_values('j').astype(str).str.contains('Gambiaka'))
                         & (df.index.get_level_values('u').astype(str) == 'Kg')
                         & (df.index.get_level_values('s').astype(str) == 'purchased')]
    m['rice_kg_purchased_rows'] = [int(len(rice(a))), int(len(rice(b)))]
    m['rice_kg_purchased_quantity'] = [round(float(rice(a)['Quantity'].sum()), 3), round(float(rice(b)['Quantity'].sum()), 3)]
    m['inputs_s07bq03a_sum'] = round(float(pd.to_numeric(inp['s07bq03a']).sum()), 3)
    pur = lambda df: df[(df.index.get_level_values('t').astype(str) == '2018-19') & (df.index.get_level_values('s').astype(str) == 'purchased')]  # noqa: E731
    m['purchased_rows_2018'] = [int(len(pur(a))), int(len(pur(b)))]
    m['purchased_expenditure_na_2018'] = [int(pur(a)['Expenditure'].isna().sum()), int(pur(b)['Expenditure'].isna().sum())]
    m['purchased_expenditure_sum_2018'] = [round(float(pur(a)['Expenditure'].sum()), 2), round(float(pur(b)['Expenditure'].sum()), 2)]
    rest = a.drop(index=oa.append(ch))
    m['rest_equal'] = bool(rest.equals(b.loc[rest.index]))
    S['Mali'] = m
except Exception as e:  # noqa: BLE001
    S['Mali'] = {'error': f'{type(e).__name__}: {e}'}

# ---------------------------------------------------------------- Senegal
try:
    a, b = load(B, 'Senegal.food_acquired'), load(N, 'Senegal.food_acquired')
    s = {}
    inp = load(N, 'Senegal.inputs.salt-per-capita-withheld')
    s['salt_inputs_rows'] = int(len(inp))
    ik = set(tij(inp.index))
    d, oa, ob, ch = delta(a, b, 'Senegal')
    s['only_base'] = int(len(oa)); s['only_cand'] = int(len(ob)); s['changed'] = int(len(ch))
    s['only_base_by_t'] = dict(Counter(map(str, oa.get_level_values('t'))))
    s['only_base_all_salt_inputs'] = bool(pd.Index(tij(oa)).isin(ik).all())
    s['only_base_u'] = dict(Counter(map(str, oa.get_level_values('u'))))
    s['only_base_s'] = dict(Counter(map(str, oa.get_level_values('s'))))
    s['salt_base_rows_on_input_keys_u_kg'] = int((pd.Index(tij(a.index)).isin(ik) & (a.index.get_level_values('u').astype(str) == 'Kg')).sum())
    s['salt_removed_quantity'] = round(float(a.loc[oa, 'Quantity'].sum()), 6)
    s['salt_removed_expenditure'] = round(float(a.loc[oa, 'Expenditure'].sum()), 2)
    s['expenditure_2021_total'] = [round(float(wave_slice(a, ['2021-22'])['Expenditure'].sum()), 2),
                                   round(float(wave_slice(b, ['2021-22'])['Expenditure'].sum()), 2)]
    nz = load(N, 'Senegal.inputs.near-zero-quantity-withheld')
    s['near_zero_inputs_rows'] = int(len(nz))
    nk = set(tij(nz.index))
    s['changed_by_t'] = dict(Counter(map(str, ch.get_level_values('t'))))
    s['changed_all_near_zero'] = bool(pd.Index(tij(ch)).isin(nk).all()) if len(ch) else None
    s['changed_cols'] = {c: int(neq(a.loc[ch, c], b.loc[ch, c]).sum()) for c in a.columns if c in b.columns} if len(ch) else {}
    s['near_zero_cand_state'] = {c: int(b.loc[ch, c].isna().sum()) for c in ('Quantity', 'Expenditure', 'Quantity_kg') if c in b} if len(ch) else {}
    s['near_zero_base_quantity_max'] = float(a.loc[ch, 'Quantity'].max()) if len(ch) else None
    s['near_zero_base_derivation'] = dict(Counter(map(str, a.loc[ch, 'Derivation']))) if len(ch) else None
    s['near_zero_cand_derivation'] = dict(Counter(map(str, b.loc[ch, 'Derivation']))) if len(ch) else None
    rest = a.drop(index=oa.append(ch))
    s['rest_equal'] = bool(rest.equals(b.loc[rest.index]))
    for nm in ('Senegal.food_prices', 'Senegal.food_prices.units=unitvalue'):
        pa, pb = load(B, nm), load(N, nm)
        if pa is None or pb is None:
            continue
        col = 'Price' if 'Price' in pa.columns else pa.columns[0]
        for arm, df in (('base', pa), ('cand', pb)):
            w = wave_slice(df, ['2018-19'])[col]
            s[f'{nm}.{arm}.2018_max'] = float(w.max())
            s[f'{nm}.{arm}.2018_mean'] = float(w.mean())
            s[f'{nm}.{arm}.over_1e20'] = int((df[col] > 1e20).sum())
            s[f'{nm}.{arm}.over_1e10'] = int((df[col] > 1e10).sum())
    S['Senegal'] = s
except Exception as e:  # noqa: BLE001
    import traceback
    S['Senegal'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}

# ---------------------------------------------------------------- Niger
try:
    a, b = load(B, 'Niger.food_acquired'), load(N, 'Niger.food_acquired')
    n = {}
    KEY = 'Niger::food_acquired::computed-last-purchase-quantity'
    other = [c for c in a.columns if c != 'Derivation']
    n['index_equal'] = bool(a.index.equals(b.index))
    try:
        pd.testing.assert_frame_equal(a[other], b[other], check_exact=True)
        n['all_but_derivation_equal'] = True
    except AssertionError as e:
        n['all_but_derivation_equal'] = False
        n['msg'] = str(e)[:600]
    da, db = a['Derivation'].astype(object), b['Derivation'].astype(object)
    changed = neq(da, db)
    n['derivation_changed'] = int(changed.sum())
    stamped = db.fillna('').str.contains(KEY, regex=False)
    n['stamped'] = int(stamped.sum())
    n['stamped_by_t'] = dict(Counter(map(str, b.index[stamped.to_numpy()].get_level_values('t'))))
    n['stamped_s'] = dict(Counter(map(str, b.index[stamped.to_numpy()].get_level_values('s'))))
    strip = db.where(~stamped, db.fillna('').str.replace('+' + KEY, '', regex=False).str.replace(KEY + '+', '', regex=False))
    n['change_is_only_the_key'] = bool((~neq(da, strip.replace('', np.nan))).all())
    n['stamped_expenditure_finite'] = int(b.loc[stamped.to_numpy(), 'Expenditure'].notna().sum())
    w = (b.index.get_level_values('t').astype(str) == '2018-19') & (b.index.get_level_values('s').astype(str) == 'purchased')
    tot = float(b.loc[w, 'Expenditure'].sum())
    n['stamped_expenditure_share_2018'] = round(100 * float(b.loc[stamped.to_numpy() & w, 'Expenditure'].sum()) / tot, 3)
    n['purchased_valued_rows_2018'] = int(b.loc[w, 'Expenditure'].notna().sum())
    inp = load(N, 'Niger.inputs.computed-last-purchase-quantity')
    n['inputs_rows'] = int(len(inp))
    ik = set(tij(inp.index))
    sk = pd.Index(tij(b.index[stamped.to_numpy()]))
    n['stamped_rows_joining_inputs'] = int(sk.isin(ik).sum())
    n['inputs_s07bq06'] = dict(Counter(map(str, inp['s07bq06'])))
    S['Niger'] = n
except Exception as e:  # noqa: BLE001
    import traceback
    S['Niger'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}

# ---------------------------------------------------------------- Uganda
try:
    a, b = load(B, 'Uganda.sample'), load(N, 'Uganda.sample')
    u = {'index_equal': bool(a.index.equals(b.index))}
    other = [c for c in a.columns if c not in ('ea', 'Derivation')]
    try:
        pd.testing.assert_frame_equal(a[other], b[other], check_exact=True)
        u['all_but_ea_derivation_equal'] = True
    except AssertionError as e:
        u['all_but_ea_derivation_equal'] = False
        u['msg'] = str(e)[:600]
    ce = neq(a['ea'].astype(object), b['ea'].astype(object))
    u['ea_changed'] = int(ce.sum())
    u['ea_changed_by_t'] = dict(Counter(map(str, a.index[ce.to_numpy()].get_level_values('t'))))
    u['ea_changed_from_na'] = int((ce & a['ea'].isna()).sum())
    u['ea_nonmissing_changed'] = int((ce & a['ea'].notna()).sum())
    cd = neq(a['Derivation'].astype(object), b['Derivation'].astype(object))
    u['derivation_changed'] = int(cd.sum())
    u['derivation_changed_outside_ea_changed'] = int((cd & ~ce).sum())
    key = 'Uganda::sample::ea-same-household'
    st = b['Derivation'].astype(object).fillna('').str.contains(key, regex=False)
    u['stamped'] = int(st.sum())
    u['stamped_eq_filled'] = bool((st == ce).all())
    t = b.index.get_level_values('t').astype(str)
    nine_a, nine_b = a[t == '2009-10'], b[t == '2009-10']
    u['ea_na_2009'] = [int(nine_a['ea'].isna().sum()), int(nine_b['ea'].isna().sum())]
    wt = nine_a['weight'].sum()
    u['weight_share_na_2009'] = [round(100 * float(nine_a.loc[nine_a['ea'].isna(), 'weight'].sum()) / float(wt), 3),
                                 round(100 * float(nine_b.loc[nine_b['ea'].isna(), 'weight'].sum()) / float(wt), 3)]
    u['rows_2009'] = [int(len(nine_a)), int(len(nine_b))]
    u['ea_distinct_2009'] = [int(nine_a['ea'].nunique()), int(nine_b['ea'].nunique())]
    u['filled_labels_known_elsewhere'] = int(b.loc[ce.to_numpy(), 'ea'].isin(set(b.loc[t != '2009-10', 'ea'].dropna())).sum())
    u['ea_dtype'] = [str(a['ea'].dtype), str(b['ea'].dtype)]
    inp = load(N, 'Uganda.inputs.ea-same-household')
    u['inputs_rows'] = int(len(inp))
    u['inputs_households'] = int(inp.index.get_level_values('i').nunique())
    u['inputs_sources'] = dict(Counter(map(str, inp['_source']))) if '_source' in inp else None
    sk = b.index[st.to_numpy()].to_frame(index=False)[['t', 'i']].astype(str)
    ik = set(map(tuple, inp.index.to_frame(index=False)[['t', 'i']].astype(str).to_numpy()))
    u['stamped_joining_inputs'] = int(sum(tuple(x) in ik for x in sk.to_numpy()))
    u['stamped_count_for_join'] = int(len(sk))
    S['Uganda'] = u
except Exception as e:  # noqa: BLE001
    import traceback
    S['Uganda'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}

R['specific'] = S
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps(S, indent=1, default=str))
print('COMPARE DONE', flush=True)
