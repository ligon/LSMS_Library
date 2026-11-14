"""Base vs candidate frame comparison.  Usage: python compare.py <base_frames> <cand_frames>
Aggregates only -> $RESULTS/compare.json."""
import json
import os
import sys
import traceback

import numpy as np
import pandas as pd

B, C = sys.argv[1], sys.argv[2]
OUT = {}


def load(d, tag):
    p = os.path.join(d, tag + '.pkl')
    return pd.read_pickle(p) if os.path.exists(p) else None


def eqcol(a, b):
    """elementwise equal with NA==NA, robust to dtypes"""
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
    """Index-robust diff: stringified keys (NA kept as a value) + duplicate ordinal, outer merge."""
    kb, kc = keystr(b), keystr(c)
    vb = b.reset_index(drop=True).astype(object)
    vc = c.reset_index(drop=True).astype(object)
    vb.columns = [f'{x}__b' for x in vb.columns]
    vc.columns = [f'{x}__c' for x in vc.columns]
    L = pd.concat([kb, vb], axis=1)
    Rr = pd.concat([kc, vc], axis=1)
    keys = list(kb.columns)
    m = L.merge(Rr, on=keys, how='outer', indicator=True)
    o = {'left_only': int((m['_merge'] == 'left_only').sum()), 'right_only': int((m['_merge'] == 'right_only').sum()),
         'both': int((m['_merge'] == 'both').sum()), 'nan_keys_base': int((kb == '__NA__').any(axis=1).sum()),
         'dup_ordinals_base': int((kb['_dup'] > 0).sum())}
    both = m[m['_merge'] == 'both']
    chg = {}
    anyc = np.zeros(len(both), dtype=bool)
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
    tb, tc = b.index.get_level_values('t').astype(str), c.index.get_level_values('t').astype(str)
    for w in sorted(set(tb) | set(tc)):
        try:
            pd.testing.assert_frame_equal(b[tb == w], c[tc == w], check_exact=True)
            out[w] = True
        except AssertionError as e:
            out[w] = str(e).split('\n')[0][:200]
    return out


def diff(tag):
    b, c = load(B, tag), load(C, tag)
    o = {'tag': tag}
    if b is None or c is None:
        o['missing'] = {'base': b is None, 'cand': c is None}
        return o, None, None, None
    try:
        o['merge_diff'] = merge_diff(b, c)
    except Exception as e:
        o['merge_diff'] = f'ERR {type(e).__name__}: {e}'
    try:
        o['per_wave_assert_frame_equal'] = per_wave_equal(b, c)
    except Exception as e:
        o['per_wave_assert_frame_equal'] = f'ERR {type(e).__name__}: {e}'
    o['shape'] = {'base': list(b.shape), 'cand': list(c.shape)}
    o['index_names_equal'] = list(b.index.names) == list(c.index.names)
    o['columns'] = {'base': list(b.columns), 'cand': list(c.columns)}
    bi, ci = b.index, c.index
    o['dup_index'] = {'base': int(bi.duplicated().sum()), 'cand': int(ci.duplicated().sum())}
    only_b = bi.difference(ci)
    only_c = ci.difference(bi)
    common = bi.intersection(ci)
    o['rows_only_base'] = len(only_b)
    o['rows_only_cand'] = len(only_c)
    o['rows_common'] = len(common)
    bb, cc = b.loc[common], c.loc[common]
    changed = {}
    mask_any = np.zeros(len(common), dtype=bool)
    for col in sorted(set(b.columns) | set(c.columns)):
        if col not in b.columns or col not in c.columns:
            changed[col] = 'missing-on-one-side'
            continue
        e = eqcol(bb[col], cc[col])
        n = int((~e).sum())
        if n:
            changed[col] = n
            mask_any |= ~e
    o['changed_cells_by_column'] = changed
    o['rows_changed'] = int(mask_any.sum())
    try:
        o['frame_equal_strict'] = bool(b.equals(c))
    except Exception:
        o['frame_equal_strict'] = None
    o['dtypes_equal'] = {k: (str(b[k].dtype) == str(c[k].dtype)) for k in b.columns if k in c.columns}
    if 't' in b.index.names:
        def by_t(idx):
            return {str(k): int(v) for k, v in pd.Series(idx.get_level_values('t').astype(str)).value_counts().items()}
        o['only_base_by_t'] = by_t(only_b) if len(only_b) else {}
        o['changed_by_t'] = by_t(common[mask_any]) if mask_any.any() else {}
        o['rows_by_t'] = {'base': by_t(bi), 'cand': by_t(ci)}
    return o, (b, c), (only_b, only_c), common[mask_any]


def run(name, fn):
    try:
        OUT[name] = fn()
    except Exception as e:
        OUT[name] = {'ERROR': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
    print(name, json.dumps(OUT[name], default=str)[:4000], flush=True)


TAGS = []
for c in ('Mali', 'Senegal', 'Niger'):
    TAGS += [f'{c}__{t}' for t in ('food_acquired', 'food_expenditures', 'food_prices', 'food_quantities')]
TAGS += ['Senegal__food_prices__units-unitvalue', 'Mali__food_quantities__units-units',
         'Uganda__sample', 'Uganda__cluster_features', 'Uganda__household_characteristics']
for tag in TAGS:
    run(tag, lambda tag=tag: diff(tag)[0])


def lv(df, name):
    return df.index.get_level_values(name).astype(str)


# ----------------------------------------------------------- Mali detail
def mali():
    o = {}
    _, (b, c), (ob, oc), ch = diff('Mali__food_acquired')
    rec = pd.read_pickle(os.path.join(C, 'mali_records.pkl'))
    blk = rec[rec['block']]
    gone = b.loc[ob]
    o['gone_rows'] = len(gone)
    o['gone_t'] = sorted(set(lv(gone, 't')))
    o['gone_j'] = sorted(set(lv(gone, 'j')))
    o['gone_u'] = sorted(set(lv(gone, 'u')))
    o['gone_s'] = sorted(set(lv(gone, 's')))
    o['gone_expenditure_notna'] = int(gone['Expenditure'].notna().sum())
    o['gone_quantity_sum'] = round(float(gone['Quantity'].astype(float).sum()), 3)
    tij_gone = set(zip(lv(gone, 't'), lv(gone, 'i'), lv(gone, 'j')))
    tij_blk = set(zip(blk['t'], blk['i'], blk['j']))
    o['gone_tij_subset_of_block_records'] = tij_gone <= tij_blk
    o['block_records_not_gone'] = len(tij_blk - tij_gone)
    o['changed_rows'] = len(ch)
    if len(ch):
        cb, cc_ = b.loc[ch], c.loc[ch]
        o['changed_keys_in_block'] = int(sum((t, i, j) in tij_blk for t, i, j in zip(lv(cb, 't'), lv(cb, 'i'), lv(cb, 'j'))))
        o['changed_j_u_s'] = sorted(set(zip(lv(cb, 'j'), lv(cb, 'u'), lv(cb, 's'))))
        cols = [k for k in cb.columns if not eqcol(cb[k], cc_[k]).all()]
        o['changed_columns'] = cols
        # the household's two records: block q3 and genuine purchased quantity
        for n, (key, rb) in enumerate(cb.iterrows()):
            t, i, j = str(key[cb.index.names.index('t')]), str(key[cb.index.names.index('i')]), str(key[cb.index.names.index('j')])
            r = rec[(rec['t'] == t) & (rec['i'] == i) & (rec['j'] == j)]
            bq = float(rb['Quantity']); cq = float(cc_.loc[key, 'Quantity'])
            blq = float(r.loc[r['block'], 'q3'].sum())
            gen = r[r['genuine_rice_kg']]
            o[f'changed_{n}'] = {
                'base_minus_cand_equals_block_q3': bool(np.isclose(bq - cq, blq)),
                'cand_equals_genuine_purchased': bool(np.isclose(cq, float(gen['purch'].sum()))),
                'genuine_records': len(gen), 'block_records': int(r['block'].sum()),
                'other_columns_equal': [k for k in cb.columns if k != 'Quantity' and eqcol(cb.loc[[key], k], cc_.loc[[key], k]).all()],
            }
    o['removed_plus_changed_quantity'] = round(o['gone_quantity_sum'] + sum(
        float(b.loc[k, 'Quantity']) - float(c.loc[k, 'Quantity']) for k in ch), 3)
    # inputs callable vs the withheld records
    ri = pd.read_pickle(os.path.join(C, 'mali_rice_inputs.pkl'))
    rtij = set(zip(lv(ri, 't'), lv(ri, 'i'), lv(ri, 'j')))
    chtij = set(zip(lv(b.loc[ch], 't'), lv(b.loc[ch], 'i'), lv(b.loc[ch], 'j'))) if len(ch) else set()
    o['inputs_tij_equals_gone_plus_changed'] = rtij == (tij_gone | chtij)
    o['inputs_rows'] = len(ri)
    # other waves untouched
    o['other_waves_identical'] = all(
        b[lv(b, 't') == w].equals(c[lv(c, 't') == w]) for w in sorted(set(lv(b, 't'))) if w != '2018-19')
    return o


def senegal():
    o = {}
    _, (b, c), (ob, oc), ch = diff('Senegal__food_acquired')
    rec21 = pd.read_pickle(os.path.join(C, 'senegal21_records.pkl'))
    rec18 = pd.read_pickle(os.path.join(C, 'senegal18_records.pkl'))
    gone = b.loc[ob]
    o['gone_rows'] = len(gone)
    o['gone_by_t_s'] = {f'{t}/{s}': int(n) for (t, s), n in pd.Series(1, index=pd.MultiIndex.from_arrays([lv(gone, 't'), lv(gone, 's')])).groupby(level=[0, 1]).sum().items()}
    o['gone_j_u'] = sorted(set(zip(lv(gone, 'j'), lv(gone, 'u'))))
    o['gone_quantity_sum'] = round(float(gone['Quantity'].astype(float).sum()), 4)
    o['gone_expenditure_sum'] = round(float(gone['Expenditure'].astype(float).sum()), 4)
    o['gone_expenditure_notna'] = int(gone['Expenditure'].notna().sum())
    sel = rec21[rec21['salt_sel']]
    tij_sel = set(zip(sel['t'], sel['i'], sel['j']))
    o['gone_tij_subset_selected'] = set(zip(lv(gone, 't'), lv(gone, 'i'), lv(gone, 'j'))) <= tij_sel
    o['selected_households'] = len(set(sel['i']))
    o['changed_rows'] = len(ch)
    if len(ch):
        cb, cc_ = b.loc[ch], c.loc[ch]
        o['changed_by_t'] = {str(k): int(v) for k, v in pd.Series(lv(cb, 't')).value_counts().items()}
        o['changed_columns'] = {k: int((~eqcol(cb[k], cc_[k])).sum()) for k in cb.columns if not eqcol(cb[k], cc_[k]).all()}
        hit = rec18[rec18['hit']]
        tij_hit = set(zip(hit['t'], hit['i'], hit['j']))
        o['changed_tij_in_near_zero'] = int(sum(x in tij_hit for x in zip(lv(cb, 't'), lv(cb, 'i'), lv(cb, 'j'))))
        o['changed_base_expenditure_notna'] = int(cb['Expenditure'].notna().sum()); o['changed_base_expenditure_sum'] = float(cb['Expenditure'].astype(float).sum())
        o['changed_cand_expenditure_all_na'] = bool(cc_['Expenditure'].isna().all())
        o['changed_cand_quantity_all_na'] = bool(cc_['Quantity'].isna().all())
        o['changed_base_quantity_max'] = float(cb['Quantity'].astype(float).max())
        o['changed_base_quantity_kg_na'] = bool(cb['Quantity_kg'].isna().all()) if 'Quantity_kg' in cb else None
        o['changed_derivation_equal'] = bool(eqcol(cb['Derivation'], cc_['Derivation']).all()) if 'Derivation' in cb else None
        o['changed_j_u_s'] = sorted(set(zip(lv(cb, 'j'), lv(cb, 'u'), lv(cb, 's'))))
    # remaining salt-kg rows in 2021-22 (the other 194 records): unchanged?
    cell = rec21[rec21['salt_cell'] & ~rec21['salt_sel']]
    tij_cell = set(zip(cell['t'], cell['i'], cell['j']))
    keep = [k for k in b.index if (str(k[b.index.names.index('t')]), str(k[b.index.names.index('i')]), str(k[b.index.names.index('j')])) in tij_cell]
    o['other_salt_rows_present_in_cand'] = int(pd.Index(keep).isin(c.index).sum())
    o['other_salt_rows_base'] = len(keep)
    for tag in ('Senegal__food_prices', 'Senegal__food_prices__units-unitvalue'):
        bp, cp = load(B, tag), load(C, tag)
        col = 'Price' if 'Price' in bp.columns else bp.columns[0]
        o[tag] = {'base_max': float(bp[col].astype(float).max()), 'cand_max': float(cp[col].astype(float).max()),
                  'cand_above_1e10': int((cp[col].astype(float) > 1e10).sum()),
                  'base_above_1e10': int((bp[col].astype(float) > 1e10).sum())}
    return o


def niger():
    o = {}
    _, (b, c), (ob, oc), ch = diff('Niger__food_acquired')
    rec = pd.read_pickle(os.path.join(C, 'niger_records.pkl'))
    key = 'Niger::food_acquired::computed-last-purchase-quantity'
    cb, cc_ = b.loc[ch], c.loc[ch]
    o['changed_rows'] = len(ch)
    o['changed_columns'] = [k for k in cb.columns if not eqcol(cb[k], cc_[k]).all()]
    bd = cb['Derivation'].astype(object).fillna('').astype(str)
    cd = cc_['Derivation'].astype(object).fillna('').astype(str)
    added = [set(y.split('+')) - set(x.split('+')) for x, y in zip(bd, cd)]
    removed = [set(x.split('+')) - set(y.split('+')) for x, y in zip(bd, cd)]
    o['added_only_key'] = all(a == {key} for a in added)
    o['nothing_removed'] = all(not r - {''} for r in removed)
    o['changed_s'] = sorted(set(lv(cb, 's')))
    o['changed_t'] = sorted(set(lv(cb, 't')))
    stamped = c[c['Derivation'].astype(object).fillna('').astype(str).str.contains(key, regex=False)]
    o['stamped_rows_cand'] = len(stamped)
    o['stamped_expenditure_notna'] = int(stamped['Expenditure'].notna().sum())
    t18 = c[lv(c, 't') == '2018-19']
    p18 = t18[lv(t18, 's') == 'purchased']
    o['purchased_2018_expenditure_total'] = float(p18['Expenditure'].astype(float).sum())
    o['stamped_expenditure_share'] = round(float(stamped['Expenditure'].astype(float).sum()) / o['purchased_2018_expenditure_total'], 4)
    # intended rows, by (t, i, j): flagged records with a purchased quantity
    F = rec[rec['flag'] & (rec['purch'] > 0)]
    U = rec[~rec['flag'] & (rec['purch'] > 0)]
    tijF = set(zip(F['t'], F['i'], F['j']))
    tijU = set(zip(U['t'], U['i'], U['j']))
    st = set(zip(lv(stamped, 't'), lv(stamped, 'i'), lv(stamped, 'j')))
    o['flagged_records_with_purchase'] = len(F)
    o['flagged_tij'] = len(tijF)
    o['stamped_tij'] = len(st)
    o['stamped_tij_not_flagged'] = len(st - tijF)
    o['flagged_tij_not_stamped'] = len(tijF - st)
    unst = p18[~p18['Derivation'].astype(object).fillna('').astype(str).str.contains(key, regex=False)]
    un_tij = set(zip(lv(unst, 't'), lv(unst, 'i'), lv(unst, 'j')))
    amb = tijF & un_tij
    o['unstamped_purchased_rows_sharing_tij_with_flagged'] = len(amb)
    o['of_which_tij_also_has_unflagged_record'] = len(amb & tijU)
    # rows of a (t,i,j) whose ONLY records are flagged must all be stamped
    only_flag = tijF - tijU
    o['tij_with_only_flagged_records'] = len(only_flag)
    o['unstamped_purchased_rows_in_only_flagged_tij'] = int(sum(x in only_flag for x in zip(lv(unst, 't'), lv(unst, 'i'), lv(unst, 'j'))))
    ri = pd.read_pickle(os.path.join(C, 'niger_flag_inputs.pkl'))
    rtij = set(zip(lv(ri, 't'), lv(ri, 'i'), lv(ri, 'j')))
    o['inputs_rows'] = len(ri)
    o['stamped_tij_subset_inputs'] = st <= rtij
    o['other_tables_frame_equal'] = {t: bool(load(B, t).equals(load(C, t))) for t in ('Niger__food_expenditures', 'Niger__food_prices', 'Niger__food_quantities')}
    return o


def uganda():
    o = {}
    _, (b, c), (ob, oc), ch = diff('Uganda__sample')
    cb, cc_ = b.loc[ch], c.loc[ch]
    key = 'Uganda::sample::ea-same-household'
    o['changed_rows'] = len(ch)
    o['changed_columns'] = [k for k in cb.columns if not eqcol(cb[k], cc_[k]).all()]
    o['changed_t'] = sorted(set(lv(cb, 't')))
    o['changed_base_ea_all_na'] = bool(cb['ea'].isna().all())
    o['changed_cand_ea_all_notna'] = bool(cc_['ea'].notna().all())
    # every non-missing base ea unchanged
    common = b.index.intersection(c.index)
    bb, cc2 = b.loc[common], c.loc[common]
    nn = bb['ea'].notna()
    o['nonmissing_base_ea_changed'] = int((~eqcol(bb.loc[nn, 'ea'], cc2.loc[nn, 'ea'])).sum())
    o['v_changed'] = int((~eqcol(bb['v'], cc2['v'])).sum())
    st = cc2['Derivation'].astype(object).fillna('').astype(str).str.contains(key, regex=False)
    o['stamped'] = int(st.sum())
    o['stamped_equals_changed'] = set(common[st.to_numpy()]) == set(ch)
    bd = cb['Derivation'].astype(object).fillna('').astype(str)
    cd = cc_['Derivation'].astype(object).fillna('').astype(str)
    o['derivation_added_only_key'] = all((set(y.split('+')) - set(x.split('+'))) == {key} for x, y in zip(bd, cd))
    o['derivation_base_on_changed'] = {str(k): int(v) for k, v in bd.value_counts().items()}
    for lab, fr in (('base', b), ('cand', c)):
        x = fr[lv(fr, 't') == '2009-10']
        w = x['weight'].astype(float)
        na = x['ea'].isna()
        o[f'{lab}_2009_rows'] = len(x)
        o[f'{lab}_2009_ea_na'] = int(na.sum())
        o[f'{lab}_2009_ea_na_weight_share'] = round(float(w[na].sum() / w.sum()), 5)
        o[f'{lab}_2009_ea_na_weight_missing'] = int((na & w.isna()).sum())
        o[f'{lab}_2009_ea_na_weight_zero'] = int((na & (w == 0)).sum())
        o[f'{lab}_2009_distinct_ea'] = int(x['ea'].nunique())
        o[f'{lab}_all_distinct_ea'] = int(fr['ea'].nunique())
    # filled labels exist elsewhere, and agree with the household's other waves
    filled = cc_['ea'].astype(str)
    fi = lv(cc_, 'i')
    others = c[lv(c, 't') != '2009-10']
    o['filled_labels_seen_in_other_waves'] = bool(set(filled) <= set(others['ea'].dropna().astype(str)))
    oth = others.reset_index()[['i', 't', 'ea']].dropna(subset=['ea'])
    oth['i'] = oth['i'].astype(str); oth['ea'] = oth['ea'].astype(str)
    m = pd.DataFrame({'i': fi, 'fill': filled.to_numpy()}).merge(oth, on='i', how='left')
    has = m['ea'].notna()
    o['filled_households'] = int(pd.Series(fi).nunique())
    o['filled_households_with_other_wave_label'] = int(m.loc[has, 'i'].nunique())
    o['filled_households_disagreeing_with_any_other_wave'] = int(m.loc[has & (m['ea'] != m['fill']), 'i'].nunique())
    o['other_wave_hits_by_t'] = {str(k): int(v) for k, v in m.loc[has].groupby('t')['i'].nunique().items()}
    o['other_wave_disagree_by_t'] = {str(k): int(v) for k, v in m.loc[has & (m['ea'] != m['fill'])].groupby('t')['i'].nunique().items()}
    # positive control on the HHID key: 2009-10 households with their OWN ea vs same-i ea elsewhere
    x9 = c[(lv(c, 't') == '2009-10') & c['ea'].notna()]
    st9 = x9['Derivation'].astype(object).fillna('').astype(str).str.contains(key, regex=False)
    own = x9[~st9.to_numpy()]
    pm = pd.DataFrame({'i': lv(own, 'i'), 'own': own['ea'].astype(str).to_numpy()}).merge(oth, on='i', how='inner')
    o['own_ea_households'] = int(own.shape[0])
    o['own_ea_control_by_t'] = {str(t): {'households': int(g['i'].nunique()), 'disagree': int(g.loc[g['ea'] != g['own'], 'i'].nunique())}
                                for t, g in pm.groupby('t')}
    # status composition
    g = pd.read_pickle(os.path.join(C, 'uganda_gsec1_2009.pkl'))
    if 'hh_status' in g.columns:
        st_ = g.set_index('HHID')['hh_status']
        o['filled_by_hh_status'] = {str(k): int(v) for k, v in st_.reindex(pd.Index(fi)).value_counts(dropna=False).items()}
        left = c[(lv(c, 't') == '2009-10') & c['ea'].isna()]
        o['still_na_by_hh_status'] = {str(k): int(v) for k, v in st_.reindex(pd.Index(lv(left, 'i'))).value_counts(dropna=False).items()}
    # derived function reproduces the served value
    der = pd.read_pickle(os.path.join(C, 'uganda_ea_derived.pkl')).iloc[:, 0]
    der.index = der.index.droplevel('t') if 't' in der.index.names else der.index
    served = pd.Series(filled.to_numpy(), index=pd.Index(fi))
    o['derived_equals_served'] = bool(der.astype(str).sort_index().equals(served.sort_index()))
    o['cluster_features_frame_equal'] = bool(load(B, 'Uganda__cluster_features').equals(load(C, 'Uganda__cluster_features')))
    o['household_characteristics_frame_equal'] = bool(load(B, 'Uganda__household_characteristics').equals(load(C, 'Uganda__household_characteristics')))
    cf = load(C, 'Uganda__cluster_features')
    o['cluster_features_has_ea'] = 'ea' in cf.columns or 'ea' in cf.index.names
    return o




def claims():
    """The numbers the branch's CONTENTS.org / derivations.yml quote, recomputed on both builds."""
    o = {}
    for lab, d in (('base', B), ('cand', C)):
        r = {}
        fa = load(d, 'Mali__food_acquired')
        x = fa[lv(fa, 't') == '2018-19']
        p = x[lv(x, 's') == 'purchased']
        r['mali18_rows'] = len(x); r['mali18_purchased'] = len(p)
        r['mali18_purchased_exp_na'] = int(p['Expenditure'].isna().sum())
        r['mali18_purchased_exp_sum'] = round(float(p['Expenditure'].astype(float).sum()), 2)
        rk = p[(lv(p, 'j') == 'Riz local Gambiaka') & (lv(p, 'u').str.lower() == 'kg')]
        r['mali18_rice_kg_purchased_rows'] = len(rk); r['mali18_rice_kg_purchased_q'] = round(float(rk['Quantity'].astype(float).sum()), 1)
        r['mali_rows_by_t'] = {str(k): int(v) for k, v in pd.Series(lv(fa, 't')).value_counts().items()}
        fa = load(d, 'Senegal__food_acquired')
        for w in ('2018-19', '2021-22'):
            x = fa[lv(fa, 't') == w]
            p = x[lv(x, 's') == 'purchased']
            r[f'sen{w[:4]}_rows'] = len(x)
            r[f'sen{w[:4]}_purchased_exp_sum'] = round(float(p['Expenditure'].astype(float).sum()), 2)
            r[f'sen{w[:4]}_purchased_exp_na'] = int(p['Expenditure'].isna().sum())
            r[f'sen{w[:4]}_inkind_rows'] = int((lv(x, 's') == 'inkind').sum())
        fp = load(d, 'Senegal__food_prices')
        pc = 'Price' if 'Price' in fp.columns else fp.columns[0]
        x = fp[lv(fp, 't') == '2018-19'][pc].astype(float)
        r['sen18_food_prices_max'] = float(x.max()); r['sen18_food_prices_mean'] = round(float(x.mean()), 2)
        fu = load(d, 'Senegal__food_prices__units-unitvalue')
        r['sen_unitvalue_max'] = float(fu[pc].astype(float).max())
        fa = load(d, 'Niger__food_acquired')
        x = fa[lv(fa, 't') == '2018-19']
        p = x[lv(x, 's') == 'purchased']
        st = p['Derivation'].astype(object).fillna('').astype(str).str.contains('computed-last-purchase-quantity', regex=False)
        r['niger18_stamped'] = int(st.sum())
        r['niger18_stamped_with_exp'] = int(p.loc[st, 'Expenditure'].notna().sum())
        tot = float(p['Expenditure'].astype(float).sum())
        r['niger18_stamped_exp_share'] = round(float(p.loc[st, 'Expenditure'].astype(float).sum()) / tot, 4) if tot else None
        s = load(d, 'Uganda__sample')
        x = s[lv(s, 't') == '2009-10']
        r['uga09_ea_na'] = int(x['ea'].isna().sum()); r['uga09_distinct'] = int(x['ea'].nunique())
        r['uga_all_distinct'] = int(s['ea'].nunique())
        o[lab] = r
    return o
run('mali_detail', mali)
run('senegal_detail', senegal)
run('niger_detail', niger)
run('uganda_detail', uganda)
run('claims', claims)
json.dump(OUT, open(os.path.join(os.environ['RESULTS'], 'compare.json'), 'w'), indent=1, default=str)
print('done compare', flush=True)
