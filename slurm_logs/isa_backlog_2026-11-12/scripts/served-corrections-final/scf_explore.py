"""Read-only measurements for the 2026-11-12 final pass.  Aggregates only -> results/explore.json.

Run on the branch head 3ae15b56c (its Niger / Senegal modules), private root.
"""
import collections
import json
import os
import sys
import traceback

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scf_common import RESULTS, guard, load_wave_module, num  # noqa: E402

guard()
from lsms_library import ehcvm  # noqa: E402
from lsms_library import local_tools as tools  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

R = {}


def q(x, ps=(5, 10, 25, 50, 75, 90, 95)):
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if not len(x):
        return None
    return {f'p{p}': float(np.percentile(x, p)) for p in ps} | {'n': int(len(x))}


def loo_medians(v):
    """Leave-one-out median for each element of the SORTED array v."""
    n = len(v)
    m = n - 1
    out = np.full(n, np.nan)
    if m <= 0:
        return out
    p = np.arange(n)

    def at(k):
        return np.where(k < p, v[np.minimum(k, n - 1)], v[np.minimum(k + 1, n - 1)])
    if m % 2:
        out = at(np.full(n, m // 2))
    else:
        out = (at(np.full(n, m // 2 - 1)) + at(np.full(n, m // 2))) / 2
    return out


def niger():
    nig = load_wave_module('Niger', '2018-19')
    data = countries_root() / 'Niger' / '2018-19' / 'Data'
    raw = tools.get_dataframe(str(data / 's07b_me_ner2018.dta'), convert_categoricals=False)
    R['niger_s07b_columns'] = list(map(str, raw.columns))
    lab = ehcvm.inputs_last_purchase('Niger', '2018-19')
    assert len(lab) == len(raw)
    assert np.array_equal(num(lab['s07bq03a']), num(raw['s07bq03a']), equal_nan=True)

    # --- unit identity: does each code carry one key, and the same key in both variables?
    ident = {}
    for code_var, other in (('s07bq03b', 's07bq07b'), ('s07bq03c', 's07bq07c')):
        maps = {}
        for var in (code_var, other):
            df = pd.DataFrame({'c': num(raw[var]), 'k': lab[var].map(ehcvm._unit_key).astype(object)})
            df = df[np.isfinite(df['c'])]
            maps[var] = df.groupby('c')['k'].agg(lambda s: tuple(sorted(set(map(str, s.dropna())))))
        a, b = maps[code_var], maps[other]
        common = a.index.intersection(b.index)
        ident[f'{code_var}~{other}'] = {
            'codes_' + code_var: int(len(a)), 'codes_' + other: int(len(b)), 'common': int(len(common)),
            'multi_key_codes_' + code_var: int((a.map(len) > 1).sum()),
            'multi_key_codes_' + other: int((b.map(len) > 1).sum()),
            'common_codes_key_differs': {str(int(c)): [list(a[c]), list(b[c])] for c in common if a[c] != b[c]},
        }
    R['niger_unit_identity'] = ident

    # --- region source
    region_cols = [c for c in raw.columns if str(c).lower() in ('s00q01', 'region', 'reg')]
    R['niger_s07b_region_columns'] = region_cols
    cover = tools.get_dataframe(str(data / 's00_me_ner2018.dta'), convert_categoricals=False)
    R['niger_cover_has'] = [c for c in ('vague', 'grappe', 'menage', 's00q01', 's00q04') if c in cover.columns]
    ck = pd.MultiIndex.from_arrays([num(cover['grappe']), num(cover['menage'])])
    reg = pd.Series(num(cover['s00q01']), index=ck)
    R['niger_cover_hh_unique'] = bool(not reg.index.duplicated().any())
    g2r = pd.DataFrame({'g': num(cover['grappe']), 'r': num(cover['s00q01'])}).dropna().groupby('g')['r'].nunique()
    R['niger_grappe_regions'] = {'grappes': int(len(g2r)), 'with_more_than_one_region': int((g2r > 1).sum())}
    rk = pd.MultiIndex.from_arrays([num(raw['grappe']), num(raw['menage'])])
    region = reg[~reg.index.duplicated()].reindex(rk).to_numpy()
    R['niger_s07b_records_without_cover_region'] = int(np.isnan(region).sum())
    R['niger_regions'] = int(len(set(region[np.isfinite(region)])))

    q3, q4, q5 = num(raw['s07bq03a']), num(raw['s07bq04']), num(raw['s07bq05'])
    q7, q8 = num(raw['s07bq07a']), num(raw['s07bq08'])
    s6 = num(raw['s07bq06'])
    flag = nig.computed_last_purchase_rows(raw['s07bq07a'])
    pq = np.clip(np.nan_to_num(q3) - np.nan_to_num(q4) - np.nan_to_num(q5), 0, None)
    cu = lab['s07bq03b'].map(ehcvm._unit_key).astype(object).to_numpy()
    cs = lab['s07bq03c'].map(ehcvm._unit_key).astype(object).to_numpy()
    lu = lab['s07bq07b'].map(ehcvm._unit_key).astype(object).to_numpy()
    ls = lab['s07bq07c'].map(ehcvm._unit_key).astype(object).to_numpy()
    match = ehcvm.unit_match(lab['s07bq03b'], lab['s07bq03c'], lab['s07bq07b'], lab['s07bq07c']).to_numpy()
    with np.errstate(divide='ignore', invalid='ignore'):
        own_uv = np.where(q7 > 0, q8 / q7, np.nan)
    own = match & (q7 > 0)
    base_exp = np.where(own, pq * own_uv, np.nan)
    prod = num(raw['s07bq01'])
    grappe = num(raw['grappe'])

    counts = {
        'records': int(len(raw)), 'flagged': int(flag.sum()),
        'flagged_purchased(pq>0)': int((flag & (pq > 0)).sum()),
        'flagged_purchased_own_match': int((flag & (pq > 0) & own).sum()),
        'flagged_purchased_own_match_valued(finite)': int((flag & (pq > 0) & np.isfinite(base_exp)).sum()),
        'purchased_records(pq>0)': int((pq > 0).sum()),
        'base_exp_total_record_level': float(np.nansum(np.where(pq > 0, base_exp, np.nan))),
        'base_exp_flagged_record_level': float(np.nansum(np.where(flag & (pq > 0), base_exp, np.nan))),
        'unflagged_q7pos': int((~flag & (q7 > 0)).sum()),
        'unflagged_q7pos_q8_nan': int((~flag & (q7 > 0) & ~np.isfinite(q8)).sum()),
        'unflagged_q7pos_q8_zero': int((~flag & (q7 > 0) & (q8 == 0)).sum()),
        'unflagged_q7pos_q8_neg': int((~flag & (q7 > 0) & (q8 < 0)).sum()),
        'flagged_q8_nan': int((flag & ~np.isfinite(q8)).sum()),
        'flagged_q8_zero': int((flag & (q8 == 0)).sum()),
        'flagged_s07bq06': {str(k): int(v) for k, v in collections.Counter(np.where(np.isfinite(s6[flag]), s6[flag], -1).astype(int)).items()},
    }
    genuine = (~flag & (q7 > 0) & np.isfinite(q8) & (q8 > 0)
               & pd.notna(lu) & pd.notna(ls) & np.isfinite(prod))
    counts['genuine_pool'] = int(genuine.sum())
    counts['genuine_pool_nonint_q8'] = int((genuine & (np.round(q8) != q8)).sum())
    counts['genuine_pool_no_region'] = int((genuine & ~np.isfinite(region)).sum())
    R['niger_counts'] = counts

    pool = pd.DataFrame({'g': grappe, 'r': region, 'p': prod, 'u': lu, 's': ls,
                         'uv': np.where(genuine, own_uv, np.nan), 'ni': genuine & (np.round(q8) != q8)})[genuine]
    cl = pool.groupby(['g', 'p', 'u', 's'])['uv'].agg(['median', 'size'])
    rg = pool.dropna(subset=['r']).groupby(['r', 'p', 'u', 's'])['uv'].agg(['median', 'size'])
    cl_ni = pool.groupby(['g', 'p', 'u', 's'])['ni'].any()
    rg_ni = pool.dropna(subset=['r']).groupby(['r', 'p', 'u', 's'])['ni'].any()

    target = flag & (pq > 0)
    T = pd.DataFrame({'g': grappe, 'r': region, 'p': prod, 'u': cu, 's': cs, 'own': own, 'pq': pq,
                      'own_uv': own_uv, 'base_exp': base_exp})[target]
    ck = pd.MultiIndex.from_arrays([T['g'], T['p'], T['u'], T['s']])
    rk2 = pd.MultiIndex.from_arrays([T['r'], T['p'], T['u'], T['s']])
    T['cl_med'] = cl['median'].reindex(ck).to_numpy()
    T['cl_n'] = cl['size'].reindex(ck).fillna(0).to_numpy()
    T['rg_med'] = rg['median'].reindex(rk2).to_numpy()
    T['rg_n'] = rg['size'].reindex(rk2).fillna(0).to_numpy()
    T['cl_ni'] = cl_ni.reindex(ck).fillna(False).to_numpy(dtype=bool)
    T['rg_ni'] = rg_ni.reindex(rk2).fillna(False).to_numpy(dtype=bool)
    T['key_missing'] = pd.isna(T['u']).to_numpy() | pd.isna(T['s']).to_numpy()
    floors = {}
    for k in (1, 2, 3, 5):
        lvl = np.where(T['cl_n'] >= k, 'cluster', np.where(T['rg_n'] >= k, 'region', 'none'))
        med = np.where(lvl == 'cluster', T['cl_med'], np.where(lvl == 'region', T['rg_med'], np.nan))
        rev = T['pq'].to_numpy() * med
        d = {}
        for name, sel in (('all', np.ones(len(T), bool)), ('previously_valued', np.isfinite(T['base_exp'].to_numpy())),
                          ('previously_NA', ~np.isfinite(T['base_exp'].to_numpy()))):
            c = collections.Counter(lvl[sel])
            d[name] = {'rows': int(sel.sum()), **{lv: int(c.get(lv, 0)) for lv in ('cluster', 'region', 'none')},
                       'revalued_total': float(np.nansum(rev[sel])),
                       'base_total': float(np.nansum(T['base_exp'].to_numpy()[sel]))}
        pv = np.isfinite(T['base_exp'].to_numpy()) & np.isfinite(med)
        ratio = med[pv] / T['own_uv'].to_numpy()[pv]
        lr = np.abs(np.log(ratio))
        d['ratio_local_over_producer_on_previously_valued'] = q(ratio) | {
            'within_10pct': float(np.mean(lr < np.log(1.1))), 'within_25pct': float(np.mean(lr < np.log(1.25))),
            'within_2x': float(np.mean(lr < np.log(2))), 'median_abs_log': float(np.median(lr))}
        d['cells_used_with_a_nonint_q8_report'] = int(((lvl == 'cluster') & T['cl_ni'].to_numpy()).sum()
                                                      + ((lvl == 'region') & T['rg_ni'].to_numpy()).sum())
        floors[str(k)] = d
    R['niger_floors'] = floors
    R['niger_target_key_missing'] = int(T['key_missing'].sum())
    R['niger_target_cons_unit_ne_last_unit'] = int((~T['own']).sum())

    # --- leave-one-out: cluster median of the OTHER genuine reports vs region median, by n_others
    pool = pool.dropna(subset=['r']).copy()
    pool['cl_loo'] = np.nan
    pool['rg_loo'] = np.nan
    pool['cl_n_others'] = 0
    for cols, out, nout in ((['g', 'p', 'u', 's'], 'cl_loo', 'cl_n_others'), (['r', 'p', 'u', 's'], 'rg_loo', None)):
        for _, idx in pool.groupby(cols).indices.items():
            vals = pool['uv'].to_numpy()[idx]
            order = np.argsort(vals, kind='stable')
            loo = loo_medians(vals[order])
            res = np.empty(len(idx))
            res[order] = loo
            pool.iloc[idx, pool.columns.get_loc(out)] = res
            if nout:
                pool.iloc[idx, pool.columns.get_loc(nout)] = len(idx) - 1
    loo = {}
    for k in (1, 2, 3, 4, 5, 6, 8, 10):
        sel = (pool['cl_n_others'] == k) if k < 10 else (pool['cl_n_others'] >= 10)
        sel = sel & np.isfinite(pool['cl_loo']) & np.isfinite(pool['rg_loo'])
        if not sel.any():
            continue
        ecl = np.abs(np.log(pool.loc[sel, 'cl_loo'] / pool.loc[sel, 'uv']))
        erg = np.abs(np.log(pool.loc[sel, 'rg_loo'] / pool.loc[sel, 'uv']))
        loo[str(k) if k < 10 else '10+'] = {
            'records': int(sel.sum()), 'cluster_median_abs_log': float(ecl.median()),
            'region_median_abs_log': float(erg.median()),
            'cluster_within_25pct': float((ecl < np.log(1.25)).mean()),
            'region_within_25pct': float((erg < np.log(1.25)).mean()),
            'cluster_better_share': float((ecl < erg).mean())}
    R['niger_leave_one_out'] = loo
    rsize = pool.groupby(['r', 'p', 'u', 's']).size()
    R['niger_region_cell_sizes'] = q(rsize.to_numpy())

    # --- served-key sharing among purchased records
    idx = lab.index.to_frame(index=False)
    key = pd.DataFrame({'i': idx['i'].astype(str), 'j': idx['j'].astype(str),
                        'u': pd.Series(cu).astype(str), 's': pd.Series(cs).astype(str)})
    pur = pq > 0
    kk = key[pur]
    dup = kk.duplicated(keep=False).to_numpy()
    fl = flag[pur]
    grp = kk.assign(f=fl).groupby(['i', 'j', 'u', 's'])['f'].agg(['size', 'sum'])
    R['niger_shared_keys'] = {'purchased_records_in_duplicated_keys': int(dup.sum()),
                              'flagged_in_duplicated_keys': int((dup & fl).sum()),
                              'groups_mixing_flagged_and_unflagged': int(((grp['size'] > 1) & (grp['sum'] > 0) & (grp['sum'] < grp['size'])).sum())}


def senegal():
    sen21 = load_wave_module('Senegal', '2021-22')
    data = countries_root() / 'Senegal' / '2021-22' / 'Data'
    raw = tools.get_dataframe(str(data / 's07b_me_sen2021.dta'), convert_categoricals=False)
    c = 0.0199997
    qa = num(raw['s07bq03a'])
    cell = (num(raw['s07bq01']) == 139) & (num(raw['s07bq03b']) == 100) & (num(raw['s07bq03c']) == 0)
    with np.errstate(invalid='ignore', divide='ignore'):
        m = qa / c
        dev = np.abs(m - np.round(m))
    base = cell & (qa > 0)
    out = {'salt_kg_records_positive': int(base.sum())}
    for tol in (1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-9, 1e-12):
        out[f'new_rule_tol_{tol:g}'] = int((base & (dev < tol) & (np.round(m) >= 1)).sum())
    new = base & (dev < 1e-9) & (np.round(m) >= 1)
    old_raw = sen21._salt_raw()
    old = sen21.salt_per_capita_rows(old_raw)
    out['old_selector'] = int(old.sum())
    out['new_selector'] = int(new.sum())
    out['identical_masks'] = bool(np.array_equal(old, new))
    out['max_dev_multiple_selected'] = float(dev[new].max())
    out['max_dev_kg_selected'] = float((dev[new] * c).max())
    out['min_dev_multiple_unselected_salt_kg'] = float(dev[base & ~new].min())
    out['dev_unselected_salt_kg_quantiles'] = q(dev[base & ~new], ps=(0, 1, 5, 50))
    hh = num(old_raw['hhsize'])
    out['multiple_equals_hhsize_on_selected'] = int((np.round(m[new]) == hh[new]).sum())
    out['multiple_range_selected'] = [int(np.round(m[new]).min()), int(np.round(m[new]).max())]
    anyprod = (qa > 0) & (dev < 1e-9) & (np.round(m) >= 1)
    out['any_product_unit_at_constant'] = int(anyprod.sum())
    out['non_salt_kg_at_constant'] = int((anyprod & ~cell).sum())
    # typed values: the deviation of round kg amounts
    out['dev_of_typed_values'] = {str(v): float(abs(v / c - round(v / c))) for v in (0.02, 0.05, 0.1, 0.25, 0.5, 1.0)}
    R['senegal_salt'] = out
    # 2018-19, informative
    d18 = countries_root() / 'Senegal' / '2018-19' / 'Data'
    raw18 = tools.get_dataframe(str(d18 / 's07b_me_sen2018.dta'), convert_categoricals=False)
    q18 = num(raw18['s07bq03a'])
    with np.errstate(invalid='ignore', divide='ignore'):
        m18 = q18 / c
        dev18 = np.abs(m18 - np.round(m18))
    R['senegal_2018_any_record_at_constant'] = int(((q18 > 0) & (dev18 < 1e-9) & (np.round(m18) >= 1)).sum())


for name, fn in (('niger', niger), ('senegal', senegal)):
    try:
        fn()
    except Exception as e:
        R[f'{name}_error'] = f'{type(e).__name__}: {e}'
        R[f'{name}_tb'] = traceback.format_exc()[-3000:]
        print(R[f'{name}_tb'], flush=True)
    json.dump(R, open(os.path.join(RESULTS, 'explore.json'), 'w'), indent=1, default=str)
    print('wrote', name, flush=True)
print('done explore', flush=True)
