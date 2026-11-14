"""Raw re-derivations at HEAD (06bac7429), independent of the author's helpers except
ehcvm._unit_key (the #876 unit identity, by definition of the rule).

Usage: python checks.py <frames_head_dir> <prev_clone_dir>
Aggregates only -> $RESULTS/checks.json (rewritten after every stage).
"""
import collections
import importlib.util
import json
import os
import sys
import traceback
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')
FR, PREVCLONE = sys.argv[1], sys.argv[2]

import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import countries_root, data_root
dr = os.path.realpath(str(data_root()))
assert dr.startswith('/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam2/'), dr
from lsms_library import local_tools as lt
from lsms_library.ehcvm import _unit_key
import lsms_library as ll

RES = os.environ['RESULTS']
OUT = {'lsms_library': lsms_library.__file__, 'data_root': dr}


def dump():
    json.dump(OUT, open(os.path.join(RES, 'checks.json'), 'w'), indent=1, default=str)


def run(name, fn):
    try:
        OUT[name] = fn()
    except Exception as e:
        OUT[name] = {'ERROR': f'{type(e).__name__}: {e}'[:1500], 'tb': traceback.format_exc()[-3000:]}
    print(name, json.dumps(OUT[name], default=str)[:6000], flush=True)
    dump()


def num(s):
    return pd.to_numeric(pd.Series(np.asarray(s)), errors='coerce').to_numpy(dtype=float)


def lv(df, name):
    return df.index.get_level_values(name).astype(str)


def load_mod(root, country, wave, tag):
    path = os.path.join(root, 'lsms_library', 'countries', country, wave, '_', 'mapping.py')
    spec = importlib.util.spec_from_file_location(f'_rt2_{tag}_{country}_{wave.replace("-", "_")}', path)
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, os.path.join(root, 'lsms_library', 'countries', country, '_'))
    try:
        spec.loader.exec_module(m)
    finally:
        sys.path.pop(0)
    return m


def hkey(f):
    return pd.MultiIndex.from_arrays([num(f['grappe']), num(f['menage'])])


def eq_nan(a, b, rtol=1e-12):
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    return len(a) == len(b) and bool(np.allclose(a, b, rtol=rtol, atol=0, equal_nan=True))


# ------------------------------------------------------------------ Senegal salt
def senegal_salt():
    o = {}
    D = countries_root() / 'Senegal' / '2021-22' / 'Data'
    raw = lt.get_dataframe(str(D / 's07b_me_sen2021.dta'), convert_categoricals=False)
    wel = lt.get_dataframe(str(D / 'ehcvm_welfare_sen2021.dta'), convert_categoricals=False)
    size = pd.Series(num(wel['hhsize']), index=hkey(wel))
    o['records'] = len(raw)
    o['welfare_key_unique'] = not bool(size.index.duplicated().any())
    hh = size[~size.index.duplicated()].reindex(hkey(raw)).to_numpy(dtype=float)
    q = num(raw['s07bq03a'])
    cell = (num(raw['s07bq01']) == 139) & (num(raw['s07bq03b']) == 100) & (num(raw['s07bq03c']) == 0)
    C = 0.0199997
    with np.errstate(invalid='ignore', divide='ignore'):
        old = cell & (q > 0) & (np.abs(q - C * hh) < 1e-9)
        mult = q / C
        n = np.round(mult)
        dist = np.abs(mult - n)
        new_mine = cell & (q > 0) & (n >= 1) & (dist < 1e-9)
    o['salt_kg_positive'] = int((cell & (q > 0)).sum())
    o['old_rule_count'] = int(old.sum())
    o['new_rule_mine_count'] = int(new_mine.sum())
    o['old_equals_new_mine'] = bool(np.array_equal(old, new_mine))
    head = load_mod(top, 'Senegal', '2021-22', 'head')
    new_lib = np.asarray(head.salt_per_capita_rows(raw), dtype=bool)
    o['new_rule_lib_count'] = int(new_lib.sum())
    o['new_lib_equals_old'] = bool(np.array_equal(new_lib, old))
    o['new_lib_equals_new_mine'] = bool(np.array_equal(new_lib, new_mine))
    o['head_constants'] = {'SALT_KG_PER_PERSON': head.SALT_KG_PER_PERSON, '_SALT_TOLERANCE': head._SALT_TOLERANCE,
                           '_SALT_CELL': head._SALT_CELL}
    try:
        prev = load_mod(PREVCLONE, 'Senegal', '2021-22', 'prev')
        prev_raw = prev._salt_raw()
        prev_sel = np.asarray(prev.salt_per_capita_rows(prev_raw), dtype=bool)
        o['prev_lib_count'] = int(prev_sel.sum())
        o['prev_lib_equals_new_lib'] = bool(np.array_equal(prev_sel, new_lib))
    except Exception as e:
        o['prev_lib'] = f'ERR {type(e).__name__}: {e}'[:500]
    o['selected_multiple_equals_hhsize'] = int((n[new_lib] == hh[new_lib]).sum())
    o['selected_without_hhsize'] = int(np.isnan(hh[new_lib]).sum())
    o['selected_max_dist'] = float(dist[new_lib].max())
    other = cell & (q > 0) & ~new_lib
    o['nearest_other_salt_kg_dist'] = float(np.nanmin(np.where(n[other] >= 1, dist[other], np.inf)))
    o['tolerance_sweep_new_rule'] = {f'{t:g}': int((cell & (q > 0) & (n >= 1) & (dist < t)).sum())
                                     for t in (1e-15, 1e-14, 1e-12, 1e-10, 1e-9, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2)}
    with np.errstate(invalid='ignore', divide='ignore'):
        o['any_product_unit_multiple_1e9'] = int(((q > 0) & (n >= 1) & (dist < 1e-9)).sum())
        o['any_product_unit_multiple_1e9_not_salt_cell'] = int(((q > 0) & (n >= 1) & (dist < 1e-9) & ~cell).sum())
    return o


# ------------------------------------------------------------------ Niger #1708
LK = 'Niger::food_acquired::flagged-purchase-local-median'
FK = 'Niger::food_acquired::computed-last-purchase-quantity'
NIG = {}


def sig6(x):
    return np.array([float(f'{v:.6g}') if np.isfinite(v) else np.nan for v in np.asarray(x, dtype=float)])


def niger_raw():
    o = {}
    D18 = countries_root() / 'Niger' / '2018-19' / 'Data'
    rc = lt.get_dataframe(str(D18 / 's07b_me_ner2018.dta'), convert_categoricals=False)
    rl = lt.get_dataframe(str(D18 / 's07b_me_ner2018.dta'))
    cov = lt.get_dataframe(str(D18 / 's00_me_ner2018.dta'), convert_categoricals=False)
    o['records'] = len(rc)
    o['label_read_aligned'] = len(rl) == len(rc) and bool(np.array_equal(num(rc['s07bq07a']), num(rl['s07bq07a']), equal_nan=True))
    reg = pd.Series(num(cov['s00q01']), index=hkey(cov))
    o['cover_rows'] = len(cov)
    o['cover_dup_households'] = int(reg.index.duplicated().sum())
    region = reg[~reg.index.duplicated()].reindex(hkey(rc)).to_numpy(dtype=float)
    o['records_without_region'] = int(np.isnan(region).sum())
    grappe = num(rc['grappe'])
    gr = pd.DataFrame({'g': grappe, 'r': region}).dropna().groupby('g')['r'].nunique()
    o['grappes'] = int(len(gr))
    o['grappes_in_two_regions'] = int((gr > 1).sum())
    V = {c: num(rc[c]) for c in ('s07bq01', 's07bq03a', 's07bq04', 's07bq05', 's07bq06', 's07bq07a', 's07bq08')}
    K = {c: np.array([_unit_key(v) for v in rl[c].astype(object).to_numpy()], dtype=object)
         for c in ('s07bq03b', 's07bq03c', 's07bq07b', 's07bq07c')}
    NIG.update(rc=rc, rl=rl, region=region, grappe=grappe, V=V, K=K)
    q7, q8, q6 = V['s07bq07a'], V['s07bq08'], V['s07bq06']
    with np.errstate(invalid='ignore', divide='ignore'):
        flag = (q7 > 0) & (np.abs(4 * q7 - np.round(4 * q7)) >= 1e-6)
    NIG['flag'] = flag
    o['q7_positive'] = int((q7 > 0).sum())
    o['flagged'] = int(flag.sum())
    o['flagged_by_s07bq06'] = {str(k): int(v) for k, v in pd.Series(q6[flag]).value_counts(dropna=False).items()}
    o['s07bq06_codes_all'] = {str(k): int(v) for k, v in pd.Series(q6).value_counts(dropna=False).items()}
    return o


def nn(a):
    return np.array([x is not None for x in a], dtype=bool)


def tables(area, mask, prod, lu, ls, uv):
    df = pd.DataFrame({'a': area[mask], 'p': prod[mask], 'u': lu[mask], 's': ls[mask], 'uv': uv[mask]})
    df = df[np.isfinite(df['a'].to_numpy(dtype=float))]
    g = df.groupby(['a', 'p', 'u', 's'], sort=False)['uv']
    med, size = g.median(), g.size()
    return dict(zip(med.index, zip(med.to_numpy(dtype=float), size.reindex(med.index).to_numpy())))


def revalue(pool, target, cl_floor=3, rg_floor=3):
    V, K, grappe, region = NIG['V'], NIG['K'], NIG['grappe'], NIG['region']
    prod = V['s07bq01']
    cu, cs, lu, ls = K['s07bq03b'], K['s07bq03c'], K['s07bq07b'], K['s07bq07c']
    Tc = tables(grappe, pool, prod, lu, ls, NIG['uvg'])
    Tr = tables(region, pool, prod, lu, ls, NIG['uvg'])
    N = len(prod)
    val = np.full(N, np.nan)
    lev = np.full(N, None, dtype=object)
    ncl = np.zeros(N, dtype=int)
    nrg = np.zeros(N, dtype=int)
    for r in np.flatnonzero(target):
        if cu[r] is None or cs[r] is None or not np.isfinite(prod[r]):
            continue
        k = (prod[r], cu[r], cs[r])
        c = Tc.get((grappe[r],) + k) if np.isfinite(grappe[r]) else None
        g = Tr.get((region[r],) + k) if np.isfinite(region[r]) else None
        ncl[r] = c[1] if c else 0
        nrg[r] = g[1] if g else 0
        if c and c[1] >= cl_floor:
            val[r], lev[r] = c[0], 'cluster'
        elif g and g[1] >= rg_floor:
            val[r], lev[r] = g[0], 'region'
    return val, lev, ncl, nrg


def niger_revaluation():
    o = {}
    V, K, flag = NIG['V'], NIG['K'], NIG['flag']
    q3, q4, q5, q7, q8, prod = V['s07bq03a'], V['s07bq04'], V['s07bq05'], V['s07bq07a'], V['s07bq08'], V['s07bq01']
    cu, cs, lu, ls = K['s07bq03b'], K['s07bq03c'], K['s07bq07b'], K['s07bq07c']
    with np.errstate(invalid='ignore', divide='ignore'):
        net = q3 - np.nan_to_num(q5)
        purch = np.clip(np.nan_to_num(net, nan=0.0) - np.nan_to_num(q4), 0, None)
        match = nn(cu) & nn(lu) & nn(cs) & nn(ls) & (cu == lu) & (cs == ls)
        uv_own = np.where(q7 > 0, q8 / np.where(q7 > 0, q7, np.nan), np.nan)
        v876 = np.where(match, purch * uv_own, np.nan)
        keep = (purch > 0) | (v876 > 0)
        genuine = (~flag & (q7 > 0) & np.isfinite(q8) & (q8 > 0) & np.isfinite(prod) & nn(lu) & nn(ls))
        uvg = np.where(genuine, q8 / np.where(q7 > 0, q7, np.nan), np.nan)
    NIG.update(purch=purch, v876=v876, keep=keep, genuine=genuine, uvg=uvg, uv_own=uv_own, match=match)
    scope = flag & (purch > 0) & np.isfinite(v876)
    NIG['scope'] = scope
    fp = flag & keep
    o['flagged_served_purchased_records'] = int(fp.sum())
    o['scope_previously_valued'] = int(scope.sum())
    o['flagged_purchased_876_na'] = int((fp & ~np.isfinite(v876)).sum())
    o['of_which_unit_mismatch'] = int((fp & ~np.isfinite(v876) & ~match).sum())
    o['of_which_units_match_but_no_q8'] = int((fp & ~np.isfinite(v876) & match).sum())
    o['genuine_reports'] = int(genuine.sum())
    o['genuine_intersect_flagged'] = int((genuine & flag).sum())
    val, lev, ncl, nrg = revalue(genuine, scope)
    NIG.update(val=val, lev=lev, ncl=ncl, nrg=nrg)
    new = np.where(scope, purch * val, np.nan)
    NIG['new'] = new
    o['levels'] = {str(k): int(v) for k, v in pd.Series(lev[scope]).value_counts(dropna=False).items()}
    na = scope & ~np.isfinite(new)
    o['na'] = int(na.sum())
    o['na_region_reports_0'] = int((na & (nrg == 0)).sum())
    o['na_region_reports_1_2'] = int((na & (nrg >= 1) & (nrg <= 2)).sum())
    reg = scope & (lev == 'region')
    o['region_values_by_reports'] = {'3-4': int((reg & (nrg <= 4)).sum()), '5-9': int((reg & (nrg >= 5) & (nrg <= 9)).sum()),
                                     '10+': int((reg & (nrg >= 10)).sum())}
    o['flagged_exp_before_876'] = round(float(np.nansum(v876[fp])), 2)
    o['flagged_exp_after'] = round(float(np.nansum(new[scope])), 2)
    valued = scope & np.isfinite(new)
    ratio = val[valued] / uv_own[valued]
    o['valued_after'] = int(valued.sum())
    o['ratio_new_over_producer_uv'] = {'median': float(np.median(ratio)), 'exact_1e-9': round(float(np.mean(np.abs(ratio - 1) < 1e-9)), 4),
                                       'p10': float(np.quantile(ratio, .1)), 'p90': float(np.quantile(ratio, .9)),
                                       'within_25pct': round(float(np.mean(np.abs(ratio - 1) <= .25)), 4)}
    for cf, rf in ((3, 3), (3, 1), (1, 1)):
        v2, _, _, _ = revalue(genuine, fp, cf, rf)
        o[f'all_flagged_floors_cl{cf}_rg{rf}'] = {'na': int((fp & ~np.isfinite(v2)).sum()),
                                                   'total': round(float(np.nansum(purch[fp] * v2[fp])), 2)}
    return o


def niger_served():
    o = {}
    rc, scope, new, lev = NIG['rc'], NIG['scope'], NIG['new'], NIG['lev']
    fa = pd.read_pickle(os.path.join(FR, 'Niger__food_acquired.pkl'))
    t, s = lv(fa, 't'), lv(fa, 's')
    o['served_rows_by_t'] = {str(k): int(v) for k, v in pd.Series(t).value_counts().sort_index().items()}
    p18 = fa[(t == '2018-19') & (s == 'purchased')]
    der = p18['Derivation'].astype(object).fillna('').astype(str)
    hasL = der.str.contains(LK, regex=False).to_numpy()
    hasF = der.str.contains(FK, regex=False).to_numpy()
    o['purchased_rows_2018'] = len(p18)
    o['purchased_exp_total_2018'] = round(float(p18['Expenditure'].astype(float).sum()), 2)
    o['rows_with_flag_key'] = int(hasF.sum())
    o['rows_with_local_key'] = int(hasL.sum())
    o['local_without_flag'] = int((hasL & ~hasF).sum())
    o['flag_only_rows'] = int((hasF & ~hasL).sum())
    o['flag_only_exp_notna'] = int(p18.loc[hasF & ~hasL, 'Expenditure'].notna().sum())
    o['local_key_rows_exp_na'] = int(p18.loc[hasL, 'Expenditure'].isna().sum())
    o['flagged_rows_exp_sum'] = round(float(p18.loc[hasF, 'Expenditure'].astype(float).sum()), 2)
    o['flagged_rows_with_exp'] = int(p18.loc[hasF, 'Expenditure'].notna().sum())
    allk = set(fa['Derivation'].astype(object).dropna().astype(str).str.split('+').explode())
    o['keys_on_any_row_other_waves_with_local'] = int(fa.loc[t != '2018-19', 'Derivation'].astype(object).fillna('').astype(str)
                                                     .str.contains(LK, regex=False).sum())
    o['local_key_on_non_purchased'] = int(fa.loc[s != 'purchased', 'Derivation'].astype(object).fillna('').astype(str)
                                          .str.contains(LK, regex=False).sum())
    o['n_distinct_keys'] = len(allk)
    c = ll.Country('Niger')
    inputs = c.derivation_inputs(LK, wave='2018-19')
    o['inputs_rows'] = len(inputs)
    o['inputs_index'] = list(inputs.index.names)
    o['inputs_columns'] = list(inputs.columns)
    o['inputs_flagged'] = int(inputs['flagged'].sum())
    o['inputs_aligned_with_raw'] = len(inputs) == len(rc) and all(
        np.array_equal(num(inputs[k]), num(rc[k]), equal_nan=True) for k in ('s07bq03a', 's07bq04', 's07bq05', 's07bq07a', 's07bq08', 's07bq01'))
    o['inputs_region_equals_mine'] = bool(np.array_equal(num(inputs['s00q01']), NIG['region'], equal_nan=True))
    o['inputs_flag_equals_mine'] = bool(np.array_equal(inputs['flagged'].to_numpy(dtype=bool), NIG['flag']))
    for k in ('s07bq03b', 's07bq03c', 's07bq07b', 's07bq07c'):
        theirs = np.array([_unit_key(v) for v in inputs[k].astype(object).to_numpy()], dtype=object)
        mine = NIG['K'][k]
        o[f'unit_key_agree_{k}'] = int(sum(a == b for a, b in zip(theirs, mine)))
    rec = c.derivations()[LK]
    o['record'] = {'rows': str(getattr(rec, 'rows', None)), 'columns': getattr(rec, 'columns', None),
                   'waves': getattr(rec, 'waves', None), 'function': getattr(rec, 'function', None),
                   'inputs': getattr(rec, 'inputs', None), 'raw_variables': getattr(rec, 'raw_variables', None),
                   'n_assumptions': len(getattr(rec, 'assumptions', []) or []),
                   'bases': [(a.get('basis') if isinstance(a, dict) else getattr(a, 'basis', None))
                             for a in (getattr(rec, 'assumptions', []) or [])]}
    o['raw_variables_in_inputs'] = sorted(set(rec.raw_variables) - set(inputs.columns))
    derived = rec.resolve()(inputs)
    pos = np.flatnonzero(scope)
    o['derived_rows'] = len(derived)
    o['derived_positional_exp_equals_mine'] = eq_nan(num(derived['Expenditure']), new[pos])
    o['derived_level_equals_mine'] = [None if pd.isna(x) else str(x) for x in derived['level'].astype(object)] == [None if x is None else x for x in lev[pos]]
    o['derived_reports_equal_mine'] = bool(np.array_equal(derived['cluster_reports'].to_numpy(), NIG['ncl'][pos])
                                           and np.array_equal(derived['region_reports'].to_numpy(), NIG['nrg'][pos]))
    idx = inputs.index
    tij = list(zip(idx.get_level_values('t').astype(str), idx.get_level_values('i').astype(str), idx.get_level_values('j').astype(str)))

    def grouped(keys, values):
        d = collections.defaultdict(list)
        for k, v in zip(keys, values):
            d[k].append(float(v))
        return {k: sorted(v, key=lambda x: (np.isnan(x), x)) for k, v in d.items()}

    mine = grouped([tij[r] for r in pos], new[pos])
    sL = p18[hasL]
    served = grouped(list(zip(lv(sL, 't'), lv(sL, 'i'), lv(sL, 'j'))), num(sL['Expenditure']))
    o['mine_keys'] = len(mine)
    o['served_local_keys'] = len(served)
    o['key_sets_equal'] = set(mine) == set(served)
    bad = [k for k in set(mine) & set(served) if not eq_nan(mine[k], served[k], rtol=1e-9)]
    o['keys_value_mismatch'] = len(bad)
    o['mine_rows'] = sum(map(len, mine.values()))
    o['served_rows'] = sum(map(len, served.values()))
    o['multi_row_keys'] = int(sum(len(v) > 1 for v in served.values()))
    # flagged purchased records (all scope) -> served flag key set
    fp = NIG['flag'] & NIG['keep']
    mineF = set(tij[r] for r in np.flatnonzero(fp))
    sF = p18[hasF]
    servF = set(zip(lv(sF, 't'), lv(sF, 'i'), lv(sF, 'j')))
    o['flag_key_tij_equals_flagged_records'] = mineF == servF
    o['flag_tij_only_mine'] = len(mineF - servF)
    o['flag_tij_only_served'] = len(servF - mineF)
    return o


def niger_leakage():
    o = {}
    V, K, flag, genuine, uvg = NIG['V'], NIG['K'], NIG['flag'], NIG['genuine'], NIG['uvg']
    q6, q7, q8, prod = V['s07bq06'], V['s07bq07a'], V['s07bq08'], V['s07bq01']
    region, scope, new, lev, purch = NIG['region'], NIG['scope'], NIG['new'], NIG['lev'], NIG['purch']
    cu, cs, lu, ls = K['s07bq03b'], K['s07bq03c'], K['s07bq07b'], K['s07bq07c']
    disabled = (q7 > 0) & ~np.isin(q6, [1, 2, 3])
    o['disabled_but_filled'] = int(disabled.sum())
    o['disabled_but_filled_with_q8'] = int((disabled & np.isfinite(q8) & (q8 > 0)).sum())
    o['disabled_but_filled_in_genuine'] = int((disabled & genuine).sum())
    o['disabled_and_flagged'] = int((disabled & flag).sum())
    nonint = genuine & (np.abs(q8 - np.round(q8)) > 1e-9)
    o['genuine_nonint_q8'] = int(nonint.sum())
    # P: modal flagged unit value per (region, product, last unit, last size)
    fl = flag & np.isfinite(q8) & (q8 > 0) & nn(lu) & nn(ls) & np.isfinite(region)
    uvf = np.where(fl, q8 / np.where(q7 > 0, q7, np.nan), np.nan)
    dfP = pd.DataFrame({'a': region[fl], 'p': prod[fl], 'u': lu[fl], 's': ls[fl], 'v': sig6(uvf[fl])})
    mode = dfP.groupby(['a', 'p', 'u', 's'])['v'].agg(lambda x: x.value_counts().idxmax())
    nflag = dfP.groupby(['a', 'p', 'u', 's']).size()
    P = mode.to_dict()

    def p_of(mask, uu, ss):
        out = np.full(len(prod), np.nan)
        for r in np.flatnonzero(mask):
            if np.isfinite(region[r]) and uu[r] is not None and ss[r] is not None:
                out[r] = P.get((region[r], prod[r], uu[r], ss[r]), np.nan)
        return out
    Pf = p_of(fl, lu, ls)
    o['flagged_priced'] = int(fl.sum())
    o['flagged_at_P'] = int((fl & (np.abs(uvf / Pf - 1) < 1e-6)).sum())
    o['P_cells'] = int(len(P))
    Pg = p_of(genuine, lu, ls)
    inP = genuine & np.isfinite(Pg)
    atP = inP & (np.abs(uvg / Pg - 1) < 1e-6)
    o['genuine_in_P_cells'] = int(inP.sum())
    o['genuine_at_P'] = int(atP.sum())
    o['genuine_at_P_share_of_P_cells'] = round(float(atP.sum() / max(inP.sum(), 1)), 4)
    o['nonint_in_P_cells'] = int((nonint & inP).sum())
    o['nonint_at_P'] = int((nonint & atP).sum())
    # estimate of computed values that landed ON the grid: q7 = q8 / P on the grid
    gi = np.flatnonzero(inP & ~atP)
    dg = pd.DataFrame({'a': region[gi], 'p': prod[gi], 'u': lu[gi], 's': ls[gi], 'x': q8[gi] / Pg[gi]})
    dg['on'] = (np.abs(4 * dg['x'] - np.round(4 * dg['x'])) < 1e-6)
    st = dg.groupby(['a', 'p', 'u', 's'])['on'].agg(['mean', 'size'])
    st = st[(st['size'] >= 5) & (st['mean'] < 1)]
    nf = nflag.reindex(st.index).fillna(0).astype(float)
    o['estimated_on_grid_constructions'] = round(float((nf * st['mean'] / (1 - st['mean'])).sum()), 1)
    o['estimate_cells_used'] = int(len(st))
    o['estimate_flagged_in_cells_used'] = int(nf.sum())
    o['estimate_p_on_median'] = float(st['mean'].median()) if len(st) else None
    # does a valued row's median equal its cell's P?
    Ps = p_of(scope, cu, cs)
    valued = scope & np.isfinite(new)
    o['valued_rows_median_equals_P'] = int((valued & (np.abs(NIG['val'] / Ps - 1) < 1e-6)).sum())
    o['valued_rows'] = int(valued.sum())

    def total(v):
        return round(float(np.nansum(purch[scope] * v[scope])), 2)
    base_total = total(NIG['val'])
    o['revalued_total'] = base_total
    for name, pool in (('drop_nonint', genuine & ~nonint), ('drop_atP', genuine & ~atP),
                       ('drop_nonint_and_atP', genuine & ~nonint & ~atP)):
        v2, l2, _, _ = revalue(pool, scope)
        n2 = np.where(scope, purch * v2, np.nan)
        same = np.isclose(n2, new, rtol=1e-12, atol=0, equal_nan=True)
        o[name] = {'total': total(v2), 'delta_vs_revalued': round(total(v2) - base_total, 2),
                   'rows_changed': int((scope & ~same).sum()), 'na': int((scope & ~np.isfinite(n2)).sum()),
                   'pool': int(pool.sum())}
    # placebo: at-mode share of genuine reports, 2018-19 vs 2021-22 (same instrument, no construction)
    def at_mode(area, pr, uu, ss, uv, mask, minn=5):
        df = pd.DataFrame({'a': area[mask], 'p': pr[mask], 'u': uu[mask], 's': ss[mask], 'v': sig6(uv[mask])})
        g = df.groupby(['a', 'p', 'u', 's'])['v']
        n_ = g.transform('size')
        md = g.transform(lambda x: x.value_counts().idxmax())
        ok = n_ >= minn
        return {'reports': int(ok.sum()), 'at_mode': int(((np.abs(df['v'] / md - 1) < 1e-6) & ok).sum()),
                'share': round(float(((np.abs(df['v'] / md - 1) < 1e-6) & ok).sum() / max(ok.sum(), 1)), 4)}
    m18 = genuine & np.isfinite(region)
    o['at_mode_2018_genuine'] = at_mode(region, prod, lu, ls, uvg, m18)
    o['at_mode_2018_genuine_P_cells'] = at_mode(region, prod, lu, ls, uvg, m18 & inP)
    o['at_mode_2018_genuine_non_P_cells'] = at_mode(region, prod, lu, ls, uvg, m18 & ~inP)
    try:
        D21 = countries_root() / 'Niger' / '2021-22' / 'Data'
        c21 = lt.get_dataframe(str(D21 / 's07b_me_ner2021.dta'), convert_categoricals=False)
        l21 = lt.get_dataframe(str(D21 / 's07b_me_ner2021.dta'))
        v21 = lt.get_dataframe(str(D21 / 's00_me_ner2021.dta'), convert_categoricals=False)
        r21 = pd.Series(num(v21['s00q01']), index=hkey(v21))
        reg21 = r21[~r21.index.duplicated()].reindex(hkey(c21)).to_numpy(dtype=float)
        a7, a8, ap = num(c21['s07bq07a']), num(c21['s07bq08']), num(c21['s07bq01'])
        b_lu = np.array([_unit_key(v) for v in l21['s07bq07b'].astype(object).to_numpy()], dtype=object)
        b_ls = np.array([_unit_key(v) for v in l21['s07bq07c'].astype(object).to_numpy()], dtype=object)
        with np.errstate(invalid='ignore', divide='ignore'):
            ongrid = (a7 > 0) & (np.abs(4 * a7 - np.round(4 * a7)) < 1e-6)
            g21 = ongrid & np.isfinite(a8) & (a8 > 0) & np.isfinite(ap) & nn(b_lu) & nn(b_ls) & np.isfinite(reg21)
            uv21 = np.where(g21, a8 / np.where(a7 > 0, a7, np.nan), np.nan)
        o['records_2021'] = len(c21)
        o['records_2021_without_region'] = int(np.isnan(reg21).sum())
        o['at_mode_2021_genuine'] = at_mode(reg21, ap, b_lu, b_ls, uv21, g21)
        o['nonint_q8_2021_share'] = round(float((g21 & (np.abs(a8 - np.round(a8)) > 1e-9)).sum() / max(g21.sum(), 1)), 5)
        o['nonint_q8_2018_genuine_share'] = round(float(nonint.sum() / max(genuine.sum(), 1)), 5)
    except Exception as e:
        o['placebo_2021'] = f'ERR {type(e).__name__}: {e}'[:500]
    return o


run('senegal_salt', senegal_salt)
run('niger_raw', niger_raw)
run('niger_revaluation', niger_revaluation)
run('niger_served', niger_served)
run('niger_leakage', niger_leakage)
print('done checks', flush=True)
