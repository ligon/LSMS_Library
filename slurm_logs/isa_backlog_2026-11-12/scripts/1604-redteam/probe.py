"""Candidate-side probes of Malawi 2024-25 (run after build.py in the SAME private root).

usage: python probe.py <clone> <framesdir> <out.json>
"""
import importlib.util
import json
import re
import sys
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, fd, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
assert_env(clone)
warnings.simplefilter('ignore')

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
import lsms_library.paths as paths  # noqa: E402
from lsms_library.local_tools import get_dataframe, format_id, id_walk, _walk_partition  # noqa: E402

W = '2024-25'
KEY = 'Malawi::food_acquired::borrow-ihs5-factor'
R = {}


def _clean(o):
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    return o


def save():
    out.write_text(json.dumps(_clean(R), indent=1, default=str))


import os  # noqa: E402
ONLY = set(filter(None, os.environ.get('PROBE_SECTIONS', '').split(',')))
FA = os.environ.get('PROBE_FA')  # optional pickle of the served food_acquired


def section(name):
    def deco(fn):
        if ONLY and name not in ONLY:
            return
        try:
            R[name] = fn()
        except Exception as e:
            R[name] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
        print('SECTION', name, json.dumps(_clean(R[name]), default=str)[:6000], flush=True)
        save()
    return deco


c = ll.Country('Malawi')
root = Path(paths.countries_root()) / 'Malawi' / W / 'Data'
ups = c.updated_ids
m24 = ups[W]
inv24 = {v: k for k, v in m24.items()}
assert len(inv24) == len(m24)

xs = get_dataframe(str(root / 'Cross_Sectional/hh_mod_a_filt.dta'), convert_categoricals=False)
pn = get_dataframe(str(root / 'Panel/hh_mod_a_filt_24.dta'), convert_categoricals=False)
xs_id = 'cs-24-' + xs['case_id'].map(format_id)
pn_y5 = pn['y5_hhid'].map(format_id)
pn_y4 = pn['y4_hhid'].map(format_id)
retained = pn_y4.notna()
refreshed = ~retained


@section('covers')
def _():
    return {
        'xs_rows': len(xs), 'xs_case_unique': bool(xs['case_id'].is_unique),
        'xs_cols_present': {c_: c_ in xs.columns for c_ in ['wt_long', 'y5_hhid', 'wt_cross', 'hh_wgt', 'ea_id', 'reside', 'region', 'district']},
        'pn_rows': len(pn), 'pn_retained': int(retained.sum()), 'pn_refreshed': int(refreshed.sum()),
        'pn_cols_present': {c_: c_ in pn.columns for c_ in ['wt_long', 'y5_hhid', 'wt_cross', 'hh_wgt', 'ea_id', 'case_id', 'panelweight_2019']},
        'pn_y5_unique': bool(pn_y5.is_unique),
        'refreshed_case_unique': bool(pn.loc[refreshed, 'case_id'].map(format_id).is_unique),
        'refreshed_case_in_xs': int(pn.loc[refreshed, 'case_id'].map(format_id).isin(set(xs['case_id'].map(format_id))).sum()),
        'retained_ea_in_xs_ea': int(pn.loc[retained, 'ea_id'].map(format_id).isin(set(xs['ea_id'].map(format_id))).sum()),
        'refreshed_ea_in_xs_ea': int(pn.loc[refreshed, 'ea_id'].map(format_id).isin(set(xs['ea_id'].map(format_id))).sum()),
        'wt_long_nonnull_retained': int(pn.loc[retained, 'wt_long'].notna().sum()),
        'wt_long_nonnull_refreshed': int(pn.loc[refreshed, 'wt_long'].notna().sum()),
        'wt_cross_nonnull': int(pn['wt_cross'].notna().sum()),
        'xs_hh_wgt_nonnull': int(xs['hh_wgt'].notna().sum()),
        'y5_matches_id_pattern': int(pn_y5.str.fullmatch(r'\d{4}-\d{3}').sum()),
        'updated_ids_2024_keys_eq_retained_y5': set(m24) == set(pn_y5[retained]),
    }


@section('sample_raw_parquet')
def _():
    p = Path(paths.data_root()) / 'Malawi' / 'var' / 'sample.parquet'
    raw = pd.read_parquet(p)
    t = raw.index.get_level_values('t').astype(str)
    s = raw[t == W].reset_index()
    s['i'] = s['i'].astype(str)
    r = {'rows': len(s), 'expected_rows': len(xs) + int(retained.sum()), 'i_unique': bool(s['i'].is_unique),
         'columns': list(raw.columns), 'dtypes': {k: str(v) for k, v in raw.dtypes.items()}}
    cs = s['i'].str.startswith('cs-24-')
    r['n_cs24'] = int(cs.sum()); r['n_other'] = int((~cs).sum())
    r['other_all_in_V'] = bool(s.loc[~cs, 'i'].isin(set(m24.values())).all())
    r['raw_y5_pattern_left'] = int(s['i'].str.fullmatch(r'\d{4}-\d{3}').sum())
    r['refreshed_y5_in_index'] = int(s['i'].isin(set(pn_y5[refreshed])).sum())
    # weights
    hh = pd.Series(xs['hh_wgt'].astype('float64').to_numpy(), index=xs_id.to_numpy())
    wc = pd.Series(pn['wt_cross'].astype('float64').to_numpy(), index=pn_y5.to_numpy())
    wl = pd.Series(pn['wt_long'].astype('float64').to_numpy(), index=pn_y5.to_numpy())
    y5_of = s['i'].map(lambda x: x if x.startswith('cs-24-') else inv24.get(x))
    exp_w = np.where(cs, s['i'].map(hh), y5_of.map(wc))
    got_w = s['weight'].astype('float64').to_numpy()
    eqw = (exp_w == got_w) | (np.isnan(exp_w) & np.isnan(got_w))
    r['weight_equal_ruled'] = int(eqw.sum()); r['weight_mismatch'] = int((~eqw).sum())
    exp_pw = np.where(cs, np.nan, y5_of.map(wl))
    if 'wt_long' in xs.columns:
        xwl = pd.Series(xs['wt_long'].astype('float64').to_numpy(), index=xs_id.to_numpy())
        exp_pw = np.where(cs, s['i'].map(xwl), exp_pw)
    got_pw = s['panel_weight'].astype('float64').to_numpy()
    eqp = (exp_pw == got_pw) | (np.isnan(exp_pw) & np.isnan(got_pw))
    r['panel_weight_equal_ruled'] = int(eqp.sum()); r['panel_weight_mismatch'] = int((~eqp).sum())
    r['panel_weight_nonnull'] = int(s['panel_weight'].notna().sum())
    r['panel_weight_nonnull_cs24'] = int(s.loc[cs, 'panel_weight'].notna().sum())
    # ihps cols
    case_to_y5 = dict(zip('cs-24-' + pn.loc[refreshed, 'case_id'].map(format_id), pn_y5[refreshed]))
    exp_h = np.where(cs, s['i'].map(case_to_y5), y5_of)
    got_h = s['ihps_hhid'].astype('string').to_numpy(dtype=object)
    eqh = [(a == b) if isinstance(a, str) else pd.isna(b) for a, b in zip(exp_h, got_h)]
    r['ihps_hhid_equal'] = int(sum(eqh)); r['ihps_hhid_nonnull'] = int(s['ihps_hhid'].notna().sum())
    exp_iw = pd.Series(exp_h).map(wc).to_numpy(dtype='float64')
    got_iw = s['ihps_weight'].astype('float64').to_numpy()
    eqi = (exp_iw == got_iw) | (np.isnan(exp_iw) & np.isnan(got_iw))
    r['ihps_weight_equal'] = int(eqi.sum()); r['ihps_weight_nonnull'] = int(s['ihps_weight'].notna().sum())
    r['refreshed_rows_weight_is_hh_wgt'] = int((cs & s['i'].isin(set(case_to_y5)) & eqw).sum())
    r['refreshed_rows_with_ihs6'] = int(s['i'].isin(set(case_to_y5)).sum())
    r['v_nonnull'] = int(s['v'].notna().sum())
    # other waves: new columns
    o = raw[t != W]
    r['older_waves_ihps_nonnull'] = {col: int(o[col].notna().sum()) for col in ['ihps_hhid', 'ihps_weight'] if col in o}
    return r


@section('sample_api')
def _():
    s = c.sample()
    t = s.index.get_level_values('t').astype(str)
    r = {'means': {}}
    for w in sorted(set(t)):
        sl = s[t == w]
        r['means'][w] = {col: (float(sl[col].mean()) if col in sl and sl[col].notna().any() else None)
                         for col in ['weight', 'panel_weight', 'ihps_weight']}
    ids = {w: set(s[t == w].index.get_level_values('i').astype(str)) for w in sorted(set(t))}
    earlier = set().union(*[v for w, v in ids.items() if w != W])
    i24 = ids[W]
    inter = i24 & earlier
    r['ids_2024_in_earlier'] = len(inter)
    r['ids_2024_in_earlier_all_continuations'] = bool(inter <= set(m24.values()))
    minted = {v for k, v in m24.items() if v not in earlier}
    r['V_not_in_any_earlier_served_wave'] = len(minted)
    r['cs24_in_earlier'] = sum(1 for x in earlier if x.startswith('cs-24-'))
    bare = set(xs['case_id'].map(format_id))
    r['bare_ihs6_case_in_earlier_by_wave'] = {w: len(bare & v) for w, v in ids.items() if w != W}
    r['raw_y5_pattern_in_2024'] = sum(1 for x in i24 if re.fullmatch(r'\d{4}-\d{3}', x))
    r['n_2024'] = len(i24)
    r['dup_index'] = int(s.index.duplicated().sum())
    return r


def classify(df):
    raw_only, walked_only, chained = _walk_partition(m24)
    ids = pd.Index(df.index.get_level_values('i').astype(str))
    return {'raw_only': int(ids.isin(raw_only).sum()), 'walked_only': int(ids.isin(walked_only).sum()),
            'chained': int(ids.isin(chained).sum()), 'inert': int((~ids.isin(raw_only | walked_only | chained)).sum())}


@section('id_walk')
def _():
    r = {}
    raw_only, walked_only, chained = _walk_partition(m24)
    r['partition_sizes'] = [len(raw_only), len(walked_only), len(chained)]
    for name in ['sample', 'food_acquired']:
        df = pd.read_parquet(fd / f'cand.{name}.parquet')
        sl = df[df.index.get_level_values('t').astype(str) == W]
        r[name] = classify(sl)
        again = id_walk(sl.copy(), ups)
        r[name]['second_walk_identity'] = bool(again.index.equals(sl.index))
        againall = id_walk(df.copy(), ups)
        r[name]['second_walk_identity_all_waves'] = bool(againall.index.equals(df.index))
    return r


@section('derivation_join')
def _():
    fa = pd.read_pickle(FA) if FA else pd.read_parquet(fd / 'cand.food_acquired.parquet')
    r = {'index_names': list(fa.index.names), 'columns': list(fa.columns)}
    t = fa.index.get_level_values('t').astype(str)
    r['derivation_counts_by_t'] = {w: fa.loc[t == w, 'Derivation'].value_counts(dropna=False).to_dict() if 'Derivation' in fa else None
                                   for w in sorted(set(t))}
    fa24 = fa[t == W].reset_index()
    stamped = fa24[fa24['Derivation'].astype('string').eq(KEY).fillna(False)]
    r['stamped_rows'] = len(stamped)
    r['qkg_nonnull_rows_2024'] = int(fa24['Quantity_kg'].notna().sum()) if 'Quantity_kg' in fa24 else None
    r['stamped_eq_qkg_nonnull'] = bool((fa24['Derivation'].astype('string').eq(KEY).fillna(False) == fa24['Quantity_kg'].notna()).all())
    keys = ['t', 'i', 'j', 'u', 's']
    inp = c.derivation_inputs(KEY, wave=W)
    r['inputs_index'] = list(inp.index.names); r['inputs_rows'] = len(inp)
    r['inputs_columns'] = list(inp.columns)[:80]
    ii = inp.reset_index()
    for k in keys:
        ii[k] = ii[k].astype(str)
        stamped[k] = stamped[k].astype(str)
    sk = set(map(tuple, stamped[keys].to_numpy()))
    ik = set(map(tuple, ii[keys].to_numpy()))
    r['served_keys'] = len(sk); r['input_keys'] = len(ik)
    r['served_not_in_inputs'] = len(sk - ik); r['inputs_not_in_served'] = len(ik - sk)
    r['ex_served_not_in_inputs'] = [list(x) for x in list(sk - ik)[:8]]
    r['ex_inputs_not_in_served'] = [list(x) for x in list(ik - sk)[:8]]
    # u comparison by s
    r['served_u_top'] = stamped['u'].value_counts().head(15).to_dict()
    r['inputs_u_top'] = ii['u'].value_counts().head(15).to_dict()
    qcol = {'purchased': 'hh_g04a', 'produced': 'hh_g06a', 'inkind': 'hh_g07a'}
    q = np.full(len(ii), np.nan)
    for s_, col in qcol.items():
        msk = (ii['s'] == s_).to_numpy()
        if col in ii:
            q[msk] = pd.to_numeric(ii.loc[msk, col], errors='coerce').to_numpy()
    ii['_kg'] = q * pd.to_numeric(ii['ihs5.factor'], errors='coerce').to_numpy()
    rd = ii.groupby(keys)['_kg'].sum(min_count=1)
    sv = stamped.set_index(keys)['Quantity_kg']
    common = sv.index.intersection(rd.index)
    a, b = sv.loc[common].astype(float).to_numpy(), rd.loc[common].astype(float).to_numpy()
    ok = np.isclose(a, b, rtol=1e-9, atol=0)
    r['rederived_rows'] = int(len(common)); r['rederived_match'] = int(ok.sum())
    r['rederived_examples_bad'] = [[str(i), float(x), float(y)] for i, x, y in zip(common[~ok][:8], a[~ok][:8], b[~ok][:8])]
    r['input_factor_origin_counts'] = ii['ihs5.source'].value_counts(dropna=False).to_dict() if 'ihs5.source' in ii else None
    r['input_md5'] = ii['ihs5_source_md5'].unique().tolist() if 'ihs5_source_md5' in ii else None
    return r


@section('metric_and_borrow_scope')
def _():
    p = Path(paths.countries_root()) / 'Malawi' / W / '_' / 'food_borrowing.py'
    spec = importlib.util.spec_from_file_location('fb_probe', p)
    fb = importlib.util.module_from_spec(spec); spec.loader.exec_module(fb)
    wide = fb.read_food_wide(); factors = fb.load_ihs5_factors()
    prep = fb.prepare_ihs5_factors(factors)
    r = {'factor_rows': len(prep), 'admitted': int(prep.borrow_compatible.sum()), 'refused': int((~prep.borrow_compatible).sum())}
    adm = prep[prep.borrow_compatible].set_index(fb.KEYS)
    matched = fb.match_ihs5_factors(wide, factors)
    per = {}
    for s_ in ['bought', 'produced', 'gifted']:
        q = pd.to_numeric(matched[f'quantity_{s_}'], errors='coerce')
        act = q.gt(0)
        u = matched[f'u_{s_}']
        met = u.map(fb._metric)
        unit = matched[f'unitcode_{s_}'].map(fb._code)
        raw_other = f'{fb.SOURCES[s_]}b_oth'
        detail = matched[raw_other] if raw_other in matched else matched[f'unitsdetail_{s_}']
        other = detail.map(fb._other).where(unit.eq('23'), '')
        idx = pd.MultiIndex.from_arrays([matched['_borrow_region'].map(fb._code), matched['hh_g02'].map(fb._code), unit, other], names=fb.KEYS)
        has_adm = pd.Series(adm.reindex(idx).factor.notna().to_numpy(), index=matched.index)
        d = {'active': int(act.sum()), 'active_metric': int((act & met).sum()),
             'active_metric_with_admitted_factor': int((act & met & has_adm).sum()),
             'active_with_factor': int((act & matched[f'cfactor_{s_}'].notna()).sum()),
             'active_nonmetric_no_factor': int((act & ~met & matched[f'cfactor_{s_}'].isna()).sum())}
        mk = act & met & has_adm
        d['metric_with_factor_by_u'] = u[mk].value_counts().head(12).to_dict()
        d['metric_with_factor_by_code'] = unit[mk].value_counts().head(12).to_dict()
        c26 = act & unit.eq('26')
        d['code26_active'] = int(c26.sum()); d['code26_metric'] = int((c26 & met).sum())
        d['code26_u'] = u[c26].value_counts().head(5).to_dict()
        if c26.any():
            fac = adm.reindex(idx).factor.to_numpy()[c26.to_numpy()]
            d['code26_admitted_factor_quantiles'] = pd.Series(fac).describe().to_dict()
            d['code26_qty_sum'] = float(q[c26].sum())
        per[s_] = d
    r['per_source'] = per
    return r


@section('food_acquired_2024_units')
def _():
    fa = pd.read_parquet(fd / 'cand.food_acquired.parquet')
    t = fa.index.get_level_values('t').astype(str)
    out = {}
    for w in ['2019-20', W]:
        sl = fa[t == w]
        u = sl.index.get_level_values('u').astype(str)
        out[w] = {'rows': len(sl), 'u_top': pd.Series(u).value_counts().head(25).to_dict(),
                  'qkg_nonnull': int(sl['Quantity_kg'].notna().sum()) if 'Quantity_kg' in sl else None,
                  'j_n': int(sl.index.get_level_values('j').nunique())}
    return out

print('PROBE DONE', flush=True)
