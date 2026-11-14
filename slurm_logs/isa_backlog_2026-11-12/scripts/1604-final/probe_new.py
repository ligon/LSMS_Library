"""New-arm probes of Malawi 2024-25 (same private root, after build.py).

usage: python probe_new.py <clone> <framesdir> <out.json>
"""
import importlib.util
import json
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

W = '2024-25'
KEY = 'Malawi::food_acquired::borrow-ihs5-factor'
R = {}


def _clean(o):
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    return o


def section(name):
    def deco(fn):
        try:
            R[name] = fn()
        except Exception as e:
            R[name] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
        print('SECTION', name, json.dumps(_clean(R[name]), default=str)[:5000], flush=True)
        out.write_text(json.dumps(_clean(R), indent=1, default=str))
    return deco


c = ll.Country('Malawi')
fa = pd.read_parquet(fd / 'new.food_acquired.parquet')
sa = pd.read_parquet(fd / 'new.sample.parquet')


def is_bucket(u):
    return pd.Series(u).astype(str).str.contains('chigoba', case=False).to_numpy()


@section('derivation_join')
def _():
    t = fa.index.get_level_values('t').astype(str)
    fa24 = fa[t == W].reset_index()
    stamped = fa24[fa24['Derivation'].astype('string').eq(KEY).fillna(False)].copy()
    r = {'stamped_rows': len(stamped), 'qkg_nonnull': int(fa24['Quantity_kg'].notna().sum()),
         'stamped_eq_qkg_nonnull': bool((fa24['Derivation'].astype('string').eq(KEY).fillna(False)
                                         == fa24['Quantity_kg'].notna()).all()),
         'derivation_by_t': {w: fa.loc[t == w, 'Derivation'].astype('string').value_counts(dropna=False).to_dict()
                             for w in sorted(set(t))}}
    keys = ['t', 'i', 'j', 'u', 's']
    inp = c.derivation_inputs(KEY, wave=W)
    r['inputs_rows'] = len(inp)
    ii = inp.reset_index()
    for k in keys:
        ii[k] = ii[k].astype(str)
        stamped[k] = stamped[k].astype(str)
    sk = set(map(tuple, stamped[keys].to_numpy()))
    ik = set(map(tuple, ii[keys].to_numpy()))
    r['served_keys'] = len(sk); r['input_keys'] = len(ik)
    r['served_not_in_inputs'] = len(sk - ik); r['inputs_not_in_served'] = len(ik - sk)
    r['ex_served_not_in_inputs'] = [list(x) for x in list(sk - ik)[:6]]
    r['ex_inputs_not_in_served'] = [list(x) for x in list(ik - sk)[:6]]
    qcol = {'purchased': 'hh_g04a', 'produced': 'hh_g06a', 'inkind': 'hh_g07a'}
    q = np.full(len(ii), np.nan)
    for s_, col in qcol.items():
        m = (ii['s'] == s_).to_numpy()
        q[m] = pd.to_numeric(ii.loc[m, col], errors='coerce').to_numpy()
    ii['_kg'] = q * pd.to_numeric(ii['ihs5.factor'], errors='coerce').to_numpy()
    rd = ii.groupby(keys)['_kg'].sum(min_count=1)
    sv = stamped.set_index(keys)['Quantity_kg']
    common = sv.index.intersection(rd.index)
    a, b = sv.loc[common].astype(float).to_numpy(), rd.loc[common].astype(float).to_numpy()
    ok = np.isclose(a, b, rtol=1e-9, atol=0)
    r['rederived_rows'] = int(len(common)); r['rederived_match'] = int(ok.sum())
    # bucket rows specifically
    sb = stamped[is_bucket(stamped['u'])]
    ib = ii[is_bucket(ii['u'])]
    skb = set(map(tuple, sb[keys].to_numpy())); ikb = set(map(tuple, ib[keys].to_numpy()))
    r['bucket'] = {'served_stamped_rows': len(sb), 'input_rows': len(ib), 'input_keys': len(ikb),
                   'served_not_in_inputs': len(skb - ikb), 'inputs_not_in_served': len(ikb - skb),
                   'served_qkg_sum': float(sb['Quantity_kg'].sum()),
                   'served_native_qty_sum': float(sb['Quantity'].sum()),
                   'input_unit_codes': {k: int(v) for k, v in pd.concat(
                       [ib[col] for col in ('hh_g04b', 'hh_g06b', 'hh_g07b') if col in ib]).astype(str)
                       .value_counts().head(8).items()},
                   'factor_quantiles': pd.to_numeric(ib['ihs5.factor'], errors='coerce').describe().to_dict()}
    bm = is_bucket(pd.Series([k[3] for k in common]))
    r['bucket']['rederived_rows'] = int(bm.sum()); r['bucket']['rederived_match'] = int(ok[bm].sum())
    r['input_factor_origin_counts'] = ii['ihs5.source'].astype(str).value_counts(dropna=False).to_dict()
    return r


@section('f1_scope')
def _():
    p = Path(paths.countries_root()) / 'Malawi' / W / '_' / 'food_borrowing.py'
    spec = importlib.util.spec_from_file_location('fb_probe', p)
    fb = importlib.util.module_from_spec(spec); spec.loader.exec_module(fb)
    wide = fb.read_food_wide(); factors = fb.load_ihs5_factors()
    matched = fb.match_ihs5_factors(wide, factors)
    r = {}
    for s_ in ['bought', 'produced', 'gifted']:
        q = pd.to_numeric(matched[f'quantity_{s_}'], errors='coerce')
        act = q.gt(0)
        code = matched[f'unitcode_{s_}'].map(fb._code)
        c26 = act & code.eq('26')
        cf = matched[f'cfactor_{s_}']
        r[s_] = {'code26_active': int(c26.sum()),
                 'code26_with_factor': int((c26 & cf.notna()).sum()),
                 'code26_without_factor_to_kg': int((c26 & cf.isna()).sum()),
                 'code26_qty_with_factor': float(q[c26 & cf.notna()].sum()),
                 'code26_kg_borrowed': float((q * cf)[c26 & cf.notna()].sum()),
                 'code26_kg_at_density1_if_not_borrowed': float(5 * q[c26 & cf.notna()].sum()),
                 'code26_qty_without_factor': float(q[c26 & cf.isna()].sum()),
                 'metric_refused_active': int((act & fb._metric_refused(matched, s_)).sum()),
                 'metric_refused_with_cfactor': int((fb._metric_refused(matched, s_) & cf.notna()).sum())}
    return r


@section('food_2024_components')
def _():
    t = fa.index.get_level_values('t').astype(str)
    f = fa[t == W]
    i = f.index.get_level_values('i').astype(str)
    u = f.index.get_level_values('u').astype(str)
    cs = i.str.startswith('cs-24-')
    r = {'rows': len(f), 'households': int(pd.Index(i).nunique())}
    for name, m in (('ihs6_cs24', cs), ('retained_ihps', ~cs)):
        sl = f[m]
        r[name] = {'rows': int(m.sum()), 'households': int(pd.Index(i[m]).nunique()),
                   'qkg_nonnull': int(sl['Quantity_kg'].notna().sum()),
                   'stamped': int(sl['Derivation'].astype('string').eq(KEY).fillna(False).sum()),
                   'by_s': sl.index.get_level_values('s').astype(str).value_counts().to_dict()}
    r['u_kg_rows'] = int((u == 'kg').sum())
    r['u_kg_rows_qkg_nonnull'] = int(((u == 'kg') & f['Quantity_kg'].notna().to_numpy()).sum())
    r['bucket_rows'] = int(is_bucket(u).sum())
    r['bucket_rows_qkg_nonnull'] = int((is_bucket(u) & f['Quantity_kg'].notna().to_numpy()).sum())
    return r


@section('sample_components')
def _():
    raw = pd.read_parquet(Path(paths.data_root()) / 'Malawi' / 'var' / 'sample.parquet')
    out = {}
    for label, df in (('raw', raw), ('api', sa)):
        t = df.index.get_level_values('t').astype(str)
        s = df[t == W]
        i = s.index.get_level_values('i').astype(str)
        cs = i.str.startswith('cs-24-')
        d = {'rows': len(s), 'ihs6_cs24': int(cs.sum()), 'retained': int((~cs).sum())}
        for col in ('weight', 'panel_weight', 'ihps_weight'):
            v = pd.to_numeric(s[col], errors='coerce')
            d[col] = {'nonnull': int(v.notna().sum()), 'mean': float(v.mean()),
                      'nonnull_ihs6': int(v[cs].notna().sum()), 'mean_ihs6': float(v[cs].mean()) if v[cs].notna().any() else None,
                      'nonnull_retained': int(v[~cs].notna().sum()), 'mean_retained': float(v[~cs].mean()) if v[~cs].notna().any() else None}
        d['ihps_hhid_nonnull'] = int(s['ihps_hhid'].notna().sum())
        out[label] = d
    # stacked shares on the API frame: panel (non-cs-) share of rows and weight
    t = sa.index.get_level_values('t').astype(str)
    shares = {}
    for w, pre in (('2016-17', 'cs-17-'), ('2019-20', 'cs-19-'), ('2024-25', 'cs-24-')):
        s = sa[t == w]
        i = s.index.get_level_values('i').astype(str)
        panel = ~i.str.startswith(pre)
        wt = pd.to_numeric(s['weight'], errors='coerce')
        shares[w] = {'rows': len(s), 'panel_rows': int(panel.sum()),
                     'panel_share_rows': float(panel.mean()),
                     'panel_share_weight': float(wt[panel].sum() / wt.sum())}
    out['stacked_shares'] = shares
    return out


print('PROBE DONE', flush=True)
