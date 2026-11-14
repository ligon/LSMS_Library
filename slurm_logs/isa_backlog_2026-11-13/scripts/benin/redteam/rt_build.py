"""Red-team build driver for Benin (independent of the worker's build_side.py).

usage: rt_build.py <mode> <label> <outdir>

modes
  cold    every call in CALLS (all tables, both per-wave calls, units/market
          variants), pickled; then in the SAME process: a fresh instance warm
          pass over every call, an assume_cache_fresh pass over every call,
          Feature() for every table, derivation_inputs join rates, panel props,
          check_panel_consistency.  Writes summary-cold.json.
  warm    a FRESH process over the same root: every call, compared with the
          cold pickles.  Writes summary-warm.json.
  subset  a FRESH EMPTY root: per-wave calls FIRST (2021-22, then 2018-19), then
          the all-wave call, for SUBSET_TABLES.  Pickles under subset__*.
  script7 cold build of the seven script tables plus sample/household_roster
          (used under LSMS_BUILD_WORKERS=2 and LSMS_BUILD_BACKEND=make).
"""
import json
import os
import pickle
import sys
import time
import traceback
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd

mode, label, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
import lsms_library as ll  # noqa: E402

SKIP = {'panel_ids', 'updated_ids'}
WAVES = ['2018-19', '2021-22']
SCRIPT7 = ['community_prices', 'crop_production', 'livestock', 'people_last7days',
           'plot_features', 'plot_inputs', 'plot_labor']
SUBSET_TABLES = ['household_roster', 'sample', 'plot_inputs', 'plot_features', 'food_acquired',
                 'food_quantities', 'household_characteristics', 'crop_production', 'shocks']
EXTRA = [('food_quantities', {'units': 'units'}), ('food_prices', {'units': 'unitvalue'}),
         ('food_prices', {'units': 'kgprice'}), ('food_prices', {'units': 'unitprice'}),
         ('household_characteristics', {'market': 'Region'}), ('food_expenditures', {'market': 'Region'}),
         ('food_quantities', {'units': 'kgs', 'waves': ['2021-22']})]


def key(t, kw):
    return t + ''.join(f'__{k}={"+".join(v) if isinstance(v, list) else v}' for k, v in sorted(kw.items()))


def call(c, t, **kw):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        t0 = time.time()
        try:
            df = getattr(c, t)(**kw)
            err = None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {e}'[:3000]
            traceback.print_exc()
        dt = time.time() - t0
    cats = Counter(x.category.__name__ for x in w)
    mk = sum('Makefile not found' in str(x.message) for x in w)
    msgs = sorted({f'{x.category.__name__}: {str(x.message)[:240]}' for x in w
                   if x.category.__name__ not in ('FutureWarning', 'DeprecationWarning', 'PerformanceWarning')})
    return df, err, dt, {'cats': dict(cats), 'makefile_not_found': mk, 'msgs': msgs[:40]}


def same(a, b):
    try:
        pd.testing.assert_frame_equal(a, b)
        return True
    except AssertionError as e:
        return str(e)[:600]


def calls_for(c):
    tables = [t for t in c.data_scheme if t not in SKIP]
    out = [(t, {}) for t in tables]
    for t in tables:
        for w in WAVES:
            out.append((t, {'waves': [w]}))
    out += EXTRA
    return tables, out


def dump(df, k):
    with open(outdir / f'{k}.pkl', 'wb') as fh:
        pickle.dump(df, fh)


def load(k):
    p = outdir / f'{k}.pkl'
    if not p.exists():
        return None
    with open(p, 'rb') as fh:
        return pickle.load(fh)


def rec_of(df, err, dt, info):
    r = {'err': err, 'secs': round(dt, 1), **info}
    if df is not None:
        r.update(rows=len(df), cols=list(map(str, df.columns)), index=list(map(str, df.index.names)),
                 attrs_keys=sorted(map(str, df.attrs)))
    return r


summary = {'label': label, 'mode': mode, 'env': {k: os.environ.get(k) for k in (
    'LSMS_BUILD_WORKERS', 'LSMS_BUILD_BACKEND', 'LSMS_DATA_DIR')}, 'calls': {}}
c = ll.Country('Benin')
tables, CALLS = calls_for(c)
summary['data_scheme'] = list(c.data_scheme)

if mode == 'cold':
    for t, kw in CALLS:
        df, err, dt, info = call(c, t, **kw)
        k = key(t, kw)
        summary['calls'][k] = rec_of(df, err, dt, info)
        if df is not None:
            dump(df, k)
        print(k, summary['calls'][k].get('rows'), err and err[:200], round(dt, 1), flush=True)
    for name, kwc in (('warm_same_process', {}), ('assume_cache_fresh', {'assume_cache_fresh': True})):
        c2 = ll.Country('Benin', **kwc)
        res = {}
        for t, kw in CALLS:
            k = key(t, kw)
            df, err, dt, info = call(c2, t, **kw)
            cold = load(k)
            if df is None or cold is None:
                res[k] = {'err': err, 'cold_missing': cold is None}
                continue
            res[k] = {'equal': same(df, cold), 'secs': round(dt, 1),
                      'idwalk': bool(err and 'IdWalk' in err) or any('IdWalk' in m for m in info['msgs'])}
        summary[name] = res
        print(name, sum(r.get('equal') is True for r in res.values()), 'of', len(res), flush=True)
    # Feature() for every table
    fp = {}
    for t in tables:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                f = ll.Feature(t)(['Benin'])
                cdf = load(t)
                fdf = f.xs('Benin', level='country') if 'country' in f.index.names else f
                r = {'feature_rows': len(f), 'country_rows': None if cdf is None else len(cdf),
                     'index_unique': bool(f.index.is_unique)}
                if cdf is not None and {'i', 't'} <= set(fdf.index.names) and {'i', 't'} <= set(cdf.index.names):
                    fi = set(zip(fdf.index.get_level_values('t').astype(str), fdf.index.get_level_values('i').astype(str)))
                    ci = set(zip(cdf.index.get_level_values('t').astype(str), cdf.index.get_level_values('i').astype(str)))
                    r['same_t_i'] = fi == ci
                    r['feature_minus_country'] = len(fi - ci)
                    r['country_minus_feature'] = len(ci - fi)
                with open(outdir / f'feature__{t}.pkl', 'wb') as fh:
                    pickle.dump(f, fh)
                fp[t] = r
            except Exception as e:  # noqa: BLE001
                fp[t] = {'err': f'{type(e).__name__}: {e}'[:800]}
            fp[t]['dropped_warnings'] = [str(x.message)[:300] for x in w if 'dropp' in str(x.message).lower()][:5]
    summary['feature'] = fp
    print('feature', {t: r.get('same_t_i', r.get('err')) for t, r in fp.items()}, flush=True)
    # derivation inputs join to served rows
    dj = {}
    try:
        recs = c.derivations()
    except Exception as e:  # noqa: BLE001
        recs = {}
        dj['_err'] = f'{type(e).__name__}: {e}'
    for dk in recs:
        table = dk.split('::')[1]
        served = load(table)
        for w in WAVES:
            kk = f'{dk}@{w}'
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    raw = c.derivation_inputs(dk, wave=w)
                r = {'raw_rows': len(raw), 'raw_index': list(map(str, raw.index.names))}
                if served is not None and 'Derivation' in served.columns:
                    s = served[served.index.get_level_values('t') == w]
                    s = s[s['Derivation'].fillna('').astype(str).str.contains(dk, regex=False)]
                    common = [n for n in raw.index.names if n in s.index.names]
                    r['common'] = common
                    sk = set(s.reset_index().set_index(common).index) if common else set()
                    rk = set(raw.reset_index().set_index(common).index) if common else set()
                    r['served_rows'] = len(s)
                    r['served_keys'] = len(sk)
                    r['served_keys_missing_from_raw'] = len(sk - rk)
                    r['raw_i_E2_prefixed'] = int(pd.Series(raw.index.get_level_values('i')).astype(str).str.startswith('E2_').sum()) \
                        if 'i' in raw.index.names else None
                dj[kk] = r
            except Exception as e:  # noqa: BLE001
                dj[kk] = {'err': f'{type(e).__name__}: {e}'[:600]}
    summary['derivation_inputs'] = dj
    print('derivation_inputs', dj, flush=True)
    # panel props
    try:
        c3 = ll.Country('Benin')
        ui, pi = c3.updated_ids, c3.panel_ids
        summary['panel_props'] = {'updated_ids_waves': None if ui is None else {k: len(v) for k, v in ui.items()},
                                  'panel_ids_len': None if pi is None else len(pi)}
        with open(outdir / 'updated_ids_prop.json', 'w') as fh:
            json.dump(ui, fh)
    except Exception as e:  # noqa: BLE001
        summary['panel_props'] = {'err': f'{type(e).__name__}: {e}'}
    if 'panel_ids' in c.data_scheme:
        from lsms_library.diagnostics import check_panel_consistency
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            try:
                rep = check_panel_consistency(ll.Country('Benin'))
                summary['panel_consistency'] = {'ok': bool(rep.ok), 'report': str(rep)[:6000]}
            except Exception as e:  # noqa: BLE001
                summary['panel_consistency'] = {'err': f'{type(e).__name__}: {e}'}
elif mode == 'warm':
    res = {}
    for t, kw in CALLS:
        k = key(t, kw)
        df, err, dt, info = call(c, t, **kw)
        cold = load(k)
        if df is None or cold is None:
            res[k] = {'err': err, 'cold_missing': cold is None}
            continue
        res[k] = {'equal': same(df, cold), 'secs': round(dt, 1),
                  'idwalk': bool(err and 'IdWalk' in err) or any('IdWalk' in m for m in info['msgs'])}
    summary['warm_fresh_process'] = res
    print('warm_fresh_process', sum(r.get('equal') is True for r in res.values()), 'of', len(res), flush=True)
elif mode == 'subset':
    for t in SUBSET_TABLES:
        for kw in ({'waves': ['2021-22']}, {'waves': ['2018-19']}, {}):
            df, err, dt, info = call(c, t, **kw)
            k = 'subset__' + key(t, kw)
            summary['calls'][k] = rec_of(df, err, dt, info)
            if df is not None:
                dump(df, k)
            print(k, summary['calls'][k].get('rows'), err and err[:200], flush=True)
elif mode == 'script7':
    for t in SCRIPT7 + ['sample', 'household_roster', 'household_characteristics']:
        df, err, dt, info = call(c, t)
        k = 'script7__' + t
        summary['calls'][k] = rec_of(df, err, dt, info)
        if df is not None:
            dump(df, k)
        print(k, summary['calls'][k].get('rows'), err and err[:200], flush=True)

with open(outdir / f'summary-{mode}.json', 'w') as fh:
    json.dump(summary, fh, indent=1, default=str)
print('DONE', flush=True)
