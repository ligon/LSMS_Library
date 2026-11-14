"""Cold build of every Benin table, then warm reads.  usage: build_side.py <label> <outdir> <mode>

mode 'cold': build every table (all waves, and a per-wave call for a few), pickle each frame,
             record rows / warnings / errors / time; then a same-process warm pass, an
             assume_cache_fresh pass, transforms (nitrogen_kg, seed_kg), Feature parity for
             plot_inputs, and (when declared) check_panel_consistency.
mode 'warm': a FRESH process over the same data root: every table again, compared with the
             pickled cold frames.
"""
import json
import pickle
import sys
import time
import traceback
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd

label, outdir, mode = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
outdir.mkdir(parents=True, exist_ok=True)
import lsms_library as ll  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

SKIP = {'panel_ids', 'updated_ids'}
PER_WAVE = ['sample', 'household_roster', 'plot_inputs', 'food_acquired', 'household_characteristics',
            'food_quantities', 'shocks']


def call(c, t, **kw):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        t0 = time.time()
        try:
            df = getattr(c, t)(**kw)
            err = None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {e}'[:2000]
            traceback.print_exc()
        dt = time.time() - t0
    cats = Counter(x.category.__name__ for x in w)
    msgs = sorted({f'{x.category.__name__}: {str(x.message)[:300]}' for x in w
                   if x.category.__name__ not in ('FutureWarning', 'DeprecationWarning',
                                                  'PerformanceWarning')})
    return df, err, dt, dict(cats), msgs


def key(t, kw):
    return t + ''.join(f'__{k}={v}' for k, v in sorted(kw.items()))


def same(a, b):
    try:
        pd.testing.assert_frame_equal(a, b)
        return True
    except AssertionError as e:
        return str(e)[:500]


summary = {'label': label, 'mode': mode, 'calls': {}}
c = ll.Country('Benin')
tables = [t for t in c.data_scheme if t not in SKIP]
calls = [(t, {}) for t in tables]
for t in PER_WAVE:
    for w in c.waves:
        calls.append((t, {'waves': [w]}))
calls += [('food_quantities', {'units': 'units'}), ('food_prices', {'units': 'unitvalue'})]

if mode == 'cold':
    print('panel_ids declared:', 'panel_ids' in c.data_scheme, flush=True)
    for t, kw in calls:
        df, err, dt, cats, msgs = call(c, t, **kw)
        k = key(t, kw)
        rec = {'err': err, 'secs': round(dt, 1), 'warnings': cats, 'messages': msgs}
        if df is not None:
            rec.update(rows=len(df), cols=list(map(str, df.columns)), index=list(map(str, df.index.names)))
            with open(outdir / f'{k}.pkl', 'wb') as fh:
                pickle.dump(df, fh)
        summary['calls'][k] = rec
        print(k, rec.get('rows'), err, round(dt, 1), cats, flush=True)
    # same-process warm pass on a fresh instance, and assume_cache_fresh
    for name, kwc in (('warm_same_process', {}), ('assume_cache_fresh', {'assume_cache_fresh': True})):
        c2 = ll.Country('Benin', **kwc)
        res = {}
        for t in tables:
            df, err, dt, cats, msgs = call(c2, t)
            p = outdir / f'{t}.pkl'
            if df is None or not p.exists():
                res[t] = {'err': err}
                continue
            with open(p, 'rb') as fh:
                cold = pickle.load(fh)
            res[t] = {'equal': same(df, cold), 'secs': round(dt, 1),
                      'IdWalkError': any('IdWalk' in m for m in msgs) or bool(err and 'IdWalk' in err)}
        summary[name] = res
        print(name, {t: r.get('equal', r.get('err')) for t, r in res.items()}, flush=True)
    # transforms on plot_inputs
    pi = pickle.load(open(outdir / 'plot_inputs.pkl', 'rb'))
    tr = {}
    for fn in ('nitrogen_kg', 'seed_kg'):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            try:
                out = getattr(T, fn)(pi)
                with open(outdir / f'T_{fn}.pkl', 'wb') as fh:
                    pickle.dump(out, fh)
                tr[fn] = {'rows': len(out), 'sum': {c_: float(pd.to_numeric(out[c_], errors='coerce').sum())
                                                    for c_ in out.columns
                                                    if pd.api.types.is_numeric_dtype(out[c_])}}
            except Exception as e:  # noqa: BLE001
                tr[fn] = {'err': f'{type(e).__name__}: {e}'}
    summary['transforms'] = tr
    # Feature parity for plot_inputs and household_roster
    fp = {}
    for t in ('plot_inputs', 'household_roster', 'sample'):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            try:
                f = ll.Feature(t)(['Benin'])
                cdf = pickle.load(open(outdir / f'{t}.pkl', 'rb'))
                fdf = f.xs('Benin', level='country')
                fp[t] = {'feature_rows': len(f), 'country_rows': len(cdf),
                         'index_unique': bool(f.index.is_unique),
                         'same_i_t': set(zip(fdf.index.get_level_values('i'), fdf.index.get_level_values('t')))
                         == set(zip(cdf.index.get_level_values('i'), cdf.index.get_level_values('t')))}
                if t == 'plot_inputs':
                    fp[t]['u_unknown'] = int((fdf.index.get_level_values('u') == 'Unknown').sum()) \
                        if 'u' in fdf.index.names else None
                    fp[t]['u_manquant'] = int((fdf.index.get_level_values('u') == 'Manquant').sum()) \
                        if 'u' in fdf.index.names else None
            except Exception as e:  # noqa: BLE001
                fp[t] = {'err': f'{type(e).__name__}: {e}'[:500]}
    summary['feature'] = fp
    if 'panel_ids' in c.data_scheme:
        from lsms_library.diagnostics import check_panel_consistency
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            try:
                rep = check_panel_consistency(ll.Country('Benin'))
                summary['panel_consistency'] = {'ok': bool(rep.ok), 'report': str(rep)[:6000]}
            except Exception as e:  # noqa: BLE001
                summary['panel_consistency'] = {'err': f'{type(e).__name__}: {e}'}
        print('panel_consistency', summary['panel_consistency'], flush=True)
else:
    res = {}
    for t in tables:
        df, err, dt, cats, msgs = call(c, t)
        p = outdir / f'{t}.pkl'
        if df is None or not p.exists():
            res[t] = {'err': err}
            continue
        with open(p, 'rb') as fh:
            cold = pickle.load(fh)
        res[t] = {'equal': same(df, cold), 'secs': round(dt, 1), 'warnings': cats}
    summary['warm_fresh_process'] = res
    print('warm_fresh_process', {t: r.get('equal', r.get('err')) for t, r in res.items()}, flush=True)

with open(outdir / f'summary-{mode}.json', 'w') as fh:
    json.dump(summary, fh, indent=1, default=str)
print('DONE', flush=True)
