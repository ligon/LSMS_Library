"""Isolate the cause of the older waves' pooled-kg movement by splicing frames.

usage: kgiso.py CLONE FRAMES OUT.json COUNTRY[,COUNTRY...]
Runs in a head clone (transformations.py is identical at prev and head).
For each country: old waves (t != 2021-22) and the 2021-22 slice of the
served all-waves food_acquired frame (fa_agg) from prev and head are
spliced, and food_quantities(units='kgs') / food_prices(kgvalue) are
computed on each splice with the country's own unit_kg / no_kg.
Aggregates only.
"""
import json, os, re, sys, traceback, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env

clone, frames, out, countries = sys.argv[1:5]
assert_env(clone)
import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library.transformations import (food_quantities_from_acquired,
                                          food_prices_from_acquired)

NEW = '2021-22'
DECODE = {'659': 'Main', '660': 'Quartier', '662': 'Tête'}
PAT = re.compile(r'^(659|660|662)(?:\.0)?(?=$|\s|\()')
res = {}


def dump():
    json.dump(res, open(out, 'w'), indent=1, default=str)


def tmask(df, old):
    t = df.index.get_level_values('t').astype(str)
    return (t != NEW) if old else (t == NEW)


def decode_u(df):
    u = df.index.get_level_values('u')
    newu = pd.Index([PAT.sub(lambda m: DECODE[m.group(1)], x) if isinstance(x, str) else x for x in u])
    arrays = [df.index.get_level_values(n) if n != 'u' else newu for n in df.index.names]
    out_ = df.copy()
    out_.index = pd.MultiIndex.from_arrays(arrays, names=df.index.names)
    return out_


def kg_summary(q, old_waves):
    t = q.index.get_level_values('t').astype(str)
    u = q.index.get_level_values('u').astype(str)
    s = {}
    for w in old_waves:
        m = (t == w)
        s[w] = {'rows': int(m.sum()), 'kg_rows': int((m & (u == 'kg')).sum()),
                'kg_sum': float(q.loc[m & (u == 'kg'), 'Quantity'].sum())}
    return s


def row_moves(a, b, old_waves, col):
    """Rows of the old waves whose `col` differs between two derived frames."""
    a = a[tmask(a, True)].sort_index()
    b = b[tmask(b, True)].sort_index()
    out_ = {'rows': [len(a), len(b)]}
    if not (a.index.is_unique and b.index.is_unique):
        out_['dup'] = [int(a.index.duplicated().sum()), int(b.index.duplicated().sum())]
    both = a.index.intersection(b.index)
    out_['only_a'] = int((~a.index.isin(b.index)).sum())
    out_['only_b'] = int((~b.index.isin(a.index)).sum())
    x = a.loc[both, col].astype(float).to_numpy()
    y = b.loc[both, col].astype(float).to_numpy()
    neq = ~np.isclose(x, y, rtol=1e-9, atol=0, equal_nan=True)
    out_['moved'] = int(neq.sum())
    if neq.any():
        with np.errstate(divide='ignore', invalid='ignore'):
            r = y[neq] / x[neq]
        r = r[np.isfinite(r)]
        if len(r):
            out_['ratio_q10_50_90'] = [float(v) for v in np.quantile(r, [.1, .5, .9])]
        uu = pd.Index(both).get_level_values('u').astype(str)[neq]
        out_['moved_by_u_top'] = pd.Series(uu).value_counts().head(8).to_dict()
    return out_


for country in countries.split(','):
    R = res.setdefault(country, {})
    try:
        c = ll.Country(country)
        kw = dict(unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
        P = pd.read_parquet(os.path.join(frames, 'prev', country, 'fa_agg.parquet'))
        H = pd.read_parquet(os.path.join(frames, 'head', country, 'fa_agg.parquet'))
        old_waves = sorted(set(P.index.get_level_values('t').astype(str)) - {NEW})
        R['old_waves'] = old_waves
        Pold, Pnew = P[tmask(P, True)], P[tmask(P, False)]
        Hold, Hnew = H[tmask(H, True)], H[tmask(H, False)]
        # Are the old slices identical (after the Togo decode)?
        try:
            pd.testing.assert_frame_equal(Pold.sort_index(), Hold.sort_index(), check_dtype=False,
                                          check_categorical=False, check_index_type=False)
            R['old_fa_equal_raw'] = True
        except AssertionError as e:
            R['old_fa_equal_raw'] = str(e).split('\n')[0][:200]
        if country == 'Togo':
            try:
                pd.testing.assert_frame_equal(decode_u(Pold).sort_index(), Hold.sort_index(), check_dtype=False,
                                              check_categorical=False, check_index_type=False)
                R['old_fa_equal_after_decode'] = True
            except AssertionError as e:
                R['old_fa_equal_after_decode'] = str(e).split('\n')[0][:200]
        mixes = {'A_prev': pd.concat([Pold, Pnew]),
                 'B_head': pd.concat([Hold, Hnew]),
                 'C_oldprev_newhead': pd.concat([Pold, Hnew])}
        if country == 'Togo':
            mixes['D_oldhead_newprev'] = pd.concat([Hold, Pnew])
            # 2021-22 head slice with the withheld-count rows dropped: does the NA row matter?
        q, p = {}, {}
        for k, df in mixes.items():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                q[k] = food_quantities_from_acquired(df, units='kgs', **kw)
                p[k] = food_prices_from_acquired(df, units='kgvalue', **kw)
            R.setdefault('kg', {})[k] = kg_summary(q[k], old_waves)
            R.setdefault('kg_sources', {})[k] = q[k].attrs.get('kg_factor_sources')
            print(country, k, R['kg'][k], flush=True)
            dump()
        pairs = [('A_prev', 'B_head'), ('A_prev', 'C_oldprev_newhead')]
        if country == 'Togo':
            pairs += [('A_prev', 'D_oldhead_newprev'), ('C_oldprev_newhead', 'B_head'),
                      ('D_oldhead_newprev', 'B_head')]
        for a, b in pairs:
            R.setdefault('quantity_moves', {})[f'{a}->{b}'] = row_moves(q[a], q[b], old_waves, 'Quantity')
            pcol = 'Price' if 'Price' in p[a].columns else p[a].columns[0]
            R.setdefault('price_moves', {})[f'{a}->{b}'] = row_moves(p[a], p[b], old_waves, pcol)
            dump()
        # Validation: does the unspliced head/prev frame reproduce the SERVED kg totals?
        for side, k in (('prev', 'A_prev'), ('head', 'B_head')):
            fp = os.path.join(frames, side, country, 'food_quantities.parquet')
            if os.path.exists(fp):
                served = pd.read_parquet(fp)
                R.setdefault('served_kg', {})[side] = kg_summary(served, old_waves)
        # 2021-22 evidence: rows with a purchased Expenditure and Quantity in each slice
        for side, F in (('prev', Pnew), ('head', Hnew)):
            s_ = F.index.get_level_values('s').astype(str)
            valued = (s_ == 'purchased') & F['Quantity'].notna() & F['Expenditure'].notna() & (F['Quantity'] > 0)
            R.setdefault('new_evidence', {})[side] = {'purchased_rows': int((s_ == 'purchased').sum()),
                                                     'valued_rows': int(valued.sum())}
        dump()
    except Exception as exc:
        R['error'] = f'{type(exc).__name__}: {exc}'[:800]
        R['tb'] = traceback.format_exc()[-2000:]
        print(country, 'ERROR', exc, flush=True)
        dump()
dump()
print('KGISO DONE')
