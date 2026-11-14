"""Compare the cold arms (aggregates only).  argv: frames_dir results_dir"""
import json
import os
import sys
from pathlib import Path

import pandas as pd

frames, res = Path(sys.argv[1]), Path(sys.argv[2])
PP_T = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']
out = {}


def load(arm, table):
    p = frames / arm / f'{table}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def equal(a, b):
    if a is None or b is None:
        return {'missing': [a is None, b is None]}
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=True)
        return {'equal': True, 'rows': int(len(a))}
    except AssertionError as e:
        d = {'equal': False, 'rows': [int(len(a)), int(len(b))], 'msg': str(e)[:800]}
        try:
            ai, bi = set(a.index), set(b.index)
            d['only_base'] = len(ai - bi)
            d['only_cand'] = len(bi - ai)
            common = sorted(ai & bi, key=str)
            if a.index.is_unique and b.index.is_unique and common:
                x, y = a.loc[common], b.loc[common]
                cols = [c for c in a.columns if c in b.columns]
                diff = {}
                for c in cols:
                    m = ~((x[c] == y[c]) | (x[c].isna() & y[c].isna()))
                    if m.any():
                        diff[c] = int(m.sum())
                d['common_value_diffs'] = diff
            if 'j' in a.index.names:
                ja = set(a.index.get_level_values('j').astype(str))
                jb = set(b.index.get_level_values('j').astype(str))
                d['j_only_base'] = sorted(ja - jb)[:20]
                d['j_only_cand'] = sorted(jb - ja)[:20]
        except Exception as exc:  # noqa: BLE001
            d['detail_error'] = f'{type(exc).__name__}: {exc}'[:300]
        return d


for table in ('sample', 'household_roster', 'food_acquired', 'crop_production',
              'plot_inputs', 'plot_features'):
    out[f'{table}: base vs cand'] = equal(load('base', table), load('cand', table))
    attrs = [frames / arm / f'{table}.attrs.json' for arm in ('base', 'cand')]
    if all(p.exists() for p in attrs):
        a, b = (json.loads(p.read_text()) for p in attrs)
        out[f'{table}: attrs equal'] = a == b
        if a != b:
            out[f'{table}: attrs keys differing'] = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))

prev, cand = load('prev', 'community_prices'), load('cand', 'community_prices')
if prev is not None and cand is not None:
    tp = prev.index.get_level_values('t').isin(PP_T)
    tc = cand.index.get_level_values('t').isin(PP_T)
    out['community_prices PH: prev vs cand'] = equal(prev[~tp], cand[~tc])
    pp, pc = prev[tp], cand[tc]
    left = pp.index.difference(pc.index)
    arrived = pc.index.difference(pp.index)
    common = pp.index.intersection(pc.index)

    def by(frame, idx):
        f = frame.loc[idx].reset_index()
        f['Derivation'] = f['Derivation'].fillna('none')
        return {f'{t}|{u}|{k}': int(v) for (t, u, k), v in f.groupby(['t', 'u', 'Derivation']).size().items()}

    out['community_prices PP: prev -> cand'] = {
        'prev_rows': int(len(pp)), 'cand_rows': int(len(pc)),
        'leaving': int(len(left)), 'arriving': int(len(arrived)), 'common': int(len(common)),
        'leaving_by_t_u_key(prev)': by(pp, left),
        'arriving_by_t_u_key(cand)': by(pc, arrived),
        'arriving_j': {k: int(v) for k, v in pc.loc[arrived].index.get_level_values('j').value_counts().head(15).items()},
    }
    x, y = pp.loc[common], pc.loc[common]
    diffs = {}
    for c in ('Price', 'Derivation', 'Size'):
        m = ~((x[c] == y[c]) | (x[c].isna() & y[c].isna()))
        diffs[c] = int(m.sum())
    changed_key = x['Derivation'].fillna('none') != y['Derivation'].fillna('none')
    diffs['Derivation_changes'] = {f'{a} -> {b}': int(v) for (a, b), v in
                                   pd.DataFrame({'a': x.loc[changed_key, 'Derivation'].fillna('none'),
                                                 'b': y.loc[changed_key, 'Derivation'].fillna('none')})
                                   .groupby(['a', 'b']).size().items()}
    pchg = ~((x['Price'] == y['Price']) | (x['Price'].isna() & y['Price'].isna()))
    diffs['Price_changes_outside_key_changes'] = int((pchg & ~changed_key).sum())
    out['community_prices PP: common rows'] = diffs

(res / 'compare.json').write_text(json.dumps(out, indent=1, default=str))
print(json.dumps(out, indent=1, default=str)[:6000])
print('COMPARE DONE', flush=True)
