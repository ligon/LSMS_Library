"""Cold-build Nigeria tables + the 3-country Feature for one side into a private root.

usage: python build.py <clone> <side> <framesdir> <resultsdir>
Aggregates go to <resultsdir>/<side>.build.json; frames to <framesdir> (private).
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env, Recorder  # noqa: E402

clone, side, frames, results = sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
frames.mkdir(parents=True, exist_ok=True)
results.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

R = Recorder(results / f'{side}.build.json')
R.data['head'] = head
PP_Q = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']
PH_Q = ['2011Q1', '2013Q1', '2016Q1', '2019Q1', '2024Q1']


def save(name, df):
    d = df.copy()
    d.attrs = {}
    p = frames / f'{side}.{name}.parquet'
    d.to_parquet(p)
    return str(p)


def describe(name, df, keep_attrs=('derivations', 'labels_unmatched', 'population')):
    t0 = time.time()
    out = {'rows': int(len(df)), 'columns': list(map(str, df.columns)),
           'index': list(map(str, df.index.names)), 'unique': bool(df.index.is_unique),
           'dtypes': {str(k): str(v) for k, v in df.dtypes.items()}}
    if 't' in df.index.names:
        out['rows_by_t'] = {str(k): int(v) for k, v in
                            df.index.get_level_values('t').value_counts().sort_index().items()}
    if 'country' in df.index.names:
        out['rows_by_country'] = {str(k): int(v) for k, v in
                                  df.index.get_level_values('country').value_counts().items()}
    out['attrs_keys'] = sorted(map(str, df.attrs))
    for k in keep_attrs:
        if k in df.attrs and k != 'population':
            import json
            out[f'attrs_{k}'] = json.loads(json.dumps(df.attrs[k], default=str))
    out['path'] = save(name, df)
    out['save_seconds'] = round(time.time() - t0, 1)
    return out


c = ll.Country('Nigeria')
R.step('waves', lambda: list(c.waves))
R.step('data_scheme', lambda: list(c.data_scheme))


def registry():
    import importlib; D = importlib.import_module('lsms_library.derivations')
    recs = D.derivation_records('Nigeria')
    return {k: {'rows': getattr(v, 'rows', None), 'table': getattr(v, 'table', None),
                'waves': getattr(v, 'waves', None)} for k, v in recs.items()
            if 'community_prices' in k}


R.step('registry_community_prices', registry)

t0 = time.time()
cp = R.step('community_prices_build', lambda: c.community_prices(), store=False)
R.data['community_prices_seconds'] = round(time.time() - t0, 1)
if cp is not None:
    R.step('community_prices', lambda: describe('community_prices', cp))


def subset():
    ws = [w for w in c.waves if '2018' in str(w)]
    out = {}
    for w in ws:
        sub = c.community_prices(waves=[w])
        ts = sorted(set(sub.index.get_level_values('t')))
        full = cp[cp.index.get_level_values('t').isin(ts)]
        a = sub.drop(columns=[x for x in sub.columns if x not in full.columns]).sort_index()
        b = full[a.columns].sort_index()
        eq = a.index.equals(b.index) and a.equals(b)
        out[str(w)] = {'rows': int(len(sub)), 't': ts, 'equals_full_slice': bool(eq),
                       'full_slice_rows': int(len(full))}
    return out


if cp is not None:
    R.step('community_prices_waves_subset', subset)


def agg():
    df = c.community_prices(labels='Aggregate')
    out = describe('community_prices_aggregate', df)
    out.pop('dtypes', None)
    out['dup_rows'] = int(df.index.duplicated(keep=False).sum())
    return out


R.step('community_prices_aggregate', agg)

for table in ('sample', 'household_roster', 'food_acquired', 'plot_features', 'crop_production'):
    def build(table=table):
        t0 = time.time()
        df = getattr(c, table)()
        out = describe(table, df)
        out['seconds'] = round(time.time() - t0, 1)
        return out
    R.step(table, build)


def audit_reports():
    out = {}
    from lsms_library.null_read_audit import null_read_reports
    from lsms_library.null_index_audit import null_index_reports
    from lsms_library.quantity_audit import quantity_reports
    from lsms_library.sampling import sampling_reports
    from lsms_library.country import grain_reports
    for name, fn in (('null_read', null_read_reports), ('null_index', null_index_reports),
                     ('quantity', quantity_reports), ('sampling', sampling_reports),
                     ('grain', grain_reports)):
        try:
            reps = fn(country='Nigeria')
        except TypeError:
            reps = fn('Nigeria')
        by = {}
        for r in reps:
            key = str(r.get('table', '?')) if isinstance(r, dict) else '?'
            by.setdefault(key, []).append(
                {k: (v if not isinstance(v, (list, dict, pd.DataFrame)) else f'<{type(v).__name__} len={len(v)}>')
                 for k, v in (r.items() if isinstance(r, dict) else [])})
        out[name] = {k: {'n': len(v), 'reports': v[:30]} for k, v in by.items()}
    return out


R.step('audit_reports', audit_reports)


def site_q_by_hand():
    from lsms_library.quantity_audit import audit_quantities
    if cp is None:
        return None
    df = cp.reset_index()
    out = {}
    for label, mask in (('PP', df['t'].isin(PP_Q)), ('PH', df['t'].isin(PH_Q))):
        sub = df[mask].set_index(['t', 'v', 'j', 'u', 'variety', 'obs'])
        reps = audit_quantities(sub, 'Price', country='Nigeria', table='community_prices',
                                tails=('high', 'low'))
        rows = []
        for r in reps:
            offs = r.get('offenders') or r.get('rows') or []
            rows.append({'t': r.get('t'), 'direction': r.get('direction'),
                         'n': r.get('n_flagged', len(offs) if hasattr(offs, '__len__') else None),
                         'n_checked': r.get('n_checked', r.get('n_rows')),
                         'keys': sorted(map(str, r.keys()))})
        out[label] = {'reports': rows, 'n_rows': int(mask.sum())}
    return out


R.step('site_q_by_hand_price', site_q_by_hand)


def ph_derivation_inputs():
    out = {}
    for key in ('Nigeria::community_prices::c8-unrecorded-size-drop',
                'Nigeria::community_prices::c8-metric-size'):
        inp = c.derivation_inputs(key)
        rec = {'rows': int(len(inp)), 'index': list(map(str, inp.index.names)),
               'columns': list(map(str, inp.columns))}
        flat = inp.reset_index()
        if 't' in flat:
            rec['by_t'] = {str(k): int(v) for k, v in flat['t'].value_counts().sort_index().items()}
        if 'unit_label' in flat:
            rec['by_t_unit_label'] = {f'{a}|{b}': int(n) for (a, b), n in
                                      flat.groupby(['t', 'unit_label']).size().items()}
        out[key] = rec
    return out


R.step('ph_derivation_inputs', ph_derivation_inputs)


def feature():
    t0 = time.time()
    f = ll.Feature('community_prices')(['Nigeria', 'Tanzania', 'Uganda'])
    out = describe('feature_community_prices', f)
    out['seconds'] = round(time.time() - t0, 1)
    out['dup_rows'] = int(f.index.duplicated(keep=False).sum())
    if 'Size' in f.columns:
        out['size_nonnull_by_country'] = {str(k): int(v) for k, v in
                                          f['Size'].notna().groupby(level='country').sum().items()}
    return out


R.step('feature_community_prices', feature)


def hashes():
    out = {}
    from lsms_library._build_registry import build_transforms_fingerprint
    for country in ('Nigeria', 'Tanzania', 'Uganda', 'Malawi'):
        cc = ll.Country(country)
        tabs = list(cc.data_scheme) if country == 'Nigeria' else \
            [t for t in ('community_prices', 'food_acquired', 'sample', 'household_roster')
             if t in cc.data_scheme]
        hh = {}
        for t in tabs:
            try:
                hh[t] = cc._table_cache_hash(t, list(cc.waves))
            except Exception as e:  # noqa: BLE001
                hh[t] = f'ERR {type(e).__name__}: {e}'[:200]
        out[country] = hh
    out['btf'] = {t: build_transforms_fingerprint(t) for t in
                  ('community_prices', 'food_acquired', 'sample', 'crop_production', None)}
    return out


R.step('hashes', hashes)
R.data['done'] = True
R.flush()
print('BUILD DONE', side, flush=True)
