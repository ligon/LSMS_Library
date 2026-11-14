"""Follow-up probes: W1 division vs PH/household by unit and N band; W5 g-row
text/c2q2a categories; hand-run Site Q offenders by derivation key.

usage: python probe2.py <clone> <framesdir> <resultsdir>   (aggregates only)
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env, Recorder  # noqa: E402

clone, frames, results = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
head = assert_env(clone)
import importlib  # noqa: E402
import warnings  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

pp = importlib.import_module('lsms_library.countries.Nigeria._.pp_community_prices')
R = Recorder(results / 'cand.probe2.json')
R.data['head'] = head
cp = pd.read_parquet(frames / 'cand.community_prices.parquet').reset_index()
fa = pd.read_parquet(frames / 'cand.food_acquired.parquet').reset_index()
RAW = {k: get_dataframe(str(countries_root() / 'Nigeria' / pp.pp_source(k)['source_path']),
                        convert_categoricals=False) for k in ('W1', 'W5')}


def q(s):
    s = pd.Series(s).dropna()
    return {'n': int(len(s)), 'median': round(float(s.median()), 3) if len(s) else None,
            'within2x': round(float(((s > .5) & (s < 2)).mean()), 3) if len(s) else None,
            'p10': round(float(s.quantile(.1)), 3) if len(s) else None,
            'p90': round(float(s.quantile(.9)), 3) if len(s) else None}


def w1_division():
    raw = RAW['W1']
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        d = pp.pp_dispositions(raw, 'W1')
    d['N'] = pd.to_numeric(raw['sc2q3b'], errors='coerce').values
    d['reported'] = pd.to_numeric(raw['sc2q3a'], errors='coerce').values
    d = d[d['Disposition'].isin(pp.PP_SERVED_DISPOSITIONS)].copy()
    metric = {'g': 'g', 'ml': 'ml', 'Kg': 'Kg', 'l': 'l'}
    d['ucls'] = d['u'].map(metric).fillna('non-metric')
    d.loc[d['u'].eq('Unknown'), 'ucls'] = 'Unknown'
    d['band'] = pd.cut(d['N'], [0, 1.0001, 5.0001, 50.0001, np.inf], labels=['1', '2-5', '6-50', '>50']).astype(str)
    ph = cp[cp['t'].eq('2011Q1') & cp['obs'].eq(1)].groupby(['v', 'j', 'u'])['Price'].median().rename('ph').reset_index()
    f = fa[fa['s'].eq('purchased') & fa['t'].eq('2010Q3') & fa['Quantity'].gt(0) & fa['Expenditure'].gt(0)].copy()
    f['uv'] = f['Expenditure'] / f['Quantity']
    hh = f.groupby(['v', 'j', 'u'])['uv'].median().rename('hh').reset_index()
    out = {}
    for refname, ref, col in (('PH2011Q1', ph, 'ph'), ('household2010Q3', hh, 'hh')):
        m = d.merge(ref, on=['v', 'j', 'u'])
        res = {}
        for (c, b), g in m.groupby(['ucls', 'band']):
            res[f'{c}|N{b}'] = {'served_over_ref': q(g['Price'] / g[col]),
                                'undivided_over_ref': q(g['reported'] / g[col])}
        out[refname] = res
        # top non-metric units, divided rows only
        nm = m[m['ucls'].eq('non-metric') & m['band'].ne('1')]
        out[refname + '_nonmetric_divided_by_u'] = {
            u: {'served_over_ref': q(g['Price'] / g[col]), 'undivided_over_ref': q(g['reported'] / g[col])}
            for u, g in nm.groupby('u') if len(g) >= 20}
    out['served_counts'] = {f'{c}|N{b}': int(n) for (c, b), n in d.groupby(['ucls', 'band']).size().items()}
    return out


R.step('w1_division', w1_division)


def w5_text():
    raw = RAW['W5']
    spec = pp.pp_source('W5')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        d = pp.pp_dispositions(raw, 'W5')
    m = d['Disposition'].eq('w5-metric-quantity') & d['u'].eq('g')
    txt = raw['c2q2b_os'].astype('string')
    qa = pd.to_numeric(raw['c2q2a'], errors='coerce')

    def nums(s):
        return [float(x) for x in re.findall(r'\d+(?:\.\d+)?', str(s))] if pd.notna(s) else []

    def tunit(s):
        u = str(s).upper()
        if re.search(r'\d\s*KG|\bKG\b|KILO', u):
            return 'kg'
        if re.search(r'\d\s*G\b|\d\s*GM|\bG\b|GRAM|\d\s*GR', u):
            return 'g'
        return 'none'
    cats = []
    for s, a in zip(txt[m], qa[m]):
        ns = nums(s)
        tu = tunit(s)
        first = ns[0] if ns else np.nan
        cat = (tu,
               'first==q' if ns and np.isclose(first, a, rtol=1e-4) else
               'first*1000==q' if ns and np.isclose(first * 1000, a, rtol=1e-4) else
               'some==q' if any(np.isclose(x, a, rtol=1e-4) for x in ns) else
               'some*1000==q' if any(np.isclose(x * 1000, a, rtol=1e-4) for x in ns) else
               'product==q' if len(ns) > 1 and np.isclose(np.prod(ns), a, rtol=1e-4) else
               'none', len(ns))
        cats.append('|'.join(map(str, cat)))
    price = d.loc[m, 'Price']
    med = d[d['Disposition'].eq('w5-metric-quantity')].groupby(['j', 'u'])['Price'].transform('median')[m]
    rel = (price / med)
    out = {'categories': pd.Series(cats).value_counts().to_dict()}
    cs = pd.Series(cats, index=price.index)
    out['served_over_item_median_by_category'] = {c: [round(float(x), 2) for x in rel[cs.eq(c)]][:10]
                                                  for c in cs.unique() if not c.startswith('g|first==q')}
    out['all_q'] = q(rel)
    return out


R.step('w5_text', w5_text)


def site_q():
    from lsms_library.quantity_audit import audit_quantities
    out = {}
    df = cp.copy()
    df['Derivation'] = df['Derivation'].astype('string')
    for label, ts in (('PP', ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']),
                      ('PH', ['2011Q1', '2013Q1', '2016Q1', '2019Q1', '2024Q1'])):
        sub = df[df['t'].isin(ts)].set_index(['t', 'v', 'j', 'u', 'variety', 'obs'])
        reps = audit_quantities(sub, 'Price', country='Nigeria', table='community_prices',
                                tails=('high', 'low'))
        rows = []
        for r in reps:
            offs = r.get('offenders')
            if isinstance(offs, pd.DataFrame):
                o = offs
            else:
                o = pd.DataFrame(offs or [])
            rec = {'wave': r.get('wave'), 'direction': r.get('direction'),
                   'n_implausible': r.get('n_implausible'), 'rows': r.get('rows'),
                   'max_ratio': r.get('max_ratio'), 'offender_columns': list(map(str, o.columns))}
            if len(o):
                for col in ('u', 'Derivation'):
                    if col in o:
                        rec[f'by_{col}'] = {str(k): int(v) for k, v in o[col].astype('string').fillna('NA').value_counts().items()}
                # derivation by joining back on index
                key = [c for c in ('t', 'v', 'j', 'u', 'variety', 'obs') if c in o.columns]
                if key and 'Derivation' not in o:
                    j = o[key].astype('string').merge(df[key + ['Derivation']].astype('string'), on=key, how='left')
                    rec['by_Derivation'] = {str(k): int(v) for k, v in j['Derivation'].fillna('none').value_counts().items()}
            rows.append(rec)
        out[label] = rows
    return out


R.step('site_q', site_q)
R.data['done'] = True
R.flush()
print('PROBE2 DONE', flush=True)
