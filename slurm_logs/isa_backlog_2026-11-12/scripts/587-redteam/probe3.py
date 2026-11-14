"""Within-W1 check: divided (N>1) prices vs the same item-unit's N=1 prices (national,
and within state), so season/inflation/PH-unit differences cannot explain the gap.
usage: python probe3.py <clone> <resultsdir>   (aggregates only)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env, Recorder  # noqa: E402
clone, results = sys.argv[1], Path(sys.argv[2])
head = assert_env(clone)
import importlib, warnings  # noqa: E402,E401
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
pp = importlib.import_module('lsms_library.countries.Nigeria._.pp_community_prices')
R = Recorder(results / 'cand.probe3.json'); R.data['head'] = head
raw = get_dataframe(str(countries_root() / 'Nigeria' / pp.pp_source('W1')['source_path']), convert_categoricals=False)


def q(s):
    s = pd.Series(s).dropna()
    return {'n': int(len(s)), 'median': round(float(s.median()), 3) if len(s) else None,
            'within2x': round(float(((s > .5) & (s < 2)).mean()), 3) if len(s) else None}


def run():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        d = pp.pp_dispositions(raw, 'W1')
    d['N'] = pd.to_numeric(raw['sc2q3b'], errors='coerce').values
    d['rep'] = pd.to_numeric(raw['sc2q3a'], errors='coerce').values
    d['state'] = pd.to_numeric(raw['state'], errors='coerce').values
    d = d[d['Disposition'].isin(pp.PP_SERVED_DISPOSITIONS)].copy()
    d['ucls'] = d['u'].where(d['u'].isin(['g', 'ml', 'Kg', 'l', 'Unknown']), 'non-metric')
    d['band'] = pd.cut(d['N'], [0, 1.0001, 5.0001, 50.0001, np.inf], labels=['1', '2-5', '6-50', '>50']).astype(str)
    out = {}
    for scope, keys in (('national', ['j', 'u']), ('state', ['state', 'j', 'u'])):
        ref = d[d['band'].eq('1')].groupby(keys)['Price'].agg(['median', 'size'])
        ref = ref[ref['size'] >= 3]['median'].rename('ref').reset_index()
        m = d[d['band'].ne('1')].merge(ref, on=keys)
        res = {}
        for (c, b), g in m.groupby(['ucls', 'band']):
            res[f'{c}|N{b}'] = {'divided_over_N1': q(g['Price'] / g['ref']),
                                'undivided_over_N1': q(g['rep'] / g['ref'])}
        out[scope] = res
        nm = m[m['ucls'].eq('non-metric')]
        out[scope + '_nonmetric_by_u'] = {u: {'divided_over_N1': q(g['Price'] / g['ref']),
                                              'undivided_over_N1': q(g['rep'] / g['ref'])}
                                          for u, g in nm.groupby('u') if len(g) >= 30}
    # does the reported Naira scale with N?  slope of log(rep) on log(N) within (j,u), N>1 rows
    g = d[d['N'].gt(1) & d['rep'].gt(0)].copy()
    g['lr'], g['ln'] = np.log(g['rep']), np.log(g['N'])
    sl = {}
    for c, gg in g.groupby('ucls'):
        gg = gg.copy()
        gg['lr'] -= gg.groupby(['j', 'u'])['lr'].transform('mean')
        gg['ln'] -= gg.groupby(['j', 'u'])['ln'].transform('mean')
        den = float((gg['ln'] ** 2).sum())
        sl[c] = {'rows': int(len(gg)), 'within_item_unit_elasticity_of_naira_wrt_N':
                 round(float((gg['lr'] * gg['ln']).sum() / den), 3) if den > 0 else None}
    out['elasticity'] = sl
    return out


R.step('w1_within', run)
R.data['done'] = True; R.flush(); print('PROBE3 DONE', flush=True)
