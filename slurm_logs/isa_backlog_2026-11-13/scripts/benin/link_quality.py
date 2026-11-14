"""Link quality on a crosswalk.  usage: link_quality.py <updated_ids.json>

Groups: linked (the crosswalk), the excepted 63, the 116 recurring Non-panel keys, and a
control (each 2021-22 household against another 2018-19 household of the same grappe).
Head sex / age / household size, and member-level sex and birth year by member number.
"""
import json
import sys
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

B = countries_root() / 'Benin'
links = json.load(open(sys.argv[1]))['2021-22']
linked = {v for v in links.values()}


def raw(w, n, labels=False):
    return get_dataframe(B / w / 'Data' / n, convert_categoricals=labels)


def key(g, m):
    return f"{int(g)}{int(m):03d}"


c21 = raw('2021-22', 's00_me_ben2021.dta', labels=True)
c21n = raw('2021-22', 's00_me_ben2021.dta')
c18 = raw('2018-19', 's00_me_ben2018.dta')
c21['key'] = [key(g, m) for g, m in zip(c21['grappe'], c21['menage'])]
k18 = {key(g, m) for g, m in zip(c18['grappe'], c18['menage'])}
panel = c21['PanelHH'].astype(str).str.strip().eq('Panel')
rec = c21['key'].isin(k18)
excepted = set(c21.loc[panel & rec & ~c21['key'].isin(linked), 'key'])
nonpanel_rec = set(c21.loc[~panel & rec, 'key'])
print('linked', len(linked), 'excepted', len(excepted), 'nonpanel recurring', len(nonpanel_rec))

s18 = raw('2018-19', 's01_me_ben2018.dta')
s21 = raw('2021-22', 's01_me_ben2021.dta')
s18['key'] = [key(g, m) for g, m in zip(s18['grappe'], s18['menage'])]
s21['key'] = [key(g, m) for g, m in zip(s21['grappe'], s21['menage'])]
s21m = s21[s21['s01q00aa'] != 2]


def iyear(cover):
    d = pd.to_datetime(cover['s00q23a'], errors='coerce')
    return pd.Series(d.dt.year.values, index=[key(g, m) for g, m in zip(cover['grappe'], cover['menage'])])


y18, y21 = iyear(c18), iyear(c21n)


def heads(s, years):
    h = s[s['s01q02'] == 1].copy()
    age = pd.to_numeric(h['s01q04a'], errors='coerce')
    by = pd.to_numeric(h['s01q03c'], errors='coerce').where(lambda x: (x > 1890) & (x < 2023))
    h['age'] = age.where(age.notna(), h['key'].map(years) - by)
    h['by'] = by
    return h.drop_duplicates('key').set_index('key')


h18, h21 = heads(s18, y18), heads(s21m, y21)
size18, size21 = s18.groupby('key').size(), s21m.groupby('key').size()

by_g = {}
for g, m in zip(c18['grappe'], c18['menage']):
    by_g.setdefault(int(g), []).append(key(g, m))
for g in by_g:
    by_g[g].sort()
ctrl = {}
for g, k in zip(c21['grappe'], c21['key']):
    rest = [x for x in by_g.get(int(g), []) if x != k]
    if rest:
        ctrl[k] = rest[len(k) % len(rest)]   # deterministic, as tests/test_benin_panel_ids.py

p18 = s18.assign(pid=s18['s01q00a'].astype(int)).set_index(['key', 'pid'])[['s01q01', 's01q03c']]
p18.columns = ['sex18', 'by18']
pre = s21[s21['s01q00aa'].notna()].assign(pid=lambda d: d['membres__id'].astype(int))


def head_row(name, k21, k18):
    a, b = h21.reindex(k21), h18.reindex(k18)
    sex = pd.Series(a['s01q01'].values == b['s01q01'].values)
    d = pd.Series(a['age'].values - b['age'].values)
    ds = pd.Series(size21.reindex(k21).values - size18.reindex(k18).values)
    bye = pd.Series((a['by'].values - b['by'].values))
    sub = pre[pre['key'].isin(set(k21))].copy()
    tmap = dict(zip(k21, k18))
    sub['tk'] = sub['key'].map(tmap)
    m = sub.join(p18, on=['tk', 'pid'])
    cur = m[(m['s01q00aa'] == 1) & m['sex18'].notna()]
    msex = (cur['s01q01'] == cur['sex18']).mean() if len(cur) else np.nan
    mby = ((pd.to_numeric(cur['s01q03c'], errors='coerce') - pd.to_numeric(cur['by18'], errors='coerce')).abs() <= 1).mean() if len(cur) else np.nan
    print(f'{name:44s} n={len(k21):5d} head sex {sex.mean():.3f}  head age +1..+5 {d.between(1, 5).mean():.3f}'
          f'  head birth year +-1 {(bye.abs() <= 1).mean():.3f}  size equal {(ds == 0).mean():.3f}'
          f'  size within 2 {ds.abs().le(2).mean():.3f}  members found {m["sex18"].notna().mean() if len(m) else float("nan"):.3f}'
          f'  member sex {msex:.3f}  member birth year +-1 {mby:.3f}', flush=True)


L = sorted(linked)
head_row('linked (crosswalk)', L, L)
head_row('  control for the linked', [k for k in L if k in ctrl], [ctrl[k] for k in L if k in ctrl])
E = sorted(excepted)
head_row('excepted (whole pre-load gone)', E, E)
head_row('  control for the excepted', [k for k in E if k in ctrl], [ctrl[k] for k in E if k in ctrl])
N = sorted(nonpanel_rec)
head_row('recurring key, PanelHH = Non panel', N, N)
allk = list(c21['key'])
head_row('control: every 2021-22 household', [k for k in allk if k in ctrl], [ctrl[k] for k in allk if k in ctrl])
print('DONE')
