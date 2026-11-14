"""Member-number agreement with a same-grappe control (advisor item 3)."""
import warnings
import numpy as np
import pandas as pd
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
B = countries_root() / 'Benin'
def raw(w, n, labels=False):
    return get_dataframe(B / w / 'Data' / n, convert_categoricals=labels)
def key(g, m):
    return f"{int(g)}{int(m):03d}"
c21 = raw('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = raw('2018-19', 's00_me_ben2018.dta')
k18 = {key(g, m) for g, m in zip(c18['grappe'], c18['menage'])}
c21['key'] = [key(g, m) for g, m in zip(c21['grappe'], c21['menage'])]
panel = c21['PanelHH'].astype(str).str.strip().eq('Panel')
rec = c21['key'].isin(k18)
group = pd.Series(np.select([panel & rec, panel & ~rec, ~panel & rec], ['linked', 'panel-no-2018-key', 'nonpanel-recurring'], 'new'),
                  index=c21['key'])
by_g = {}
for g, m in zip(c18['grappe'], c18['menage']):
    by_g.setdefault(int(g), []).append(key(g, m))
for g in by_g: by_g[g].sort()
ctrl = {}
for g, k in zip(c21['grappe'], c21['key']):
    rest = [x for x in by_g.get(int(g), []) if x != k]
    if rest:
        ctrl[k] = rest[len(k) % len(rest)]
s18 = raw('2018-19', 's01_me_ben2018.dta')
s18['key'] = [key(g, m) for g, m in zip(s18['grappe'], s18['menage'])]
p18 = s18.set_index(['key', s18['s01q00a'].astype(int)])[['s01q01']].rename(columns={'s01q01': 'sex18'})
s21 = raw('2021-22', 's01_me_ben2021.dta')
s21['key'] = [key(g, m) for g, m in zip(s21['grappe'], s21['menage'])]
s21['g'] = s21['key'].map(group)
pre = s21[s21['s01q00aa'].notna()].copy()
pre['pid'] = pre['membres__id'].astype(int)
for label, target in (('same key (the link)', pre['key']), ('control: another 2018-19 household, same grappe', pre['key'].map(ctrl))):
    m = pre.assign(tk=target.values).join(p18, on=['tk', 'pid'])
    m['found'] = m['sex18'].notna()
    m['sexeq'] = (m['s01q01'] == m['sex18']).where((m['s01q00aa'] == 1) & m['found']).astype(float)
    print('==', label)
    print(m.groupby('g').agg(members=('pid', 'size'), found=('found', 'mean'), sex_agree_of_found_current=('sexeq', 'mean')).to_string())
# household-level: share of 2018 members (all of them) that appear pre-loaded under the same number
n18 = s18.groupby('key').size()
for label, mp in (('same key', None), ('control', ctrl)):
    hh = pre.groupby('key')['pid'].nunique()
    tk = hh.index if mp is None else hh.index.map(mp)
    print(label, 'linked: pre-loaded members / 2018 roster size, median',
          float((hh.values / n18.reindex(tk).values)[hh.index.map(group) == 'linked'].__array__().astype(float)[~np.isnan((hh.values / n18.reindex(tk).values)[hh.index.map(group) == 'linked'].astype(float))].mean()))
print('DONE')
