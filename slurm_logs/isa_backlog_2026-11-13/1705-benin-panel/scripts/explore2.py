"""Benin #1705, second pass: other 2021 files' id variables; pre-loaded rosters;
person-level agreement by member number.  Read-only, raw files."""
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

pd.set_option('display.width', 200)
B = countries_root() / 'Benin'


def raw(wave, name, labels=False):
    return get_dataframe(B / wave / 'Data' / name, convert_categoricals=labels)


def varlabels(wave, name):
    p = get_data_file(f'Benin/{wave}/Data/{name}')
    with pd.io.stata.StataReader(p) as r:
        return r.variable_labels()


def key(g, m):
    return f"{int(g)}{int(m):03d}"


print('== variables of the 2021 household-level files', flush=True)
for name in ('ehcvm_menage_ben2021.dta', 'ehcvm_welfare_ben2021.dta', 'ehcvm_ponderations_ben2021.dta',
             'ehcvm_individu_ben2021.dta'):
    vl = varlabels('2021-22', name)
    print(f'-- {name}: {len(vl)} variables')
    print('   ', ', '.join(vl))
    hits = {k: v for k, v in vl.items()
            if any(w in (k + ' ' + (v or '')).lower()
                   for w in ('hhid', '2018', 'ehcvm1', 'panel', 'ancien', 'prec', 'numind', 'idmen', 'id_'))}
    for k, v in hits.items():
        print(f'    HIT {k}: {v}')
for name in ('s01_me_ben2021.dta',):
    vl = varlabels('2021-22', name)
    print(f'-- {name}: {len(vl)} variables:', ', '.join(vl))

c21 = raw('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = raw('2018-19', 's00_me_ben2018.dta')
k18 = {key(g, m) for g, m in zip(c18['grappe'], c18['menage'])}
c21['key'] = [key(g, m) for g, m in zip(c21['grappe'], c21['menage'])]
panel = c21['PanelHH'].astype(str).str.strip().eq('Panel')
rec = c21['key'].isin(k18)
group = pd.Series(np.select([panel & rec, panel & ~rec, ~panel & rec], ['linked', 'panel-no-2018-key', 'nonpanel-recurring'],
                            'new'), index=c21['key'])
print(group.value_counts())

# welfare / menage files: any id that differs from (grappe, menage)?
for name in ('ehcvm_welfare_ben2021.dta', 'ehcvm_menage_ben2021.dta'):
    w = raw('2021-22', name)
    cols = [c for c in w.columns if 'hhid' in c.lower() or 'panel' in c.lower()]
    print(name, 'id/panel columns:', cols)
    for c in cols:
        print(w[c].astype(str).head(5).tolist(), 'nunique', w[c].nunique())
        if 'panel' in c.lower():
            w['key'] = [key(g, m) for g, m in zip(w['grappe'], w['menage'])]
            print(pd.crosstab(w['key'].map(group), w[c].astype(str)))

print('== pre-loaded rosters (s01q00aa answered) by group', flush=True)
s21 = raw('2021-22', 's01_me_ben2021.dta')
s21['key'] = [key(g, m) for g, m in zip(s21['grappe'], s21['menage'])]
s21['g'] = s21['key'].map(group)
s21['pre'] = s21['s01q00aa'].notna()
hh = s21.groupby('key').agg(pre_any=('pre', 'any'), pre_all=('pre', 'all'), n=('pre', 'size'),
                            gone=('s01q00aa', lambda x: (x == 2).sum()))
hh['g'] = hh.index.map(group)
print(hh.groupby('g').agg(households=('n', 'size'), any_preloaded=('pre_any', 'mean'),
                          all_preloaded=('pre_all', 'mean'), mean_rows=('n', 'mean'),
                          any_left=('gone', lambda x: (x > 0).mean())).to_string())

print('== person level: 2021 member number vs 2018 s01q00a, sex agreement, pre-loaded members', flush=True)
s18 = raw('2018-19', 's01_me_ben2018.dta')
s18['key'] = [key(g, m) for g, m in zip(s18['grappe'], s18['menage'])]
p18 = s18.set_index(['key', s18['s01q00a'].astype(int)])
p21 = s21[s21['pre']].copy()
p21['pid'] = p21['membres__id'].astype(int)
m = p21.join(p18[['s01q01', 's01q04a']].rename(columns={'s01q01': 'sex18', 's01q04a': 'age18'}),
             on=['key', 'pid'])
m['found'] = m['sex18'].notna()
m['sexeq'] = (m['s01q01'] == m['sex18']).where(m['s01q00aa'] == 1).astype(float)
print(m.groupby('g').agg(members=('pid', 'size'), found_2018=('found', 'mean'),
                         sex_agree=('sexeq', 'mean')).to_string())
# control: shift to another household in the same grappe
print('DONE')
