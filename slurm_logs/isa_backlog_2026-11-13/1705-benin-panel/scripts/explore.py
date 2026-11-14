"""Benin #1705 / #1118 / gift-purchase exploration (read-only, raw files).

Run inside a burst job from a clone at the base SHA, PYTHONPATH=<clone>.
Prints: the 2021-22 cover's link variables, the link counts, the link-quality
measures, and the 2018-19 s16b unit / gift / purchase facts.
"""
import sys
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', 60)
pd.set_option('display.max_rows', 400)

B = countries_root() / 'Benin'


def raw(wave, name, labels=False):
    return get_dataframe(B / wave / 'Data' / name, convert_categoricals=labels)


def varlabels(wave, name):
    p = get_data_file(f'Benin/{wave}/Data/{name}')
    with pd.io.stata.StataReader(p) as r:
        return r.variable_labels(), r.value_labels()


def hdr(s):
    print('\n' + '=' * 8, s, '=' * 8, flush=True)


def key(g, m):
    return f"{int(g)}{int(m):03d}"


# ---------------------------------------------------------------- covers
hdr('2021-22 cover: variables and labels')
vl21, val21 = varlabels('2021-22', 's00_me_ben2021.dta')
for k, v in vl21.items():
    print(f'  {k:28s} {v}')
c21 = raw('2021-22', 's00_me_ben2021.dta', labels=True)
c21n = raw('2021-22', 's00_me_ben2021.dta')
c18 = raw('2018-19', 's00_me_ben2018.dta', labels=True)
c18n = raw('2018-19', 's00_me_ben2018.dta')
print('rows 2021', len(c21), '2018', len(c18))
hdr('2018-19 cover: variables and labels')
vl18, _ = varlabels('2018-19', 's00_me_ben2018.dta')
for k, v in vl18.items():
    print(f'  {k:28s} {v}')

hdr('PanelHH and s00q07* in 2021-22')
for col in c21.columns:
    if col.lower().startswith('panel') or col.startswith('s00q07') or 'ehcvm1' in col.lower() \
            or '2018' in col or col.lower() in ('hhid', 'hhid_ehcvm1', 'id_men', 'idmen'):
        print(f'-- {col}: dtype {c21[col].dtype}, nonnull {c21[col].notna().sum()}')
        vc = c21[col].value_counts(dropna=False)
        print(vc.head(25).to_string())

k18 = {key(g, m) for g, m in zip(c18n['grappe'], c18n['menage'])}
print('2018 keys', len(k18), 'unique on cover', len(c18n) == len(k18))
c21n['key'] = [key(g, m) for g, m in zip(c21n['grappe'], c21n['menage'])]
c21['key'] = c21n['key'].values
print('2021 keys unique', c21n['key'].is_unique)
c21['recurs'] = c21['key'].isin(k18)
panel_raw = c21['PanelHH']
print('PanelHH x recurs')
print(pd.crosstab(panel_raw.astype(str), c21['recurs']))
if 's00q07d' in c21:
    print('s00q07d x PanelHH x recurs')
    print(pd.crosstab([c21['s00q07d'].astype(str), panel_raw.astype(str)], c21['recurs']))

# any 2018 identifiers recorded in 2021?  look for variables whose label mentions 2018 / EHCVM1 / ancien
hdr('2021 variables naming the previous survey')
for k, v in vl21.items():
    if any(w in (v or '').lower() for w in ('2018', 'ehcvm 1', 'ehcvm1', 'ancien', 'précédent', 'precedent',
                                              'passage', 'panel', 'identifiant', 'numéro', 'numero')):
        print(f'  {k:28s} {v}  nonnull={c21n[k].notna().sum() if k in c21n else "-"}')

# ---------------------------------------------------------------- the link
hdr('link counts')
flag_panel = panel_raw.astype(str).str.strip()
print(flag_panel.value_counts(dropna=False))
is_panel = flag_panel.str.lower().eq('panel')
linked = c21[is_panel & c21['recurs']]
print('PanelHH==Panel', int(is_panel.sum()))
print('  with a 2018-19 key', len(linked))
print('  without', int((is_panel & ~c21['recurs']).sum()))
print('  their keys:', sorted(c21.loc[is_panel & ~c21['recurs'], 'key'])[:20])
print('recurring keys flagged Non panel', int((~is_panel & c21['recurs']).sum()))
print('non-recurring Non panel', int((~is_panel & ~c21['recurs']).sum()))
print('duplicate targets among linked', int(linked['key'].duplicated().sum()))
# menage distribution for panel vs non-panel
print('menage of Panel / Non panel, describe')
print(c21n.assign(p=is_panel.values).groupby('p')['menage'].describe())
print('vague 2018 vs 2021 on linked')
v18 = dict(zip([key(g, m) for g, m in zip(c18n['grappe'], c18n['menage'])], c18n['vague']))
print(pd.crosstab(linked['key'].map(v18), c21n.loc[linked.index, 'vague']))

# ---------------------------------------------------------------- roster facts
hdr('rosters')
s18 = raw('2018-19', 's01_me_ben2018.dta')
s21 = raw('2021-22', 's01_me_ben2021.dta')
vls18, _ = varlabels('2018-19', 's01_me_ben2018.dta')
vls21, _ = varlabels('2021-22', 's01_me_ben2021.dta')
for k in ('s01q00a', 's01q00aa', 's01q01', 's01q02', 's01q03a', 's01q03b', 's01q03c', 's01q04a', 'membres__id'):
    print(f'  {k:12s} 2018: {vls18.get(k)}   2021: {vls21.get(k)}')
s18['key'] = [key(g, m) for g, m in zip(s18['grappe'], s18['menage'])]
s21['key'] = [key(g, m) for g, m in zip(s21['grappe'], s21['menage'])]
if 's01q00aa' in s21:
    print('s01q00aa', s21['s01q00aa'].value_counts(dropna=False).to_dict())
    s21m = s21[s21['s01q00aa'] != 2]
else:
    s21m = s21
print('2018 s01 rows', len(s18), '2021 members', len(s21m))

# interview year per household for age-from-birth-year fallback
def interview_year(cover, col_candidates):
    for c in col_candidates:
        if c in cover:
            d = pd.to_datetime(cover[c], errors='coerce')
            if d.notna().mean() > 0.5:
                return pd.Series(d.dt.year.values, index=[key(g, m) for g, m in zip(cover['grappe'], cover['menage'])]), c
    return None, None


y18, c18col = interview_year(c18n, ['s00q23a', 's00q24a', 's00q25a'])
y21, c21col = interview_year(c21n, ['s00q23a', 's00q24a', 's00q25a'])
print('interview date column 2018', c18col, '2021', c21col)
if y18 is not None:
    print('2018 interview years', y18.value_counts().to_dict(), ' 2021', y21.value_counts().to_dict())


def heads(s, years):
    h = s[s['s01q02'] == 1].copy()
    age = pd.to_numeric(h['s01q04a'], errors='coerce')
    by = pd.to_numeric(h['s01q03c'], errors='coerce').where(lambda x: (x > 1890) & (x < 2023))
    fy = h['key'].map(years) if years is not None else np.nan
    h['age'] = age.where(age.notna(), fy - by)
    h['sex'] = h['s01q01']
    return h


h18 = heads(s18, y18)
h21 = heads(s21m, y21)
print('households with !=1 head: 2018', int((h18.groupby('key').size() != 1).sum()),
      ' 2021', int((h21.groupby('key').size() != 1).sum()))
h18 = h18.drop_duplicates('key').set_index('key')
h21 = h21.drop_duplicates('key').set_index('key')
size18 = s18.groupby('key').size()
size21 = s21m.groupby('key').size()


def compare(pairs, name):
    """pairs: DataFrame with columns k21, k18."""
    a = h21.reindex(pairs['k21'].values)
    b = h18.reindex(pairs['k18'].values)
    sex = (a['sex'].values == b['sex'].values)
    sex_ok = pd.Series(sex)[pd.notna(a['sex'].values) & pd.notna(b['sex'].values)]
    d = pd.Series(a['age'].values - b['age'].values)
    s_a = size21.reindex(pairs['k21'].values).values
    s_b = size18.reindex(pairs['k18'].values).values
    ds = pd.Series(s_a - s_b)
    both = pd.Series(sex & (d.between(1, 5)).values)
    print(f'{name:48s} n={len(pairs):5d}  sex={sex_ok.mean():.3f} (n={len(sex_ok)})  '
          f'age d=1..5 {d.between(1, 5).mean():.3f}  |d-3|<=1 {(d - 3).abs().le(1).mean():.3f}  '
          f'|d-3|<=2 {(d - 3).abs().le(2).mean():.3f}  median d {d.median()}  '
          f'sex&age1..5 {both.mean():.3f}  size equal {(ds == 0).mean():.3f}  |dsize|<=2 {ds.abs().le(2).mean():.3f}  '
          f'median|dsize| {ds.abs().median()}', flush=True)
    return d, ds


pairs_link = pd.DataFrame({'k21': linked['key'].values, 'k18': linked['key'].values})
nonpanel_rec = c21[~is_panel & c21['recurs']]
pairs_np = pd.DataFrame({'k21': nonpanel_rec['key'].values, 'k18': nonpanel_rec['key'].values})

# control: each 2021-22 household against a DIFFERENT 2018-19 household in the same grappe
by_g = {}
for g, m in zip(c18n['grappe'], c18n['menage']):
    by_g.setdefault(int(g), []).append(key(g, m))
for g in by_g:
    by_g[g].sort()
ctrl = []
for g, k in zip(c21n['grappe'], c21n['key']):
    lst = by_g.get(int(g), [])
    others = [x for x in lst if x != k]
    if others:
        ctrl.append((k, others[(hash(k) % len(others))]))
pairs_ctrl = pd.DataFrame(ctrl, columns=['k21', 'k18'])
newhh = c21[~is_panel & ~c21['recurs']]
ctrl_new = pairs_ctrl[pairs_ctrl['k21'].isin(set(newhh['key']))]
ctrl_link = pairs_ctrl[pairs_ctrl['k21'].isin(set(linked['key']))]

d_link, ds_link = compare(pairs_link, 'linked (PanelHH=Panel, key recurs)')
compare(pairs_np, 'recurring key, PanelHH = Non panel')
compare(pairs_ctrl, 'control: another 2018 household, same grappe')
compare(ctrl_link, '  control restricted to linked 2021 households')
compare(ctrl_new, '  control restricted to new (non-recurring)')
print('linked: age-difference distribution')
print(d_link.value_counts(dropna=False).sort_index().head(40).to_string())
print('linked: size-difference distribution')
print(ds_link.value_counts(dropna=False).sort_index().to_string())

# cluster identity on the linked: region, milieu, and any finer geography in both covers
hdr('cluster identity on linked households')
geo = [c for c in ['s00q01', 's00q02', 's00q03', 's00q04', 's00q05', 's00q06', 'vague'] if c in c18n and c in c21n]
a = c21n.set_index('key').reindex(linked['key'])
b = c18n.assign(key=[key(g, m) for g, m in zip(c18n['grappe'], c18n['menage'])]).set_index('key').reindex(linked['key'])
for c in geo:
    eq = (a[c].values == b[c].values)
    print(f'  {c:8s} label18={vl18.get(c)!r:40s} label21={vl21.get(c)!r:40s} agree {eq.mean():.4f} of {len(eq)}')

# ---------------------------------------------------------------- s16b 2018-19
hdr('2018-19 s16b: units, gift and purchase')
vl16, val16 = varlabels('2018-19', 's16b_me_ben2018.dta')
for k, v in vl16.items():
    print(f'  {k:12s} {v}')
s16 = raw('2018-19', 's16b_me_ben2018.dta', labels=True)
s16n = raw('2018-19', 's16b_me_ben2018.dta')
app = (s16n['s16bq02'] == 1).values
print('rows', len(s16), 'applied', int(app.sum()))
a16, a16n = s16[app], s16n[app]
print('s16bq03b label counts (applied, dropna=False):')
print(a16['s16bq03b'].astype('string').value_counts(dropna=False).to_string())
print('s16bq03b NaN among applied:', int(a16['s16bq03b'].isna().sum()),
      ' raw code NaN:', int(a16n['s16bq03b'].isna().sum()))
print('rows whose unit label is literally Manquant/manquant:',
      int(a16['s16bq03b'].astype('string').str.strip().str.lower().eq('manquant').sum()))
print('the NaN-unit rows:')
print(a16.loc[a16['s16bq03b'].isna(), ['grappe', 'menage', 's16bq01', 's16bq03a', 's16bq03b', 's16bq05', 's16bq08']].to_string())
for col in ('s16bq04', 's16bq05', 's16bq06', 's16bq07a', 's16bq07b', 's16bq08', 's16bq09a', 's16bq09b', 's16bq10', 's16bq11'):
    if col in a16:
        print(f'-- {col} [{vl16.get(col)}] value counts (applied):')
        vc = a16[col].astype('string').value_counts(dropna=False)
        print(vc.head(30).to_string())
print('crosstab s16bq05 (gift?) x s16bq08 (purchase?)')
print(pd.crosstab(a16['s16bq05'].astype('string').fillna('<NA>'), a16['s16bq08'].astype('string').fillna('<NA>')))
print('Quantity sums: s16bq07a', pd.to_numeric(a16n['s16bq07a'], errors='coerce').sum(),
      ' s16bq09a', pd.to_numeric(a16n['s16bq09a'], errors='coerce').sum())
print('s16bq07a nonnull', int(a16n['s16bq07a'].notna().sum()), ' s16bq09a nonnull', int(a16n['s16bq09a'].notna().sum()))
print('s16bq07a>0 where s16bq05 != Oui:', int(((pd.to_numeric(a16n['s16bq07a'], errors='coerce') > 0)
                                                & (a16['s16bq05'].astype('string') != 'Oui')).sum()))
print('s16bq09a>0 where s16bq08 != Oui:', int(((pd.to_numeric(a16n['s16bq09a'], errors='coerce') > 0)
                                                & (a16['s16bq08'].astype('string') != 'Oui')).sum()))
print('value labels attached:', {k: v for k, v in val16.items() if k.lower().startswith('s16bq0')}.keys())
print('DONE')
