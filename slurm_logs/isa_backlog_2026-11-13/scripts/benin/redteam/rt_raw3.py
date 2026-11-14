"""Red-team follow-up 3: UEMOA's own `panel` flag and hhid; the 63 linked households whose
pre-loaded members are all marked as left, and the 19 with no pre-load, matched on sex and
birth year IGNORING member number (with a same-grappe control); nitrogen_kg / seed_kg on the
base and candidate plot_inputs; the relabelled plot_inputs row.
usage: rt_raw3.py <clone> <outdir> <main_out>"""
import json
import pickle
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
clone, outdir, main = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

C = clone / 'lsms_library' / 'countries' / 'Benin'
R = {}


def key(g, m):
    return f'{int(g)}{int(m):03d}'


def dta(wave, name, labels=False):
    return get_dataframe(C / wave / 'Data' / name, convert_categoricals=labels)


def vlabels(wave, name):
    p = get_data_file(f'Benin/{wave}/Data/{name}')
    with pd.io.stata.StataReader(p) as rd:
        return rd.variable_labels()


c21 = dta('2021-22', 's00_me_ben2021.dta')
c21l = dta('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = dta('2018-19', 's00_me_ben2018.dta')
c21['key'] = [key(g, m) for g, m in zip(c21.grappe, c21.menage)]
c18['key'] = [key(g, m) for g, m in zip(c18.grappe, c18.menage)]
panel = c21l['PanelHH'].astype(str).str.strip()
rec = c21.key.isin(set(c18.key))
c21['grp'] = np.select([(panel == 'Panel') & rec, (panel == 'Panel') & ~rec, (panel != 'Panel') & rec],
                       ['linked', 'panel_nokey', 'nonpanel_recur'], 'new')
gmap = dict(zip(c21.key, c21.grp))

# the 63 and the 19
s18 = dta('2018-19', 's01_me_ben2018.dta')
s21 = dta('2021-22', 's01_me_ben2021.dta')
s18['key'] = [key(g, m) for g, m in zip(s18.grappe, s18.menage)]
s21['key'] = [key(g, m) for g, m in zip(s21.grappe, s21.menage)]
by = lambda s: pd.to_numeric(s, errors='coerce').where(lambda x: (x > 1880) & (x < 2023))  # noqa: E731
s18['by'] = by(s18.s01q03c)
s21['by'] = by(s21.s01q03c)
pre = s21[s21.s01q00aa.notna()]
st = pre[pre.key.map(gmap) == 'linked'].groupby('key')['s01q00aa'].apply(lambda s: (s != 1).all())
gone = sorted(st[st].index)
nopre = sorted(set(c21.loc[c21.grp == 'linked', 'key']) - set(pre.key))
cur = s21[s21.s01q00aa != 2]          # still members and new members
g18 = {g: sorted(v) for g, v in c18.groupby('grappe')['key']}
gr = dict(zip(c21.key, c21.grappe))
P18 = {k: list(zip(g.s01q01, g.by)) for k, g in s18.groupby('key')}


def person_match(k21, k18):
    """members of 2021 household k21 (current) with a 2018 member of k18 of the same sex and a
    birth year within 1 (both recorded), ignoring member number."""
    people = P18.get(k18, [])
    m = cur[cur.key == k21]
    hits = 0
    for sx, b in zip(m.s01q01, m.by):
        if pd.isna(b):
            continue
        if any(sx == s2 and not pd.isna(b2) and abs(b - b2) <= 1 for s2, b2 in people):
            hits += 1
    return hits, int(m.by.notna().sum())


def other(k, g):
    ks = [x for x in g18.get(g, []) if x != k]
    return ks[sum(map(ord, k)) % len(ks)] if ks else None


def summarize(keys, name):
    own = [person_match(k, k) for k in keys]
    ctl = [person_match(k, other(k, gr[k])) for k in keys if other(k, gr[k])]
    R[name] = {'n': len(keys),
               'hh_with_any_match_own_key': int(sum(h > 0 for h, _ in own)),
               'hh_with_any_match_control': int(sum(h > 0 for h, _ in ctl)),
               'hh_with_2plus_match_own_key': int(sum(h >= 2 for h, _ in own)),
               'hh_with_2plus_match_control': int(sum(h >= 2 for h, _ in ctl)),
               'members_matched_own': int(sum(h for h, _ in own)), 'members_with_by': int(sum(n for _, n in own)),
               'members_matched_control': int(sum(h for h, _ in ctl)), 'members_with_by_control': int(sum(n for _, n in ctl))}


summarize(gone, 'linked_all_preloaded_left')
summarize(nopre, 'linked_no_preload')
# a reference: a random 300 ordinary linked households
ordinary = sorted(set(c21.loc[c21.grp == 'linked', 'key']) - set(gone) - set(nopre))[::23]
summarize(ordinary, 'linked_ordinary_sample')

# UEMOA's own files
for name in ('ehcvm_welfare_ben2021.dta', 'ehcvm_menage_ben2021.dta', 'ehcvm_individu_ben2021.dta'):
    try:
        df = dta('2021-22', name)
        dfl = dta('2021-22', name, labels=True)
        vl = vlabels('2021-22', name)
        r = {'columns': {c: vl.get(c, '') for c in df.columns[:60]}}
        if 'panel' in df.columns:
            hh = dfl.drop_duplicates(['grappe', 'menage']) if 'numind' in df.columns else dfl
            hh = hh.assign(key=[key(g, m) for g, m in zip(hh.grappe, hh.menage)])
            hh['grp'] = hh.key.map(gmap)
            r['panel_values'] = dict(Counter(hh['panel'].astype(str)))
            r['panel_by_group'] = {g: dict(Counter(hh.loc[hh.grp == g, 'panel'].astype(str))) for g in sorted(set(hh.grp.dropna()))}
            r['panel_of_the_63'] = dict(Counter(hh.loc[hh.key.isin(gone), 'panel'].astype(str)))
            r['panel_of_the_19'] = dict(Counter(hh.loc[hh.key.isin(nopre), 'panel'].astype(str)))
        if 'hhid' in df.columns:
            h = df.drop_duplicates(['grappe', 'menage'])
            hid = pd.to_numeric(h.hhid, errors='coerce')
            r['hhid_eq_grappe1000_plus_menage'] = float((hid == h.grappe * 1000 + h.menage).mean())
            r['hhid_eq_grappe100_plus_menage'] = float((hid == h.grappe * 100 + h.menage).mean())
            r['hhid_examples'] = h[['grappe', 'menage', 'hhid']].head(5).astype(str).to_dict('records')
        R[name] = r
    except Exception as e:  # noqa: BLE001
        R[name] = {'err': f'{type(e).__name__}: {e}'}
for name in ('ehcvm_welfare_ben2018.dta',):
    try:
        df = dta('2018-19', name)
        h = df.drop_duplicates(['grappe', 'menage'])
        hid = pd.to_numeric(h.hhid, errors='coerce')
        R[name] = {'hhid_eq_grappe1000_plus_menage': float((hid == h.grappe * 1000 + h.menage).mean()),
                   'hhid_examples': h[['grappe', 'menage', 'hhid']].head(5).astype(str).to_dict('records')}
    except Exception as e:  # noqa: BLE001
        R[name] = {'err': f'{type(e).__name__}: {e}'}

# nitrogen_kg / seed_kg on the pickled plot_inputs, base and candidate
for side in ('base', 'cand'):
    pi = pickle.load(open(main / side / 'plot_inputs.pkl', 'rb'))
    tr = {}
    for fn in ('nitrogen_kg', 'seed_kg'):
        try:
            out = getattr(T, fn)(pi)
            tr[fn] = {'rows': len(out), 'sums': {c: float(pd.to_numeric(out[c], errors='coerce').sum())
                                                 for c in out.columns if pd.api.types.is_numeric_dtype(out[c])}}
        except Exception as e:  # noqa: BLE001
            tr[fn] = {'err': f'{type(e).__name__}: {e}'[:300]}
    R[f'transforms_{side}'] = tr

# the relabelled row
s16 = dta('2018-19', 's16b_me_ben2018.dta')
row = s16[(s16.grappe == 166) & (s16.menage == 133) & (s16.s16bq02 == 1) & s16.s16bq03b.isna()]
R['relabelled_row'] = row[['s16bq01', 's16bq03a', 's16bq05', 's16bq07a', 's16bq08', 's16bq09a', 's16bq09b']].astype(str).to_dict('records')
json.dump(R, open(outdir / 'raw3.json', 'w'), indent=1, default=str, ensure_ascii=False)
print(json.dumps(R, indent=1, default=str, ensure_ascii=False))
print('DONE')
