"""Red-team follow-up: BID 7.1 text, and roster matching of the households the rule does
not link (do the 96 new keys / 11 Panel-no-key / 116 have a findable 2018-19 origin?),
plus a validation of the scoring on the linked.  usage: rt_raw2.py <clone> <outdir>"""
import json
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
clone, outdir = Path(sys.argv[1]), Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

C = clone / 'lsms_library' / 'countries' / 'Benin'
R = {}

# ---------------------------------------------------------------- BID
pdf = get_data_file('Benin/2021-22/Documentation/ben_ehcvm2_information_de_base_eng.pdf')
txt = None
for how in ('pdfminer', 'fitz', 'pypdf'):
    try:
        if how == 'pdfminer':
            from pdfminer.high_level import extract_text
            txt = extract_text(str(pdf))
        elif how == 'fitz':
            import fitz
            txt = '\n'.join(p.get_text() for p in fitz.open(str(pdf)))
        else:
            from pypdf import PdfReader
            txt = '\n'.join((p.extract_text() or '') for p in PdfReader(str(pdf)).pages)
        R['bid_reader'] = how
        break
    except Exception as e:  # noqa: BLE001
        R.setdefault('bid_reader_errors', []).append(f'{how}: {type(e).__name__}: {e}')
if txt:
    (outdir / 'bid.txt').write_text(txt)
    lines = txt.splitlines()
    hits = [i for i, l in enumerate(lines) if 'PanelHH' in l or 'panel survey by cluster' in l.lower()
            or 'valid values for merging' in l.lower()]
    R['bid_hits'] = ['\n'.join(lines[max(0, i - 8): i + 8]) for i in hits][:10]


def key(g, m):
    return f'{int(g)}{int(m):03d}'


def dta(wave, name, labels=False):
    return get_dataframe(C / wave / 'Data' / name, convert_categoricals=labels)


c21 = dta('2021-22', 's00_me_ben2021.dta')
c21l = dta('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = dta('2018-19', 's00_me_ben2018.dta')
c21['key'] = [key(g, m) for g, m in zip(c21.grappe, c21.menage)]
c18['key'] = [key(g, m) for g, m in zip(c18.grappe, c18.menage)]
panel = c21l['PanelHH'].astype(str).str.strip()
old = set(c18.key)
rec = c21.key.isin(old)
c21['grp'] = np.select([(panel == 'Panel') & rec, (panel == 'Panel') & ~rec, (panel != 'Panel') & rec],
                       ['linked', 'panel_nokey', 'nonpanel_recur'], 'new')
gmap = dict(zip(c21.key, c21.grp))
linked_targets = set(c21.loc[c21.grp == 'linked', 'key'])
present21 = set(c21.key)

s18 = dta('2018-19', 's01_me_ben2018.dta')
s21 = dta('2021-22', 's01_me_ben2021.dta')
s21l = dta('2021-22', 's01_me_ben2021.dta', labels=True)
s18['key'] = [key(g, m) for g, m in zip(s18.grappe, s18.menage)]
s21['key'] = [key(g, m) for g, m in zip(s21.grappe, s21.menage)]
s21['grp'] = s21.key.map(gmap)
s18['num'] = s18['s01q00a'].astype(int)
s21['num'] = s21['membres__id'].astype(int)
by = lambda s: pd.to_numeric(s, errors='coerce').where(lambda x: (x > 1880) & (x < 2023))  # noqa: E731
s18['by'] = by(s18.s01q03c)
s21['by'] = by(s21.s01q03c)
pre = s21[s21.s01q00aa.notna()].copy()
R18 = {k: g.set_index('num')[['s01q01', 'by']] for k, g in s18.groupby('key')}
g18 = {g: sorted(v) for g, v in c18.groupby('grappe')['key']}


def score(members, k18):
    r = R18.get(k18)
    if r is None:
        return 0
    s = 0
    for n, sx, b in zip(members.num, members.s01q01, members.by):
        if n in r.index:
            rs, rb = r.at[n, 's01q01'], r.at[n, 'by']
            if rs == sx and (pd.isna(b) or pd.isna(rb) or abs(b - rb) <= 1):
                s += 1
    return s


def best(hkey, grappe):
    m = pre[pre.key == hkey]
    sc = sorted(((score(m, k), k) for k in g18.get(grappe, [])), reverse=True)
    if not sc:
        return None
    top = sc[0]
    second = sc[1][0] if len(sc) > 1 else 0
    return {'n_pre': len(m), 'best': top[1], 'best_score': top[0], 'second_score': second,
            'own_key_score': score(m, hkey) if hkey in R18 else None,
            'best_is_own': top[1] == hkey, 'unique': top[0] > second,
            'best_linked_elsewhere': top[1] in linked_targets and top[1] != hkey,
            'best_present_2021': top[1] in present21}


gr = dict(zip(c21.key, c21.grappe))
out = {}
for g in ('new', 'panel_nokey', 'nonpanel_recur'):
    keys = sorted(set(pre.loc[pre.grp == g, 'key']))
    rows = [dict(key=k, **(best(k, gr[k]) or {})) for k in keys]
    out[g] = rows
    df = pd.DataFrame(rows)
    if len(df):
        strong = df[(df.best_score >= 2) & df.unique & (df.best_score >= 0.5 * df.n_pre)]
        R[f'match_{g}'] = {
            'hh_with_preload': len(df), 'strong_unique_match': len(strong),
            'strong_best_is_own_key': int(strong.best_is_own.sum()),
            'strong_best_linked_to_another_2021_hh': int(strong.best_linked_elsewhere.sum()),
            'strong_best_has_a_2021_record_under_its_key': int(strong.best_present_2021.sum()),
            'score_hist': dict(Counter(df.best_score.astype(int))),
        }
# validation on a sample of linked households (every 10th)
lk = sorted(set(pre.loc[pre.grp == 'linked', 'key']))[::10]
vr = pd.DataFrame([dict(key=k, **(best(k, gr[k]) or {})) for k in lk])
R['validate_linked_sample'] = {'n': len(vr), 'best_is_own': int(vr.best_is_own.sum()),
                               'best_is_own_and_unique': int((vr.best_is_own & vr.unique).sum()),
                               'own_score_zero': int((vr.own_key_score == 0).sum())}
# the linked households with no continuing pre-loaded member
st = pre[pre.grp == 'linked'].groupby('key')['s01q00aa'].apply(lambda s: (s != 1).all())
gone = sorted(st[st].index)
R['linked_all_left'] = {'n': len(gone),
                        's00q07e': dict(Counter(c21l.set_index(c21.key).loc[gone, 's00q07e'].astype(str))),
                        'current_members': int(s21[(s21.key.isin(gone)) & (s21.s01q00aa != 2)].shape[0]),
                        's01q00d_of_current': dict(Counter(s21l.loc[(s21.key.isin(gone)).to_numpy() & (s21.s01q00aa != 2).to_numpy(), 's01q00d'].astype(str)))}
R['linked_no_preload'] = int(len(set(c21.loc[c21.grp == 'linked', 'key']) - set(pre.key)))
R['s01q00d_values'] = dict(Counter(s21l['s01q00d'].astype(str)))
R['s01q00f_values'] = dict(Counter(s21l['s01q00f'].astype(str)).most_common(12))
R['attrited_2018'] = len(old - linked_targets)
with open(outdir / 'raw2.json', 'w') as fh:
    json.dump(R, fh, indent=1, default=str, ensure_ascii=False)
with open(outdir / 'raw2_matches.json', 'w') as fh:
    json.dump(out, fh, indent=1, default=str)
print(json.dumps({k: v for k, v in R.items() if k != 'bid_hits'}, indent=1, default=str, ensure_ascii=False))
print('\n=====BID\n', '\n----\n'.join(R.get('bid_hits') or []))
print('DONE')
