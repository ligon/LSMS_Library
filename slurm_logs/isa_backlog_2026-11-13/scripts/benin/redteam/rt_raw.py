"""Red-team raw-file checks for Benin: the link rule, member-level evidence, BID 7.1,
and the 2018-19 s16b labels.  usage: rt_raw.py <clone> <outdir>"""
import json
import re
import shutil
import subprocess
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
clone, outdir = Path(sys.argv[1]), Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)
from lsms_library.local_tools import get_dataframe, format_id, df_from_orgfile  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

C = clone / 'lsms_library' / 'countries' / 'Benin'
R = {}


def dta(wave, name, labels=False):
    return get_dataframe(C / wave / 'Data' / name, convert_categoricals=labels)


def meta(wave, name):
    p = get_data_file(f'Benin/{wave}/Data/{name}')
    with pd.io.stata.StataReader(p) as rd:
        vl = rd.variable_labels()
        try:
            vals = rd.value_labels()
        except Exception:  # noqa: BLE001
            vals = {}
        lblname = {}
        try:
            lblname = dict(zip(rd._varlist, rd._lbllist))
        except Exception:  # noqa: BLE001
            pass
    return vl, vals, lblname


def key(g, m):
    return f'{int(g)}{int(m):03d}'


# ---------------------------------------------------------------- covers
c21 = dta('2021-22', 's00_me_ben2021.dta')
c21l = dta('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = dta('2018-19', 's00_me_ben2018.dta')
vl21, vals21, ln21 = meta('2021-22', 's00_me_ben2021.dta')
R['cover21_columns'] = {c: vl21.get(c, '') for c in c21.columns}
R['cover21_suspicious'] = {c: vl21.get(c, '') for c in c21.columns
                           if re.search(r'2018|2019|ehcvm.?1|ancien|pr[ée]c[ée]dent|panel|EHCVM1|_1$', f'{c} {vl21.get(c, "")}', re.I)}
c21['key'] = [key(g, m) for g, m in zip(c21.grappe, c21.menage)]
c18['key'] = [key(g, m) for g, m in zip(c18.grappe, c18.menage)]
assert all(key(g, m) == format_id(g) + format_id(m, zeropadding=3) for g, m in zip(c21.grappe, c21.menage))
assert all(key(g, m) == format_id(g) + format_id(m, zeropadding=3) for g, m in zip(c18.grappe, c18.menage))
R['menage_max'] = {'2018-19': int(c18.menage.max()), '2021-22': int(c21.menage.max())}
R['keys_unique'] = {'2018': bool(c18.key.is_unique), '2021': bool(c21.key.is_unique)}
panel = c21l['PanelHH'].astype(str).str.strip()
R['PanelHH_values'] = dict(Counter(panel))
R['PanelHH_codes'] = dict(Counter(c21['PanelHH'].astype(str)))
old = set(c18.key)
recurs = c21.key.isin(old)
grp = np.select([(panel == 'Panel') & recurs, (panel == 'Panel') & ~recurs, (panel != 'Panel') & recurs],
                ['linked', 'panel_nokey', 'nonpanel_recur'], 'new')
c21['grp'] = grp
R['groups'] = dict(Counter(grp))
xw = {'E2_' + k: k for k in c21.loc[c21.grp == 'linked', 'key']}
R['dup_targets'] = len(xw) - len(set(xw.values()))
with open(outdir / 'derived_crosswalk.json', 'w') as fh:
    json.dump({'2018-19': {}, '2021-22': dict(sorted(xw.items()))}, fh)
with open(C / '_' / 'updated_ids.json') as fh:
    committed = json.load(fh)
with open(C / '_' / 'panel_ids.json') as fh:
    committed_p = json.load(fh)
R['committed_equals_derived'] = committed == {'2018-19': {}, '2021-22': dict(sorted(xw.items()))}
R['panel_json_consistent'] = committed_p == {f'2021-22,{k}': f'2018-19,{v}' for k, v in committed['2021-22'].items()}
# s00q07d / s00q07e by group
for col in ('s00q07d', 's00q07e', 'vague'):
    if col in c21l.columns:
        R[f'{col}_by_group'] = {g: dict(Counter(c21l.loc[c21.grp == g, col].astype(str))) for g in sorted(set(grp))}
# vague preserved for linked
m = c21[c21.grp == 'linked'].merge(c18[['key', 'vague']], on='key', suffixes=('', '_18'))
R['linked_vague_equal'] = int((m.vague.astype(int) == m.vague_18.astype(int)).sum())
# menage distribution by group
R['menage_by_group'] = {g: {'min': int(c21.loc[c21.grp == g, 'menage'].min()), 'max': int(c21.loc[c21.grp == g, 'menage'].max()),
                            'le12': int((c21.loc[c21.grp == g, 'menage'] <= 12).sum()),
                            'n': int((c21.grp == g).sum())} for g in sorted(set(grp))}
mx18 = c18.groupby('grappe').menage.max()
c21['mx18'] = c21.grappe.map(mx18)
R['menage_vs_2018_grappe_max'] = {g: {'le_max18': int((c21.loc[c21.grp == g, 'menage'] <= c21.loc[c21.grp == g, 'mx18']).sum())}
                                  for g in sorted(set(grp))}
R['2018_menage_per_grappe'] = dict(Counter(c18.groupby('grappe').size()))
R['2021_hh_per_grappe'] = dict(Counter(c21.groupby('grappe').size()))
R['linked_per_grappe'] = dict(Counter(c21[c21.grp == 'linked'].groupby('grappe').size()))

# ---------------------------------------------------------------- rosters
s18 = dta('2018-19', 's01_me_ben2018.dta')
s21 = dta('2021-22', 's01_me_ben2021.dta')
v18, vals18, ln18 = meta('2018-19', 's01_me_ben2018.dta')
v21, vals21s, ln21s = meta('2021-22', 's01_me_ben2021.dta')
R['s01_2021_columns'] = {c: v21.get(c, '') for c in list(s21.columns)[:40]}
R['s01_2018_columns'] = {c: v18.get(c, '') for c in list(s18.columns)[:25]}
s18['key'] = [key(g, m) for g, m in zip(s18.grappe, s18.menage)]
s21['key'] = [key(g, m) for g, m in zip(s21.grappe, s21.menage)]
num18 = 's01q00a'
num21 = 'membres__id'
R['num_vars'] = {'2018': [num18, v18.get(num18)], '2021': [num21, v21.get(num21)], 's01q00aa': v21.get('s01q00aa'),
                 's01q00aa_values': dict(Counter(s21['s01q00aa'].astype(str)))}
R['num18_na'] = int(s18[num18].isna().sum())
R['num21_na'] = int(s21[num21].isna().sum())
s18 = s18[s18[num18].notna()].copy()
s18[num18] = pd.to_numeric(s18[num18]).astype(int)
s21 = s21[s21[num21].notna()].copy()
s21[num21] = pd.to_numeric(s21[num21]).astype(int)
s18i = s18.set_index(['key', num18])
assert s18i.index.is_unique, '2018 (key, member number) not unique'
gmap = dict(zip(c21.key, c21.grp))
s21['grp'] = s21.key.map(gmap)
pre = s21[s21['s01q00aa'].notna()].copy()
R['preloaded_members_by_group'] = dict(Counter(pre.grp))
R['preloaded_hh_by_group'] = {g: int(pre.loc[pre.grp == g, 'key'].nunique()) for g in sorted(set(grp))}
R['still_member_share_by_group'] = {g: float((pre.loc[pre.grp == g, 's01q00aa'] == 1).mean()) if (pre.grp == g).any() else None
                                    for g in sorted(set(grp))}
R['hh_all_preloaded_left_by_group'] = {g: int((pre[pre.grp == g].groupby('key')['s01q00aa'].apply(lambda s: (s != 1).all())).sum())
                                       for g in sorted(set(grp)) if (pre.grp == g).any()}


def bad_year(y):
    y = pd.to_numeric(y, errors='coerce')
    return y.where((y > 1880) & (y < 2023))


def member_compare(frame, key_col):
    """frame: 2021 member rows with columns key_col (2018 key to compare to), num21, s01q01, s01q03c, s01q04a, s01q00aa."""
    idx = list(zip(frame[key_col], frame[num21].astype(int)))
    found = np.array([k in s18i.index for k in idx])
    sub = frame[found]
    o = s18i.loc[[k for k, f in zip(idx, found) if f]]
    still = (sub['s01q00aa'] == 1).to_numpy()
    out = {'n': int(len(frame)), 'found': float(found.mean()) if len(frame) else None}
    if len(sub):
        sex = (sub['s01q01'].to_numpy() == o['s01q01'].to_numpy())
        y21, y18 = bad_year(sub['s01q03c']).to_numpy(), bad_year(o['s01q03c']).to_numpy()
        okby = ~np.isnan(y21) & ~np.isnan(y18)
        a21, a18 = pd.to_numeric(sub['s01q04a'], errors='coerce').to_numpy(), pd.to_numeric(o['s01q04a'], errors='coerce').to_numpy()
        oka = ~np.isnan(a21) & ~np.isnan(a18)
        d = a21 - a18
        out.update({
            'still_members_found': int(still.sum()),
            'sex_eq_still': float(sex[still].mean()) if still.any() else None,
            'sex_nonmissing_still': float((~pd.isna(sub['s01q01'].to_numpy()[still])).mean()) if still.any() else None,
            'birthyear_both_still': int((okby & still).sum()),
            'birthyear_eq_still': float((y21 == y18)[okby & still].mean()) if (okby & still).any() else None,
            'birthyear_within1_still': float((np.abs(y21 - y18) <= 1)[okby & still].mean()) if (okby & still).any() else None,
            'age_both_still': int((oka & still).sum()),
            'age_diff_2to4_still': float(((d >= 2) & (d <= 4))[oka & still].mean()) if (oka & still).any() else None,
            'age_diff_median_still': float(np.median(d[oka & still])) if (oka & still).any() else None,
            'sex_and_age2to4_still': float((sex & (d >= 2) & (d <= 4))[oka & still].mean()) if (oka & still).any() else None,
        })
    return out


pre['key18'] = pre['key']
R['member_same_key'] = {g: member_compare(pre[pre.grp == g], 'key18') for g in ('linked', 'nonpanel_recur', 'panel_nokey', 'new')}
# control: another 2018-19 household of the same grappe (next key, cyclic)
by_g = {g: sorted(v) for g, v in c18.groupby('grappe')['key']}


def other(k, g):
    ks = by_g.get(g, [])
    ks2 = [x for x in ks if x != k]
    if not ks2:
        return None
    return ks2[(sum(map(ord, k))) % len(ks2)]


ctl = pre[pre.grp.isin(['linked', 'nonpanel_recur'])].copy()
ctl['key18'] = [other(k, g) for k, g in zip(ctl.key, ctl.grappe)]
ctl = ctl[ctl.key18.notna()]
R['member_control'] = {g: member_compare(ctl[ctl.grp == g], 'key18') for g in ('linked', 'nonpanel_recur')}

# heads: a 2021 head (s01q02 == 1, still a member or new) vs the 2018 head of the compared key
h18 = s18[s18.s01q02 == 1].drop_duplicates('key').set_index('key')
h21 = s21[(s21.s01q02 == 1) & (s21['s01q00aa'] != 2)].drop_duplicates('key').set_index('key')
R['heads_unique'] = {'2018': int(s18[s18.s01q02 == 1].key.duplicated().sum()), '2021': int(s21[(s21.s01q02 == 1) & (s21['s01q00aa'] != 2)].key.duplicated().sum())}


def head_cmp(pairs):
    pairs = [(a, b) for a, b in pairs if a in h21.index and b in h18.index]
    if not pairs:
        return {'n': 0}
    A = h21.loc[[a for a, _ in pairs]]
    B = h18.loc[[b for _, b in pairs]]
    sex = A.s01q01.to_numpy() == B.s01q01.to_numpy()
    y21, y18 = bad_year(A.s01q03c).to_numpy(), bad_year(B.s01q03c).to_numpy()
    ok = ~np.isnan(y21) & ~np.isnan(y18)
    same_pid = A[num21].astype(int).to_numpy() == B[num18].astype(int).to_numpy()
    return {'n': len(pairs), 'sex_eq': float(sex.mean()), 'birthyear_eq': float((y21 == y18)[ok].mean()) if ok.any() else None,
            'birthyear_within1': float((np.abs(y21 - y18) <= 1)[ok].mean()) if ok.any() else None,
            'same_member_number': float(same_pid.mean()),
            'sex_and_by_within1': float((sex & (np.abs(y21 - y18) <= 1))[ok].mean()) if ok.any() else None}


R['heads'] = {}
for g in ('linked', 'nonpanel_recur'):
    ks = list(c21.loc[c21.grp == g, 'key'])
    R['heads'][g] = head_cmp([(k, k) for k in ks])
    gg = dict(zip(c21.key, c21.grappe))
    R['heads'][g + '_control'] = head_cmp([(k, other(k, gg[k])) for k in ks if other(k, gg[k])])

# 2021 roster vars that look pre-filled
R['s01_2021_labels_preload'] = {c: v21.get(c) for c in s21.columns if re.search(r'pr[ée].?rempl|preload|2018|toujours', str(v21.get(c, '')), re.I)}

# ---------------------------------------------------------------- BID 7.1
try:
    pdf = get_data_file('Benin/2021-22/Documentation/ben_ehcvm2_information_de_base_eng.pdf')
    txt_path = outdir / 'bid.txt'
    if shutil.which('pdftotext'):
        subprocess.run(['pdftotext', '-layout', str(pdf), str(txt_path)], check=True)
        txt = txt_path.read_text(errors='replace')
    else:
        try:
            from pypdf import PdfReader
        except ImportError:
            from PyPDF2 import PdfReader  # type: ignore
        txt = '\n'.join((p.extract_text() or '') for p in PdfReader(str(pdf)).pages)
        txt_path.write_text(txt)
    lines = txt.splitlines()
    hits = [i for i, l in enumerate(lines) if 'PanelHH' in l or 'panel survey by cluster' in l.lower()]
    R['bid_hits'] = ['\n'.join(lines[max(0, i - 6): i + 6]) for i in hits][:8]
except Exception as e:  # noqa: BLE001
    R['bid_err'] = f'{type(e).__name__}: {e}'

# ---------------------------------------------------------------- s16b 2018-19
s16 = dta('2018-19', 's16b_me_ben2018.dta')
s16l = dta('2018-19', 's16b_me_ben2018.dta', labels=True)
vb, valsb, lnb = meta('2018-19', 's16b_me_ben2018.dta')
R['s16b_labels'] = {c: vb.get(c) for c in s16.columns if c.startswith('s16bq')}
R['s16b_value_label_names'] = {c: lnb.get(c) for c in ('s16bq03b', 's16bq07b', 's16bq09b', 's16bq05', 's16bq08')}
for c in ('s16bq03b', 's16bq07b', 's16bq09b'):
    nm = lnb.get(c)
    R[f'{c}_value_labels'] = {str(k): v for k, v in (valsb.get(nm, {}) or {}).items()} if nm else None
ap = s16[s16.s16bq02 == 1]
apl = s16l[(s16.s16bq02 == 1).to_numpy()]
R['applied'] = int(len(ap))
R['crosstab_gift_x_purchase'] = {f'{a}|{b}': int(n) for (a, b), n in Counter(zip(ap.s16bq05.astype(str), ap.s16bq08.astype(str))).items()}
for c in ('s16bq03a', 's16bq07a', 's16bq09a', 's16bq09c'):
    x = pd.to_numeric(ap[c], errors='coerce')
    R[f'{c}_stats'] = {'n': int(x.notna().sum()), 'sum': float(x.sum()), 'max': float(x.max()) if x.notna().any() else None,
                       'ge9999': int((x >= 9999).sum()), 'le0': int((x <= 0).sum())}
R['purchase_consistency'] = {
    'q08_yes': int((ap.s16bq08 == 1).sum()),
    'q09a_notna': int(ap.s16bq09a.notna().sum()),
    'q09a_notna_and_q08_yes': int((ap.s16bq09a.notna() & (ap.s16bq08 == 1)).sum()),
    'q09b_notna': int(ap.s16bq09b.notna().sum()),
    'q09a_notna_q09b_na': int((ap.s16bq09a.notna() & ap.s16bq09b.isna()).sum()),
    'q05_yes': int((ap.s16bq05 == 1).sum()),
    'q07a_notna': int(ap.s16bq07a.notna().sum()),
    'q07b_notna': int(ap.s16bq07b.notna().sum()),
    'q03b_na': int(ap.s16bq03b.isna().sum()),
}
# unit decoding vs the u table
uorg = df_from_orgfile(C / '_' / 'categorical_mapping.org', name='u')
R['u_table_columns'] = list(map(str, uorg.columns))
orig = set((uorg['Original Label'] if 'Original Label' in uorg.columns else uorg.iloc[:, 0]).astype(str))
pref = set(uorg['Preferred Label'].astype(str)) if 'Preferred Label' in uorg.columns else set()
for c in ('s16bq03b', 's16bq07b', 's16bq09b'):
    labs = Counter(apl[c].dropna().astype(str))
    R[f'{c}_decoded'] = {lab: {'n': n, 'in_u_original': lab in orig, 'in_u_preferred': lab in pref} for lab, n in labs.items()}
blank = ap[ap.s16bq03b.isna()]
R['blank_unit_rows'] = blank[['grappe', 'menage', 's16bq01', 's16bq03a']].astype(str).to_dict('records')

with open(outdir / 'raw.json', 'w') as fh:
    json.dump(R, fh, indent=1, default=str, ensure_ascii=False)
print(json.dumps({k: R[k] for k in ('groups', 'dup_targets', 'committed_equals_derived', 'panel_json_consistent',
                                    'member_same_key', 'member_control', 'heads') if k in R}, indent=1, default=str))
print('DONE')
