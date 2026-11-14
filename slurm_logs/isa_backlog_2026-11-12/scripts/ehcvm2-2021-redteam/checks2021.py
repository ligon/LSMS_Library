"""Independent checks of the 2021-22 wiring against the raw files (read via the library).

usage: checks2021.py CLONE FRAMES_HEAD_DIR OUT.json
Reads the head frames built by job1 (all waves, cold), and raw files via get_dataframe.
"""
import json, os, sys, traceback, warnings, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env
clone, frames, out = sys.argv[1:4]
assert_env(clone)
warnings.simplefilter('ignore')
import numpy as np
import pandas as pd
from lsms_library.local_tools import get_dataframe, get_formatting_functions
from lsms_library.paths import countries_root
from lsms_library.data_access import get_data_file

W = '2021-22'
SUF = {'Togo': 'tgo2021', 'Benin': 'ben2021', 'CotedIvoire': 'civ2021'}
OLD = {'Togo': ('2018', 'Data1/s00_me_tgo2018.dta'),
       'Benin': ('2018-19', 'Data/s00_me_ben2018.dta'),
       'CotedIvoire': ('2018-19', 'Data/Menage/s00_me_CIV2018.dta')}
CR = countries_root()
res = {}
import subprocess
_here = os.path.dirname(os.path.abspath(__file__))
_rr = os.path.dirname(_here)
for jb, cs in [('job1', 'Togo,Benin,CotedIvoire'), ('job2', 'Senegal,Mali,Guinea-Bissau,Niger,Burkina_Faso')]:
    rc = subprocess.call([sys.executable, os.path.join(_here, 'compare.py'), os.path.join(_rr, 'frames', jb), cs,
                          os.path.join(_rr, 'results', f'{jb}_compare_v2.json')])
    print('compare v2', jb, 'rc', rc, flush=True)


def raw(C, f, wave=W, cat=True):
    return get_dataframe(CR / C / wave / 'Data' / f, convert_categoricals=cat)


def frame(C, t):
    p = os.path.join(frames, C, f'{t}.parquet')
    df = pd.read_parquet(p)
    return df


def w21(df):
    return df[df.index.get_level_values('t').astype(str) == W]


def labels(C, f):
    p = get_data_file(f'{C}/{W}/Data/{f}')
    with pd.io.stata.StataReader(str(p)) as rd:
        vl = rd.variable_labels()
        try:
            vals = rd.value_labels()
        except Exception:
            vals = {}
        lbl = {}
        try:
            lbl = dict(zip(rd.varlist, rd.lbllist))
        except Exception:
            pass
    return vl, vals, lbl


def ids(C, df):
    fns = get_formatting_functions(CR / C / W / '_' / 'mapping.py', name=f'_rt_{C}_map')
    f = fns['i']
    return [f(pd.Series([g, m])) for g, m in zip(df['grappe'], df['menage'])]


def safe(name, fn, C):
    try:
        res.setdefault(C, {})[name] = fn(C)
    except Exception as e:
        res.setdefault(C, {})[name] = {'ERROR': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}
    print(C, name, json.dumps(res[C][name], default=str)[:1500], flush=True)


# ---------------------------------------------------------------- index uniqueness
def uniq(C):
    out = {}
    for f in sorted(os.listdir(os.path.join(frames, C))):
        if not f.endswith('.parquet'):
            continue
        df = pd.read_parquet(os.path.join(frames, C, f))
        t = df.index.get_level_values('t').astype(str) if 't' in df.index.names else None
        out[f[:-8]] = {'rows': len(df), 'dup_index': int(df.index.duplicated().sum()),
                       'rows_2021': int((t == W).sum()) if t is not None else None,
                       'dup_2021': int(df[t == W].index.duplicated().sum()) if t is not None else None,
                       'null_index_2021': ({n: int(pd.isna(df[t == W].index.get_level_values(n)).sum())
                                            for n in df.index.names} if t is not None else None)}
    return out


# ---------------------------------------------------------------- sample
def sample(C):
    s = w21(frame(C, 'sample')).droplevel('t')
    wts = raw(C, f'ehcvm_ponderations_{SUF[C]}.dta')
    cov = raw(C, f's00_me_{SUF[C]}.dta')
    wts = wts.assign(i=ids(C, wts))
    cov = cov.assign(i=ids(C, cov))
    o = {'served': len(s), 'weights_rows': len(wts), 'cover_rows': len(cov),
         'weights_ids_unique': bool(wts['i'].is_unique), 'cover_ids_unique': bool(cov['i'].is_unique),
         'served_eq_weights_set': bool(set(s.index) == set(wts['i'])),
         'served_minus_weights': len(set(s.index) - set(wts['i'])),
         'weights_minus_served': len(set(wts['i']) - set(s.index))}
    wt = wts.set_index('i')['poids'].astype(float)
    wt = wt.reindex(s.index)
    norm = wt / wt.mean()
    o['weight_maxabs_vs_poids_over_mean'] = float((s['weight'] - norm).abs().max())
    o['panel_weight_eq_weight'] = bool(np.allclose(s['panel_weight'], s['weight'], equal_nan=True))
    o['weight_mean'] = float(s['weight'].mean())
    o['weight_null'] = int(s['weight'].isna().sum())
    cv = cov.set_index('i')
    o['v_eq_grappe'] = int((s['v'].astype(str) == cv['grappe'].reindex(s.index).map(lambda x: str(int(x)))).sum()) if 'v' in s else 'v not in columns'
    rural_map = {'Rural': 'Rural', 'Urbain': 'Urban'}
    exp_rural = cv['s00q04'].reindex(s.index).astype(str).str.strip().map(rural_map)
    o['rural_eq'] = int((s['Rural'].astype(str) == exp_rural.astype(str)).sum())
    o['strata_nunique'] = int(s['strata'].nunique()); o['strata_na'] = int(s['strata'].isna().sum())
    o['strata_per_grappe_max'] = int(s.groupby('v')['strata'].nunique().max()) if 'v' in s else None
    o['derivation_counts'] = s['Derivation'].astype(str).value_counts().to_dict() if 'Derivation' in s else None
    o['columns'] = list(s.columns)
    return o


# ---------------------------------------------------------------- region codes vs labels
def regions(C):
    old_w, old_f = OLD[C]
    new_c = raw(C, f's00_me_{SUF[C]}.dta', cat=False); new_l = raw(C, f's00_me_{SUF[C]}.dta', cat=True)
    old_c = get_dataframe(CR / C / old_w / old_f, convert_categoricals=False)
    old_l = get_dataframe(CR / C / old_w / old_f, convert_categoricals=True)
    def cmap(c, l):
        d = pd.DataFrame({'code': pd.to_numeric(c['s00q01'], errors='coerce'), 'label': l['s00q01'].astype(str).str.strip(),
                          'grappe': c['grappe']})
        g = d.groupby('code').agg(labels=('label', lambda x: sorted(set(x))), grappes=('grappe', 'nunique'), hh=('grappe', 'size'))
        return g, d
    gn, dn = cmap(new_c, new_l); go, do = cmap(old_c, old_l)
    rows = []
    for code in sorted(set(gn.index) | set(go.index)):
        ln = gn['labels'].get(code); lo = go['labels'].get(code)
        rows.append({'code': code, 'old': lo, 'new': ln,
                     'exact': ln == lo, 'upper_eq': (ln is not None and lo is not None and [x.upper() for x in ln] == lo),
                     'casefold_eq': (ln is not None and lo is not None and [x.casefold() for x in ln] == [x.casefold() for x in lo]),
                     'grappes_old': int(go['grappes'].get(code, 0)), 'grappes_new': int(gn['grappes'].get(code, 0))})
    # grappe -> region code agreement across waves
    gm_new = dn.groupby('grappe')['code'].agg(lambda x: sorted(set(x)))
    gm_old = do.groupby('grappe')['code'].agg(lambda x: sorted(set(x)))
    common = gm_new.index.intersection(gm_old.index)
    agree = int(sum(gm_new[g] == gm_old[g] for g in common))
    # milieu
    mil_new = sorted(set(new_l['s00q04'].astype(str).str.strip())); mil_old = sorted(set(old_l['s00q04'].astype(str).str.strip()))
    return {'codes': rows, 'n_codes_new': len(gn), 'n_codes_old': len(go),
            'n_exact': sum(r['exact'] for r in rows), 'n_upper_eq': sum(r['upper_eq'] for r in rows),
            'n_casefold_eq': sum(r['casefold_eq'] for r in rows),
            'grappes_common': len(common), 'grappes_region_code_agree': agree,
            'milieu_labels_new': mil_new, 'milieu_labels_old': mil_old}


# ---------------------------------------------------------------- shocks
def shock_labels(C):
    f = f's14b_me_{SUF[C]}.dta'
    vl, vals, lbl = labels(C, f)
    sets = {k: lbl.get(k) for k in ['s14bq04d', 's14bq04e', 's14bq04f', 's14bq05__1', 's14bq05__25']}
    return {'varlabels': {k: vl.get(k) for k in vl if k.startswith('s14bq0')},
            'value_label_sets': sets,
            'value_labels': {v: vals.get(v) for v in set(x for x in sets.values() if x)}}


def shock_fallback(C):
    f = f's14b_me_{SUF[C]}.dta'
    r = raw(C, f)
    r = r[r['grappe'].notna() & r['menage'].notna()].copy()
    m = {'Diminué': True, 'Augmenté': True, 'Inchangé': False, 'Non-concerné': False}
    s = w21(frame(C, 'shocks'))
    o = {}
    for col, src in [('AffectedConsumption', 's14bq04f'), ('AffectedFoodStock', 's14bq04e'), ('_herd', 's14bq04d')]:
        x = r[src].astype(str).str.strip().map(m)
        o[src] = {'raw_true': int((x == True).sum()), 'raw_false': int((x == False).sum()), 'raw_nonnull': int(r[src].notna().sum())}
        if col in s:
            o[src].update(served_true=int(s[col].astype('boolean').sum()), served_false=int((s[col].astype('boolean') == False).sum()))
    return o


def shocks(C):
    f = f's14b_me_{SUF[C]}.dta'
    vl, vals, lbl = {}, {}, {}
    o = {'varlabels': {k: vl.get(k) for k in ['s14bq01', 's14bq02', 's14bq04a', 's14bq04b', 's14bq04c', 's14bq04d',
                                               's14bq04e', 's14bq04f', 's14bq04g', 's14bq05__25'] if k in vl},
         'value_label_sets': {k: lbl.get(k) for k in ['s14bq04d', 's14bq04e', 's14bq04f', 's14bq05__1', 's14bq05__25']}}
    r = raw(C, f)
    vag = 'vague' if 'vague' in r.columns else None
    nn = {}
    for col in ['s14bq04d', 's14bq04e', 's14bq04f', 's14bq04g']:
        if col in r.columns:
            nn[col] = (r.groupby(vag)[col].apply(lambda x: int(x.notna().sum())).to_dict() if vag else int(r[col].notna().sum()))
    o['nonnull_by_vague'] = nn
    o['value_counts'] = {c: r[c].astype(str).value_counts().to_dict() for c in ['s14bq04d', 's14bq04e', 's14bq04f'] if c in r}
    s = w21(frame(C, 'shocks')).droplevel('t').reset_index()
    r = r[r['grappe'].notna() & r['menage'].notna()].copy()
    r['i'] = ids(C, r)
    r['Shock'] = r['s14bq01'].astype(str).str.strip()
    s['Shock'] = s['Shock'].astype(str).str.strip()
    m = {'Diminué': True, 'Augmenté': True, 'Inchangé': False, 'Non-concerné': False}
    for col, src in [('AffectedConsumption', 's14bq04f'), ('AffectedFoodStock', 's14bq04e'), ('AffectedIncome', 's14bq04a')]:
        rr = r[['i', 'Shock', src]].copy()
        rr['exp'] = rr[src].astype(str).str.strip().map(m)
        rr = rr[rr[src].notna()]
        rr = rr.drop_duplicates(['i', 'Shock'], keep=False)
        j = s[['i', 'Shock', col]].merge(rr[['i', 'Shock', 'exp']], on=['i', 'Shock'], how='inner')
        a = j[col].astype('boolean'); b = j['exp'].astype('boolean')
        o[f'{col}_vs_{src}'] = {'joined': len(j), 'agree': int((a == b).fillna(False).sum()),
                                'both_na': int((a.isna() & b.isna()).sum()),
                                'disagree': int(((a != b).fillna(True) & ~(a.isna() & b.isna())).sum()),
                                'served_true_share': float(a.mean())}
        # and how often it would agree with the herd column d
        rd = r[['i', 'Shock', 's14bq04d']].copy(); rd['d'] = rd['s14bq04d'].astype(str).str.strip().map(m)
        rd = rd.drop_duplicates(['i', 'Shock'], keep=False)
        jd = s[['i', 'Shock', col]].merge(rd[['i', 'Shock', 'd']], on=['i', 'Shock'])
        o[f'{col}_agree_with_d'] = int((jd[col].astype('boolean') == jd['d'].astype('boolean')).fillna(False).sum())
    o['served_rows'] = len(s)
    o['raw_rows_with_followup_f'] = int(r['s14bq04f'].notna().sum())
    return o


# ---------------------------------------------------------------- roster
def roster(C):
    h = w21(frame(C, 'household_roster'))
    s01 = raw(C, f's01_me_{SUF[C]}.dta')
    o = {'served': len(h), 's01_rows': len(s01), 'columns': list(h.columns), 'index': list(h.index.names)}
    if 's01q00aa' in s01:
        o['s01q00aa'] = s01['s01q00aa'].astype(str).value_counts().to_dict()
    o['s01q01_na'] = int(s01['s01q01'].isna().sum())
    ind = raw(C, f'ehcvm_individu_{SUF[C]}.dta')
    o['ehcvm_individu_rows'] = len(ind)
    for c in ['Sex', 'MonthsSpent', 'Generation', 'Distance', 'Affinity']:
        if c in h:
            o[f'{c}_counts'] = h[c].astype(str).value_counts(dropna=False).head(12).to_dict()
    if 'Age' in h:
        o['Age_na'] = int(h['Age'].isna().sum()); o['Age_max'] = float(h['Age'].max()); o['Age_min'] = float(h['Age'].min())
    # every served member is on s01 with present status; none of the departed
    s01 = s01.assign(i=ids(C, s01))
    s01['pid'] = s01['membres__id'].map(lambda x: str(int(x)) if pd.notna(x) else None)
    key = set(zip(s01['i'], s01['pid']))
    hi = h.reset_index()
    served = set(zip(hi['i'].astype(str), hi['pid'].astype(str)))
    o['served_not_in_s01'] = len(served - key)
    if 's01q00aa' in s01:
        gone = s01[s01['s01q00aa'].astype(str).str.strip().str.lower().isin(['non'])]
        o['departed_in_s01'] = len(gone)
        o['departed_served'] = len(served & set(zip(gone['i'], gone['pid'])))
    # s01 rows with all of sex/relationship/age blank (ghost / departed) served?
    blank = s01[s01['s01q01'].isna() & s01['s01q02'].isna()]
    o['blank_s01_rows'] = len(blank)
    o['blank_s01_served'] = len(served & set(zip(blank['i'], blank['pid'])))
    return o


# ---------------------------------------------------------------- food
SIZE_RE = re.compile(r'\(([^()]*)\)\s*$')


def food(C):
    f = w21(frame(C, 'food_acquired'))
    u = f.index.get_level_values('u').astype(str)
    sizes = pd.Series([m.group(1) if (m := SIZE_RE.search(x)) else None for x in u])
    o = {'rows': len(f), 'index': list(f.index.names), 'columns': list(f.columns),
         'u_distinct': int(u.nunique()),
         'size_suffix_counts': sizes.value_counts().to_dict(),
         'tres_grand_rows': int(u.str.contains('Très grand', regex=False).sum()),
         'u_numeric_like': sorted(set(x for x in u if re.fullmatch(r'\d+(\.\d+)?', x)))[:30],
         'j_numeric_like': sorted(set(x for x in f.index.get_level_values('j').astype(str) if re.fullmatch(r'\d+(\.\d+)?', x)))[:30],
         'Derivation': f['Derivation'].astype(str).value_counts().to_dict() if 'Derivation' in f else None,
         's_counts': f.index.get_level_values('s').astype(str).value_counts().to_dict() if 's' in f.index.names else None}
    for c in ('Quantity', 'Expenditure'):
        if c in f:
            o[f'{c}_sum'] = float(f[c].sum()); o[f'{c}_na'] = int(f[c].isna().sum())
            o[f'{c}_neg'] = int((f[c] < 0).sum())
    return o


def crop(C):
    f = w21(frame(C, 'crop_production'))
    o = {'rows': len(f), 'index': list(f.index.names), 'columns': list(f.columns)}
    if 'Derivation' in f:
        o['Derivation'] = f['Derivation'].astype(str).value_counts().to_dict()
    o['plot_farm_level'] = int((f.index.get_level_values('plot_id').astype(str) == 'Farm-level').sum())
    o['Quantity_notna'] = int(f['Quantity'].notna().sum()) if 'Quantity' in f else None
    o['u_top'] = f.index.get_level_values('u').astype(str).value_counts().head(15).to_dict()
    o['crop_numeric_like'] = sorted(set(x for x in f.index.get_level_values('crop').astype(str) if re.fullmatch(r'\d+(\.\d+)?', x)))
    o['condition'] = f.index.get_level_values('condition').astype(str).value_counts().to_dict()
    return o


def strata_waves(C):
    sm = frame(C, 'sample')
    cf = frame(C, 'cluster_features')
    t = sm.index.get_level_values('t').astype(str)
    o = {'strata_by_wave': {w: sm[t == w]['strata'].astype(str).value_counts(dropna=False).to_dict() for w in sorted(set(t))}}
    tc = cf.index.get_level_values('t').astype(str)
    o['region_by_wave'] = {w: sorted(cf[tc == w]['Region'].astype(str).unique()) for w in sorted(set(tc))}
    o['rural_by_wave'] = {w: cf[tc == w]['Rural'].astype(str).value_counts().to_dict() for w in sorted(set(tc))}
    return o


def raw_sizes(C):
    r = raw(C, f's07b_me_{SUF[C]}.dta')
    q = pd.to_numeric(r['s07bq03a'], errors='coerce')
    sz = r.loc[q > 0, 's07bq03c'].astype(str).str.strip()
    return {'s07bq03c_on_consumed': sz.value_counts(dropna=False).to_dict(), 'consumed_rows': int((q > 0).sum())}


def varlabels(C):
    import yaml
    di = yaml.safe_load(open(CR / C / W / '_' / 'data_info.yml'))
    want = {}
    def names(v):
        if isinstance(v, str):
            return [v]
        if isinstance(v, list):
            return [x for x in v if isinstance(x, str)]
        return []
    def walk(block, table):
        if isinstance(block, dict):
            if 'file' in block:
                vs = []
                for k in ('idxvars', 'myvars'):
                    for kk, vv in (block.get(k) or {}).items():
                        vs += [(kk, n) for n in names(vv)]
                want.setdefault(block['file'], []).extend((table, kk, n) for kk, n in vs)
            for k, v in block.items():
                if isinstance(v, dict):
                    walk(v, table)
    for table, block in di.items():
        walk(block, table)
    out = {}
    for f, items in want.items():
        try:
            vl, _, _ = labels(C, f)
            out[f] = {f'{t}.{kk}': (n, vl.get(n, 'NOT IN FILE')) for t, kk, n in items}
        except Exception as e:
            out[f] = f'ERR {type(e).__name__}: {e}'[:300]
    return out


for C in ['Togo', 'Benin', 'CotedIvoire']:
    for name, fn in [('strata_waves', strata_waves), ('shock_labels', shock_labels), ('shock_fallback', shock_fallback),
                     ('raw_sizes', raw_sizes), ('varlabels', varlabels)]:
        safe(name, fn, C)
    json.dump(res, open(out, 'w'), indent=1, default=str)

for C in ['Togo', 'Benin', 'CotedIvoire']:
    for name, fn in [('uniq', uniq), ('sample', sample), ('regions', regions), ('shocks', shocks),
                     ('roster', roster), ('food', food), ('crop', crop)]:
        safe(name, fn, C)
    json.dump(res, open(out, 'w'), indent=1, default=str)

# derivation inputs join (head): via Country API, warm/cold in this private root
import lsms_library as ll
for C in ['Togo', 'Benin', 'CotedIvoire']:
    def deriv(C):
        from lsms_library.derivations import derivation_records
        recs = derivation_records(C)
        out = {}
        for key, rec in recs.items():
            out[key] = {'waves': [str(w) for w in (rec.waves or [])], 'table': key.split('::')[1]}
        return out
    safe('registry', deriv, C)
json.dump(res, open(out, 'w'), indent=1, default=str)
print('CHECKS DONE')
