"""Red-team raw census for the burkina stream (counts only, no frames written).

cwd = a clone, PYTHONPATH = that clone, LSMS_DATA_DIR private.  Usage: raw_census.py OUT.json
Every section is independent; an error is recorded and the next section runs.
"""
import json
import sys
import traceback
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library import data_access
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

R = countries_root()
OUT = {}


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


def save():
    with open(sys.argv[1], 'w') as fh:
        json.dump(_sk(OUT), fh, indent=1, default=str, sort_keys=True)


def section(name):
    def deco(fn):
        try:
            OUT[name] = fn()
        except Exception:  # noqa: BLE001
            OUT[name] = 'ERR ' + traceback.format_exc()[-2500:]
        save()
        print('done', name, flush=True)
        return fn
    return deco


_F = {}


def frames(rel):
    if rel not in _F:
        p = str(R / rel)
        _F[rel] = (get_dataframe(p, convert_categoricals=True),
                   get_dataframe(p, convert_categoricals=False))
    return _F[rel]


def meta(rel):
    import pyreadstat
    p = data_access.get_data_file(rel)
    _, m = pyreadstat.read_dta(str(p), metadataonly=True)
    return m


def vlabels(rel, var):
    m = meta(rel)
    name = m.variable_to_label.get(var)
    lab = m.value_labels.get(name, {}) if name else {}
    return {'label_set': name, 'var_label': m.column_names_to_labels.get(var),
            'labels': {str(int(k)) if float(k).is_integer() else str(k): v for k, v in lab.items()}}


BF21 = 'Burkina_Faso/2021-22/Data/'
BF18 = 'Burkina_Faso/2018-19/Data/'


@section('columns')
def _():
    out = {}
    for rel in (BF21 + 's16c_me_bfa2021.dta', BF21 + 's16d_me_bfa2021.dta', BF18 + 's16c_me_bfa2018.dta'):
        m = meta(rel)
        out[rel] = {c: m.column_names_to_labels.get(c) for c in m.column_names}
    return out


@section('value_label_sets')
def _():
    out = {}
    for rel, var in ((BF21 + 's16c_me_bfa2021.dta', 's16cq04'), (BF21 + 's16d_me_bfa2021.dta', 's16dq01'),
                     (BF18 + 's16c_me_bfa2018.dta', 's16cq04'),
                     (BF21 + 's16c_me_bfa2021.dta', 's16cq16b'), (BF21 + 's16d_me_bfa2021.dta', 's16dq05b'),
                     (BF21 + 's16c_me_bfa2021.dta', 's16cq11'), (BF21 + 's16c_me_bfa2021.dta', 's16cq07'),
                     ('Mali/2021-22/Data/s16c_me_mli2021.dta', 's16cq04'),
                     ('Mali/2021-22/Data/s16d_me_mli2021.dta', 's16dq01'),
                     ('Niger/2021-22/Data/s16c_me_ner2021.dta', 's16cq04')):
        try:
            out[f'{rel}:{var}'] = vlabels(rel, var)
        except Exception as e:  # noqa: BLE001
            out[f'{rel}:{var}'] = f'ERR {type(e).__name__}: {e}'
    return out


def _hh(f):
    return (pd.to_numeric(f['grappe']).astype('Int64').astype(str) + '/'
            + pd.to_numeric(f['menage']).astype('Int64').astype(str))


@section('code_identity')
def _():
    cl, c = frames(BF21 + 's16c_me_bfa2021.dta')
    dl, d = frames(BF21 + 's16d_me_bfa2021.dta')
    out = {'s16c_rows': len(c), 's16d_rows': len(d)}
    keep = c['s16cq04'].notna().to_numpy()
    cl, c = cl[keep], c[keep]
    out['s16c_crop_rows'] = len(c)
    exact = d.duplicated(keep='first')
    out['s16d_exact_repeats'] = int(exact.sum())
    d, dl = d[~exact.to_numpy()], dl[~exact.to_numpy()]
    out['s16d_records_no_crop'] = int(d['s16dq01'].isna().sum())
    k = d['s16dq01'].notna().to_numpy()
    d, dl = d[k], dl[k]
    out['s16d_crop_records'] = len(d)
    out['s16d_dup_hh_crop'] = int(d.duplicated(['grappe', 'menage', 's16dq01']).sum())
    chh, dhh = _hh(c), _hh(d)
    ccode = pd.to_numeric(c['s16cq04']).astype(int)
    dcode = pd.to_numeric(d['s16dq01']).astype(int)
    clab = cl['s16cq04'].astype('string').str.strip()
    dlab = dl['s16dq01'].astype('string').str.strip()
    cpairs = set(zip(chh, ccode))
    dpairs = list(zip(dhh, dcode))
    clabpairs = set(zip(chh, clab))
    hit_code = np.array([p in cpairs for p in dpairs])
    hit_lab = np.array([p in clabpairs for p in zip(dhh, dlab)])
    q = pd.to_numeric(d['s16dq05a'], errors='coerce')
    v = pd.to_numeric(d['s16dq06'], errors='coerce')
    sale = ((q > 0) | (v > 0)).to_numpy()
    out['records_with_line_by_code'] = [int(hit_code.sum()), len(d)]
    out['records_with_line_by_label'] = [int(hit_lab.sum()), len(d)]
    out['sales'] = int(sale.sum())
    out['sales_with_line_by_code'] = int(hit_code[sale].sum())
    out['sales_with_line_by_label'] = int(hit_lab[sale].sum())
    out['sales_no_line_by_label_codes'] = (pd.Series(dcode[sale & ~hit_lab]).value_counts().to_dict())
    # Reverse: s16c (hh, code) with a s16d record
    dset = set(dpairs)
    cp = pd.Series([p in dset for p in zip(chh, ccode)])
    pairs = pd.DataFrame({'hh': chh.to_numpy(), 'code': ccode.to_numpy()}).drop_duplicates()
    pairs['has_d'] = [p in dset for p in zip(pairs['hh'], pairs['code'])]
    out['s16c_pairs'] = len(pairs)
    out['s16c_pairs_without_d_record'] = int((~pairs['has_d']).sum())
    out['s16c_pairs_without_d_by_code'] = pairs.loc[~pairs['has_d'], 'code'].value_counts().head(30).to_dict()
    # Households whose s16c has NO record at all in s16d (crop placeholders)
    dh = set(dhh)
    out['s16c_households'] = int(chh.nunique())
    out['s16c_households_absent_from_s16d_crop_records'] = int(len(set(chh) - dh))
    # per disputed code
    per = {}
    for code in (3, 8, 9, 27, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65):
        cm = ccode.eq(code).to_numpy()
        dm = (dcode == code).to_numpy()
        per[code] = {'s16c_rows': int(cm.sum()), 's16c_hh': int(chh[cm].nunique()),
                     's16c_labels': clab[cm].value_counts().to_dict(),
                     's16d_records': int(dm.sum()), 's16d_labels': dlab[dm].value_counts().to_dict(),
                     's16d_with_line_by_code': int(hit_code[dm].sum())}
    out['per_code'] = per
    # Cross: households with s16c 55 vs s16d 55/65 and vice versa
    hh55c = set(chh[ccode.eq(55).to_numpy()])
    hh65c = set(chh[ccode.eq(65).to_numpy()])
    hh55d = set(dhh[(dcode == 55).to_numpy()])
    hh65d = set(dhh[(dcode == 65).to_numpy()])
    out['cross_55_65'] = {'c55&d55': len(hh55c & hh55d), 'c55&d65': len(hh55c & hh65d),
                          'c65&d65': len(hh65c & hh65d), 'c65&d55': len(hh65c & hh55d),
                          'c55': len(hh55c), 'c65': len(hh65c), 'd55': len(hh55d), 'd65': len(hh65d)}
    # The other-text columns, if any: what is written beside each code
    texts = {}
    disputed = {53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65}
    for tag, f, codecol in (('c', c, ccode), ('d', d, dcode)):
        for col in f.columns:
            s = f[col]
            if not (s.dtype == object or str(s.dtype).startswith(('string', 'str'))):
                continue
            if col in ('grappe', 'menage'):
                continue
            s = s.astype('string').str.strip()
            filled = (s.notna() & s.ne('')).fillna(False).to_numpy()
            if not filled.any():
                texts[f'{tag}:{col}'] = 'never filled'
                continue
            cc = pd.Series(codecol.to_numpy()[filled])
            vals = s[filled].str.lower().reset_index(drop=True)
            rec = {'filled': int(filled.sum()),
                   'filled_by_code': cc.value_counts().head(40).to_dict(),
                   'top_values_overall': vals.value_counts().head(10).to_dict()}
            rec['top_values_disputed_codes'] = {
                int(k): g.value_counts().head(15).to_dict()
                for k, g in vals.groupby(cc.to_numpy()) if int(k) in disputed}
            texts[f'{tag}:{col}'] = rec
    out['string_columns_by_code'] = texts
    # Every string column in s16c/s16d (to find a text field we missed)
    out['string_columns'] = {
        'c': [col for col in c.columns if c[col].dtype == object or str(c[col].dtype).startswith(('string', 'str'))],
        'd': [col for col in d.columns if d[col].dtype == object or str(d[col].dtype).startswith(('string', 'str'))]}
    # Region distribution of the disputed codes (s00 cover, panel)
    try:
        s0l, s0 = frames(BF21 + 's00_me_bfa2021.dta')
        reg = {}
        regcol = 's00q01' if 's00q01' in s0l else None
        if regcol:
            rmap = dict(zip(_hh(s0), s0l[regcol].astype('string')))
            for code in (53, 54, 55, 58, 64, 65, 43):
                hh_ = chh[ccode.eq(code).to_numpy()]
                reg[code] = pd.Series([rmap.get(h) for h in hh_]).value_counts().to_dict()
        out['region_by_code'] = reg
    except Exception as e:  # noqa: BLE001
        out['region_by_code'] = f'ERR {e}'
    # Units: code identity on the attached single-line sales with a harvest unit
    cu = pd.to_numeric(c['s16cq16b'], errors='coerce')
    du = pd.to_numeric(d['s16dq05b'], errors='coerce')
    cul = cl['s16cq16b'].astype('string').str.strip()
    dul = dl['s16dq05b'].astype('string').str.strip()
    n_lines = pd.Series(1, index=pd.MultiIndex.from_arrays([chh, ccode])).groupby(level=[0, 1]).size()
    single_pairs = set(n_lines[n_lines == 1].index)
    cmap_u = {p: (u, l) for p, u, l in zip(zip(chh, ccode), cu, cul)}
    eq_code = eq_lab = both = 0
    for p, u, l, s_ in zip(dpairs, du, dul, sale):
        if not s_ or p not in single_pairs:
            continue
        hu, hl = cmap_u[p]
        if pd.isna(hu) or pd.isna(u):
            continue
        both += 1
        eq_code += int(hu == u)
        eq_lab += int(str(hl) == str(l))
    out['unit_single_line_sales_with_both_units'] = {'n': both, 'equal_by_code': eq_code, 'equal_by_label': eq_lab}
    try:
        def pairs_of(a, b):
            fr = pd.DataFrame({'code': a.to_numpy(), 'label': b.to_numpy()}).dropna()
            return fr.groupby(['code', 'label']).size().reset_index().astype(str).values.tolist()[:60]
        out['unit_code_label_pairs'] = {'s16cq16b': pairs_of(cu, cul), 's16dq05b': pairs_of(du, dul)}
    except Exception as e:  # noqa: BLE001
        out['unit_code_label_pairs'] = f'ERR {e}'
    return out


@section('panel_crops_2018_of_disputed_codes')
def _():
    """2021 households with code 55/58/64/65 lines: what did their 2018-19 lines say?"""
    import json as _j
    ids = _j.load(open(R / 'Burkina_Faso/_/updated_ids.json'))['2021-22']
    cl, c = frames(BF21 + 's16c_me_bfa2021.dta')
    keep = c['s16cq04'].notna().to_numpy()
    c = c[keep]
    i21 = [str(int(g)) + str(int(m)).zfill(3) for g, m in zip(c['grappe'], c['menage'])]
    i18 = [ids.get(x) for x in i21]
    l18, c18 = frames(BF18 + 's16c_me_bfa2018.dta')
    h18 = [str(int(g)) + '0' + str(int(m)).zfill(2) for g, m in zip(c18['grappe'], c18['menage'])]
    lab18 = l18['s16cq04'].astype('string').str.strip()
    by_hh = {}
    for h_, l_ in zip(h18, lab18):
        if pd.notna(l_):
            by_hh.setdefault(h_, set()).add(str(l_))
    out = {'mapped_households': int(sum(x is not None for x in i18))}
    code = pd.to_numeric(c['s16cq04']).astype(int).to_numpy()
    for k in (53, 54, 55, 58, 64, 65):
        hs = sorted({h for h, cc in zip(i18, code) if cc == k and h is not None})
        cnt = {}
        for h in hs:
            for lab in by_hh.get(h, ()):
                cnt[lab] = cnt.get(lab, 0) + 1
        out[k] = {'households_2021_with_2018_id': len(hs),
                  "with_2018_crop_rows": int(sum(h in by_hh for h in hs)),
                  '2018_label_counts': dict(sorted(cnt.items(), key=lambda kv: -kv[1])[:15])}
    # 2018 other text, if any
    tx = {}
    code18 = pd.to_numeric(c18['s16cq04'], errors='coerce')
    for col in c18.columns:
        if col in ('grappe', 'menage'):
            continue
        if c18[col].dtype == object or str(c18[col].dtype).startswith(('string', 'str')):
            s = c18[col].astype('string').str.strip()
            filled = (s.notna() & s.ne('')).fillna(False).to_numpy()
            tx[col] = {'filled': int(filled.sum()),
                       'filled_by_code': code18[filled].value_counts().head(20).to_dict(),
                       'top_values': s[filled].str.lower().value_counts().head(25).to_dict()}
    out['2018_string_columns'] = tx
    # 2018 other-text of the 2021 code-55 / 65 households (panel), if any text column exists
    hs55 = {h for h, cc in zip(i18, code) if cc == 55 and h is not None}
    hs65 = {h for h, cc in zip(i18, code) if cc == 65 and h is not None}
    hs64 = {h for h, cc in zip(i18, code) if cc == 64 and h is not None}
    h18s = pd.Series(h18)
    for col in tx:
        s = c18[col].astype('string').str.strip().str.lower().reset_index(drop=True)
        for nm, hs in (('55', hs55), ('65', hs65), ('64', hs64)):
            m = h18s.isin(hs).to_numpy() & (s.notna() & s.ne('')).fillna(False).to_numpy()
            if m.any():
                out.setdefault('2018_text_of_2021_code_households', {}).setdefault(nm, {})[col] = \
                    s[m].value_counts().head(15).to_dict()
    return out


@section('sales_value')
def _():
    dl, d = frames(BF21 + 's16d_me_bfa2021.dta')
    v = pd.to_numeric(d['s16dq06'], errors='coerce')
    q = pd.to_numeric(d['s16dq05a'], errors='coerce')
    exact = d.duplicated(keep='first').to_numpy()
    out = {'raw_sum': float(v.sum()), 'raw_nonnull': int(v.notna().sum()),
           'raw_positive': int((v > 0).sum()), 'raw_zero': int((v == 0).sum()), 'raw_negative': int((v < 0).sum()),
           'sum_after_exact_dedup': float(v[~exact].sum()),
           'value_on_record_without_crop': float(v[d['s16dq01'].isna().to_numpy()].sum()),
           'qty_pos_value_na': int(((q > 0) & v.isna()).sum()), 'value_pos_qty_na': int(((v > 0) & q.isna()).sum()),
           'value_zero_qty_pos': int(((v == 0) & (q > 0)).sum()),
           'max_value': float(v.max()), 'top5_values': v.nlargest(5).tolist(),
           'q04_on_sales': dl.loc[((q > 0) | (v > 0)).to_numpy(), 's16dq04'].astype('string').value_counts(dropna=False).to_dict(),
           'q04_all': dl['s16dq04'].astype('string').value_counts(dropna=False).to_dict()}
    # sentinel-looking values
    out['nines'] = int(v.isin([99, 999, 9999, 99999, 999999, 9999999]).sum())
    return out


@section('harvest_block_by_finished')
def _():
    cl, c = frames(BF21 + 's16c_me_bfa2021.dta')
    keep = c['s16cq04'].notna().to_numpy()
    cl, c = cl[keep], c[keep]
    fin = cl['s16cq11'].astype('string').str.strip()
    out = {'s16cq11_labels': fin.value_counts(dropna=False).to_dict(),
           's16cq11_codes': pd.to_numeric(c['s16cq11'], errors='coerce').value_counts(dropna=False).to_dict()}
    cols = [x for x in c.columns if x.startswith('s16cq1') or x.startswith('s16cq0')]
    tab = {}
    for col in cols:
        s = c[col]
        filled = s.notna() & ~(s.astype('string').str.strip().eq('').fillna(False))
        tab[col] = {str(k): int(filled[(fin == k).fillna(False).to_numpy()].sum()) for k in fin.dropna().unique()}
        tab[col]['NA'] = int(filled[fin.isna().to_numpy()].sum())
    out['filled_by_finished'] = tab
    qa = pd.to_numeric(c['s16cq16a'], errors='coerce')
    out['q16a_notna'] = int(qa.notna().sum())
    out['q16a_zero'] = int((qa == 0).sum())
    out['q16a_notna_by_finished'] = {str(k): int(n) for k, n in fin[qa.notna().to_numpy()].value_counts(dropna=False).items()}
    out['q16a_sum'] = float(qa.sum())
    out['q16a_unit_codes'] = pd.to_numeric(c.loc[qa.notna().to_numpy(), 's16cq16b'], errors='coerce').value_counts(dropna=False).to_dict()
    out['q12_on_q16a_rows'] = pd.to_numeric(c.loc[qa.notna().to_numpy(), 's16cq12'], errors='coerce').describe().to_dict()
    out['intercrop_labels'] = cl['s16cq07'].astype('string').value_counts(dropna=False).to_dict()
    return out


@section('repeated_lines')
def _():
    cl, c = frames(BF21 + 's16c_me_bfa2021.dta')
    keep = c['s16cq04'].notna().to_numpy()
    cl, c = cl[keep], c[keep]
    key = ['grappe', 'menage', 's16cq02', 's16cq03', 's16cq04']
    dup = c.duplicated(key, keep=False).to_numpy()
    rows = c[dup].sort_values(key)
    out = {'n_rows': int(dup.sum())}
    groups = []
    for _, g in rows.groupby(key):
        differing = [col for col in g.columns if g[col].astype('string').nunique(dropna=False) > 1]
        groups.append({'n': len(g), 'differing': differing,
                       'values': {col: g[col].astype('string').tolist() for col in differing + ['s16cq07', 's16cq11', 's16cq16a', 's16cq16b']},
                       'labels': {col: cl.loc[g.index, col].astype('string').tolist() for col in differing if col in cl}})
    out['groups'] = groups
    # 2018-19: are there repeats too (unique on household, field, parcel, crop, unit per CONTENTS)?
    l18, c18 = frames(BF18 + 's16c_me_bfa2018.dta')
    out['2018_dups_hh_field_parcel_crop'] = int(c18.duplicated(['grappe', 'menage', 's16cq02', 's16cq03', 's16cq04']).sum())
    return out


@section('plot_inputs_units')
def _():
    out = {}
    for rel in (BF18 + 's16b_me_bfa2018.dta', BF21 + 's16b_me_bfa2021.dta'):
        lab, num = frames(rel)
        m = {}
        for var in ('s16bq03b', 's16bq07b', 's16bq09b'):
            if var in num:
                try:
                    m[var] = vlabels(rel, var)['labels']
                except Exception as e:  # noqa: BLE001
                    m[var] = f'ERR {e}'
        applied_col = 's16bq02'
        appl = lab[applied_col].astype('string').str.strip() if applied_col in lab else None
        rec = {'rows': len(num), 'labels_contain_Manquant': {k: any('anquant' in str(x) for x in (v.values() if isinstance(v, dict) else [])) for k, v in m.items()},
               's16bq02_labels': appl.value_counts(dropna=False).to_dict() if appl is not None else None}
        if appl is not None:
            yes = pd.to_numeric(num['s16bq02'], errors='coerce').eq(1).fillna(False).to_numpy()
            rec['applied_rows'] = int(yes.sum())
            u = num['s16bq03b']
            rec['applied_blank_unit'] = int(u[yes].isna().sum())
            rec['applied_blank_unit_but_quantity'] = int((u[yes].isna() & pd.to_numeric(num.loc[yes, 's16bq03a'], errors='coerce').notna()).sum())
        rec['all_blank_unit'] = int(num['s16bq03b'].isna().sum())
        out[rel] = rec
    return out


@section('interview_stamps')
def _():
    out = {}
    import re
    iso = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')
    for rel in (BF18 + 's00_me_bfa2018.dta', BF21 + 's00_me_bfa2021.dta', BF21 + 'Cross_Section/s00_me_bfa2021.dta'):
        try:
            lab, num = frames(rel)
        except Exception as e:  # noqa: BLE001
            out[rel] = f'ERR {e}'
            continue
        rec = {'rows': len(num)}
        for col in ('s00q23a', 's00q23b', 's00q24a', 's00q24b', 's00q25a', 's00q25b'):
            if col not in num:
                rec[col] = 'absent'
                continue
            s = num[col]
            rec[col + '_dtype'] = str(s.dtype)
            st = s.astype('string')
            nonmatch = st[st.notna() & ~st.str.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$').fillna(False)]
            rec[col] = nonmatch.value_counts(dropna=False).head(8).to_dict()
            rec[col + '_na'] = int(st.isna().sum())
        out[rel] = rec
    return out


@section('section_c_2014')
def _():
    out = {}
    files = {1: 'emc2014_p1_conso7jours_16032015.dta', 2: 'emc2014_p2_conso7jours_16032015.dta',
             3: 'emc2014_p3_conso7jours_16032015.dta', 4: 'emc2014_p4_conso7jours_16032015.dta'}
    for p, f in files.items():
        rel = 'Burkina_Faso/2014/Data/' + f
        rec = {}
        try:
            m = meta(rel)
            rec['labels'] = {v: m.column_names_to_labels.get(v) for v in
                             ('achat', 'qachat', 'uachat', 'autocons', 'qautocons', 'uautocons',
                              'cadeau', 'qcadeau', 'ucadeau', 'mtt_dep', 'product') if v in m.column_names}
        except Exception as e:  # noqa: BLE001
            rec['labels'] = f'ERR {e}'
        lab, num = frames(rel)
        _F.pop(rel, None)
        for v in ('achat', 'autocons', 'cadeau', 'mtt_dep'):
            if v not in num:
                rec[v] = 'absent'
                continue
            x = pd.to_numeric(num[v], errors='coerce')
            pos = x[x > 0]
            rec[v] = {'positive': int(len(pos)), 'sum': float(pos.sum()), 'zero': int((x == 0).sum()),
                      'neg': int((x < 0).sum()), 'na': int(x.isna().sum()),
                      'mult5': round(float((pos % 5 == 0).mean()), 4) if len(pos) else None,
                      'mult25': round(float((pos % 25 == 0).mean()), 4) if len(pos) else None,
                      'mult50': round(float((pos % 50 == 0).mean()), 4) if len(pos) else None,
                      'nonint': int((pos % 1 != 0).sum()),
                      'top_values': pos.value_counts().head(8).to_dict()}
        a = pd.to_numeric(num.get('achat'), errors='coerce')
        ac = pd.to_numeric(num.get('autocons'), errors='coerce') if 'autocons' in num else None
        cd = pd.to_numeric(num.get('cadeau'), errors='coerce') if 'cadeau' in num else None
        if ac is not None:
            rec['achat_eq_autocons_pos'] = int(((a == ac) & (a > 0)).sum())
            rec['sum_achat_eq_autocons'] = float(ac[(a == ac) & (a > 0)].sum())
        if cd is not None:
            rec['achat_eq_cadeau_pos'] = int(((a == cd) & (a > 0)).sum())
            rec['sum_achat_eq_cadeau'] = float(cd[(a == cd) & (a > 0)].sum())
        if p == 1:
            qa = pd.to_numeric(num['qachat'], errors='coerce')
            qp = pd.to_numeric(num['qautocons'], errors='coerce')
            qg = pd.to_numeric(num['qcadeau'], errors='coerce')
            rec['autocons_pos_qty_na'] = int(((ac > 0) & qp.isna()).sum())
            rec['autocons_pos_qty_pos'] = int(((ac > 0) & (qp > 0)).sum())
            rec['qty_pos_autocons_na'] = int(((qp > 0) & ac.isna()).sum())
            rec['cadeau_pos_qty_pos'] = int(((cd > 0) & (qg > 0)).sum())
            # Imputation probe: unit values identical within (zd, product, unit) cells
            def cell_identity(val, qty, unit, tag):
                fr = pd.DataFrame({'zd': num['zd'], 'prod': num['product'], 'u': unit,
                                   'uv': (val / qty)}).loc[(val > 0) & (qty > 0) & unit.notna()]
                g = fr.groupby(['zd', 'prod', 'u'])['uv']
                n = g.size()
                nu = g.nunique()
                big = n >= 3
                nat = fr.groupby(['prod', 'u'])['uv']
                modal_share = (nat.transform(lambda s: (s == s.mode().iloc[0]).mean()))
                return {f'{tag}_rows': int(len(fr)), f'{tag}_cells_ge3': int(big.sum()),
                        f'{tag}_cells_ge3_all_equal': int((nu[big] == 1).sum()),
                        f'{tag}_row_share_at_national_mode': round(float(modal_share.mean()), 4) if len(fr) else None}
            rec.update(cell_identity(a, qa, num['uachat'], 'achat'))
            rec.update(cell_identity(ac, qp, num['uautocons'], 'autocons'))
            rec.update(cell_identity(cd, qg, num['ucadeau'], 'cadeau'))
            # Does autocons equal qautocons x the household's own purchase unit value (same unit)?
            same_u = (num['uachat'] == num['uautocons']) & (qa > 0) & (a > 0) & (qp > 0) & (ac > 0)
            implied = qp * a / qa
            rec['autocons_eq_own_purchase_uv_x_q'] = [int((same_u & ((implied - ac).abs() <= 1)).sum()), int(same_u.sum())]
            # ... or the zd median purchase unit value in the same unit?
            fr = pd.DataFrame({'zd': num['zd'], 'prod': num['product'], 'u': num['uachat'], 'uv': a / qa})
            fr = fr[(a > 0) & (qa > 0) & num['uachat'].notna()]
            med = fr.groupby(['zd', 'prod', 'u'])['uv'].median()
            key = pd.MultiIndex.from_arrays([num['zd'], num['product'], num['uautocons']])
            zmed = pd.Series(med.reindex(key).to_numpy(), index=num.index)
            ok = (qp > 0) & (ac > 0) & zmed.notna()
            rec['autocons_eq_zd_median_purchase_uv_x_q'] = [int((ok & ((qp * zmed - ac).abs() <= 1)).sum()), int(ok.sum())]
        out[p] = rec
        save()
    return out


save()
print('raw census complete', flush=True)
