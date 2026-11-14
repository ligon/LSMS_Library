"""Delta compare + raw oracles.  Usage: delta_cmp.py BASE_FR CAND_FR CAND.json OUT.json
(cwd = candidate clone, PYTHONPATH = it, LSMS_DATA_DIR = the candidate's root)."""
import json
import re
import sys
import traceback
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

BF, CF, CJ, OJ = sys.argv[1:5]
cj = json.load(open(CJ))
R = countries_root()
res = {}
KEYP = 'Burkina_Faso::crop_production::'


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


def save():
    json.dump(_sk(res), open(OJ, 'w'), indent=1, default=str)


def sect(name, fn):
    try:
        res[name] = fn()
    except Exception:  # noqa: BLE001
        res[name] = 'ERR ' + traceback.format_exc()[-2500:]
    save()
    print('done', name, flush=True)


def L(d, n):
    return pd.read_parquet(f'{d}/{n}.parquet')


def eq(a, b, cols=None):
    cols = cols or [c for c in a.columns if c in b.columns]
    try:
        pd.testing.assert_frame_equal(a[cols].sort_values(cols, na_position='last').reset_index(drop=True),
                                      b[cols].sort_values(cols, na_position='last').reset_index(drop=True))
        return True
    except Exception as e:  # noqa: BLE001
        return f'{type(e).__name__}: {str(e)[:500]}'


def keyed(f, key):
    d = f['Derivation'].astype('string').fillna('')
    return f[d.str.split('+').map(lambda ks: (KEYP + key) in ks)]


def s2018():
    b, c = L(BF, 'crop_production'), L(CF, 'crop_production')
    c18 = c[c['t'].astype(str) == '2018-19']
    out = {'rows': [len(b), len(c18)], 'cand_extra_cols': sorted(set(c18.columns) - set(b.columns)),
           'production_obs_values': c18['production_obs'].astype(str).value_counts().to_dict(),
           'derivation_nonnull': int(c18['Derivation'].notna().sum()) if 'Derivation' in c18 else None,
           'equal_on_base_cols': eq(b, c18, list(b.columns))}
    fb, fc = L(BF, 'feature_crop_bf'), L(CF, 'feature_crop_bf')
    fb18, fc18 = fb[fb['t'].astype(str) == '2018-19'], fc[fc['t'].astype(str) == '2018-19']
    out['feature_2018'] = {'rows': [len(fb18), len(fc18)], 'cols_b': list(fb18.columns), 'cols_c': list(fc18.columns),
                           'equal_all_base_cols': eq(fb18, fc18, list(fb18.columns)),
                           'cand_only_cols': sorted(set(fc18.columns) - set(fb18.columns)),
                           'cand_only_cols_nonnull': {k: int(fc18[k].notna().sum()) for k in set(fc18.columns) - set(fb18.columns)}}
    mb, mc = L(BF, 'feature_crop_bf_mali'), L(CF, 'feature_crop_bf_mali')
    out['feature_mali_slice'] = {'rows': [int((mb['country'] == 'Mali').sum()), int((mc['country'] == 'Mali').sum())],
                                 'equal': eq(mb[mb['country'] == 'Mali'], mc[mc['country'] == 'Mali']),
                                 'unique_cand': cj['calls']['feature_crop_bf_mali'].get('unique'),
                                 'unique_cand_bf': cj['calls']['feature_crop_bf'].get('unique'),
                                 'feature_index': cj['calls']['feature_crop_bf_mali'].get('index'),
                                 'cand_feature_bf_production_obs': mc[mc['country'] == 'Burkina_Faso']['production_obs'].astype(str).str.slice(0, 8).value_counts().to_dict()}
    return out


sect('crop_2018_and_feature', s2018)


def s2021():
    c = L(CF, 'crop_production')
    c21 = c[c['t'].astype(str) == '2021-22'].copy()
    farm = c21['plot_id'].astype(str) == 'Farm-level'
    po = c21['production_obs'].astype(str)
    out = {'rows': len(c21), 'farm_level': int(farm.sum()), 'lines': int((~farm).sum()),
           'po_lines_rowdvc': int(po[~farm].str.startswith('row:dvc:').sum()), 'po_lines_unique': bool(po[~farm].is_unique),
           'po_farm': po[farm].value_counts().to_dict()}
    k = ['t', 'i', 'plot_id', 'crop', 'u', 'condition']
    dup = c21.duplicated(k, keep=False)
    out['dup_rows_without_po'] = int(dup.sum())
    out['dup_rows_keyed'] = int(c21.loc[dup, 'Derivation'].notna().sum())
    out['dup_rows'] = c21.loc[dup, ['crop', 'plot_id', 'intercropped', 'Quantity']].astype(str).values.tolist()
    out['u_set'] = sorted(c21['u'].dropna().astype(str).unique())[:60]
    out['unit_sold_set'] = sorted(c21['Unit_sold'].dropna().astype(str).unique())[:60]
    out['unit_sold_not_in_u_vocab'] = sorted(set(c21['Unit_sold'].dropna().astype(str)) - set(c21['u'].dropna().astype(str)))[:30]
    out['crop_counts_text'] = c21[c21['crop'].astype(str).isin(['Anacarde', 'Banane douce', 'Soja', 'Autre (à préciser)'])]['crop'].astype(str).value_counts().to_dict()
    out['crop_qualified_left'] = int(c21['crop'].astype(str).str.startswith('Unknown [').sum())
    out['derivation_counts'] = c21['Derivation'].astype('string').fillna('<NA>').value_counts().to_dict()
    out['value_sold_sum'] = float(c21['Value_sold'].sum())
    out['sales_rows'] = int((c21['Quantity_sold'].notna() | c21['Value_sold'].notna()).sum())
    out['condition_sold'] = c21['Condition_sold'].astype(str).value_counts(dropna=False).to_dict()
    out['quantity_rows'] = int(c21['Quantity'].notna().sum())
    d = keyed(c21, 'harvest-from-dispositions')
    out['disp_rows'] = len(d)
    out['disp_farm'] = int((d['plot_id'].astype(str) == 'Farm-level').sum())
    out['disp_sum'] = float(d['Quantity'].sum())
    out['disp_u'] = d['u'].astype(str).value_counts().to_dict()
    out['disp_condition'] = d['condition'].astype(str).value_counts().to_dict()
    # farm rows: from sales vs from dispositions only
    fsale = farm & (c21['Quantity_sold'].notna() | c21['Value_sold'].notna())
    out['farm_with_sale'] = int(fsale.sum())
    out['farm_with_disp_and_sale'] = int((fsale & c21['Derivation'].astype('string').fillna('').str.contains('dispositions')).sum())
    return out


sect('crop_2021', s2021)


def raw_oracles():
    c = get_dataframe(str(R / 'Burkina_Faso/2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=False)
    cl = get_dataframe(str(R / 'Burkina_Faso/2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=True)
    d = get_dataframe(str(R / 'Burkina_Faso/2021-22/Data/s16d_me_bfa2021.dta'), convert_categoricals=False)
    k = c['s16cq04'].notna().to_numpy()
    c, cl = c[k], cl[k]
    d = d[~d.duplicated()]
    d = d[d['s16dq01'].notna()]
    hh = lambda f: (pd.to_numeric(f['grappe']).astype(int).astype(str) + '/' + pd.to_numeric(f['menage']).astype(int).astype(str))  # noqa: E731
    c = c.assign(hh=hh(c).to_numpy(), code=pd.to_numeric(c['s16cq04']).astype(int).to_numpy())
    d = d.assign(hh=hh(d).to_numpy(), code=pd.to_numeric(d['s16dq01']).astype(int).to_numpy())
    out = {}
    # text evidence, own patterns
    txt = c['s16cq03b'].astype('string').str.lower().fillna('')
    pats = {55: r'anac|anar|cajou|caju|acajou', 58: r'banan', 64: r'soj|soya'}
    for code, p in pats.items():
        m = c['code'] == code
        out[f'text_{code}'] = [int(txt[m.to_numpy()].str.contains(p, regex=True).sum()), int(m.sum())]
        out[f'text_{code}_nonmatching'] = txt[m.to_numpy() & ~txt.str.contains(p, regex=True).to_numpy()].value_counts().head(10).to_dict()
    aut = c['s16cq03_autre'].astype('string').str.strip()
    out['precisez_on_65'] = [int(aut[(c['code'] == 65).to_numpy()].notna().sum()), int((c['code'] == 65).sum())]
    out['precisez_on_55'] = [int(aut[(c['code'] == 55).to_numpy()].fillna('').ne('').sum()), int((c['code'] == 55).sum())]
    # dispositions oracle by CODE
    fin = pd.to_numeric(c['s16cq11'], errors='coerce').eq(1)
    rec = pd.to_numeric(c['s16cq16a'], errors='coerce').notna()
    pairs = pd.DataFrame({'hh': c['hh'], 'code': c['code'], 'fin': fin, 'rec': rec}).groupby(['hh', 'code']).agg(
        n=('fin', 'size'), fin=('fin', 'all'), rec=('rec', 'any'))
    long = pd.concat([pd.DataFrame({'hh': d['hh'], 'code': d['code'],
                                    'q': pd.to_numeric(d[f'{v}a'], errors='coerce'),
                                    'u': pd.to_numeric(d[f'{v}b'], errors='coerce')})
                      for v in ('s16dq02', 's16dq03', 's16dq05', 's16dq13')])
    long = long[long['q'] > 0]
    agg = long.groupby(['hh', 'code']).agg(total=('q', 'sum'), units=('u', 'nunique'),
                                           unitless=('u', lambda s: bool(s.isna().any())), unit=('u', 'first'))
    j = pairs.join(agg, how='left')
    cand = j[j['fin'] & ~j['rec']]
    want = cand[cand['units'].eq(1) & ~cand['unitless'].fillna(True).astype(bool)]
    out['oracle_finished_no_record_pairs'] = len(cand)
    out['oracle_one_unit'] = len(want)
    out['oracle_one_unit_single_line'] = int((want['n'] == 1).sum())
    out['oracle_one_unit_farm'] = int((want['n'] > 1).sum())
    out['oracle_span_units'] = int((cand['units'] > 1).sum())
    out['oracle_none'] = int(cand['units'].isna().sum())
    out['oracle_unitless'] = int((cand['unitless'].fillna(False).astype(bool) & cand['units'].le(1)).sum())
    out['oracle_sum'] = float(want['total'].sum())
    out['oracle_unit_codes'] = want['unit'].value_counts().head(20).to_dict()
    out['pairs_with_unfinished'] = int((~pairs['fin']).sum())
    # served vs oracle, per pair
    s = L(CF, 'crop_production')
    s = s[s['t'].astype(str) == '2021-22']
    kd = keyed(s, 'harvest-from-dispositions')
    out['served_disp_rows'] = len(kd)
    out['served_disp_sum'] = float(kd['Quantity'].sum())
    return out


sect('raw_oracles', raw_oracles)


def inrow_2018():
    c = get_dataframe(str(R / 'Burkina_Faso/2018-19/Data/s16c_me_bfa2018.dta'), convert_categoricals=False)
    fin = pd.to_numeric(c['s16cq10'], errors='coerce').eq(1)
    h = pd.to_numeric(c['s16cq12a'], errors='coerce')
    hu = pd.to_numeric(c['s16cq12b'], errors='coerce')
    parts = [('s16cq13a', 's16cq13b'), ('s16cq14a', 's16cq14b'), ('s16cq16a', 's16cq16b'), ('s16cq22a', 's16cq22b')]
    q = pd.concat([pd.to_numeric(c[a], errors='coerce') for a, _ in parts], axis=1)
    u = pd.concat([pd.to_numeric(c[b], errors='coerce') for _, b in parts], axis=1)
    pos = q.gt(0)
    same = (~pos) | u.eq(hu, axis=0)
    all_same = same.all(axis=1)
    tot = q.where(pos).sum(axis=1, min_count=1)
    out = {'rows': len(c), 'finished': int(fin.sum())}
    base = fin & all_same
    out['finished_all_positive_in_harvest_unit'] = int(base.sum())
    for tag, m in (('A_as_worker', base), ('B_harvest_pos_and_some_disposition', base & h.gt(0) & pos.any(axis=1))):
        r = (tot / h)[m]
        out[tag] = {'n': int(m.sum()), 'exact': round(float((tot[m] == h[m]).mean()), 4),
                    'within10': round(float(((r - 1).abs() <= 0.1).mean()), 4),
                    'median_ratio': float(r.median()), 'over_1_1': round(float((r > 1.1).mean()), 4),
                    'under_0_9': round(float((r < 0.9).mean()), 4),
                    'no_disposition': int((~pos.any(axis=1))[m].sum()), 'harvest_na': int(h[m].isna().sum())}
    return out


sect('inrow_2018', inrow_2018)


def inputs():
    c = L(CF, 'crop_production')
    c21 = c[c['t'].astype(str) == '2021-22']
    lv = ['t', 'i', 'plot_id', 'crop']
    out = {}
    for key in ('hh-crop-sale-single-row', 'harvest-to-date', 'harvest-from-dispositions', 'crop-code-from-roster-text'):
        try:
            inp = L(CF, 'inputs_' + key)
        except Exception as e:  # noqa: BLE001
            out[key] = f'no frame: {cj["calls"].get("inputs_" + key, {}).get("err")}'
            continue
        kr = keyed(c21, key)
        ik = list(zip(*(inp[x].astype(str) for x in lv)))
        kk = list(zip(*(kr[x].astype(str) for x in lv)))
        si, sk = set(ik), set(kk)
        rec = {'inputs': len(inp), 'keyed': len(kr), 'inputs_unique': len(si) == len(ik), 'keyed_unique': len(sk) == len(kk),
               'keyed_not_in_inputs': len(sk - si), 'inputs_not_keyed': len(si - sk)}
        if key == 'harvest-from-dispositions':
            m = kr.merge(inp, on=lv, how='inner')
            tot = sum(pd.to_numeric(m[f'{v}a'], errors='coerce').where(lambda x: x > 0).fillna(0)
                      for v in ('s16dq02', 's16dq03', 's16dq05', 's16dq13'))
            rec['qty_equals_sum_of_inputs'] = int(np.isclose(m['Quantity'].astype(float), tot).sum())
            rec['merged'] = len(m)
        if key == 'crop-code-from-roster-text':
            extra = inp.set_index(lv).index.difference(kr.set_index(lv).index)
            rec['extra_inputs_plot_id'] = pd.Series([x[2] for x in extra]).value_counts().head(3).to_dict()
        out[key] = rec
    return out


sect('inputs_join', inputs)


def others():
    out = {}
    for n in ('harvest_kg', 'yield_kg_parcel'):
        b, c = L(BF, n), L(CF, n)
        out[n] = {'rows_by_t_base': b.groupby(b['t'].astype(str)).size().astype(int).to_dict(),
                  'rows_by_t_cand': c.groupby(c['t'].astype(str)).size().astype(int).to_dict(),
                  'farm_level_rows_cand': int((c['plot_id'].astype(str) == 'Farm-level').sum()) if 'plot_id' in c else None,
                  'equal_2018': eq(b, c[c['t'].astype(str) == '2018-19'], list(b.columns))}
    for n in ('gcr', 'cdiv', 'mpv'):
        b, c = L(BF, n), L(CF, n)
        out[n] = {'rows': [len(b), len(c)], 'equal_2018': eq(b, c[c['t'].astype(str) == '2018-19'], list(b.columns))}
    g = L(CF, 'gcr')
    out['gcr_2021_sum'] = float(g[g['t'].astype(str) == '2021-22'].iloc[:, -1].sum())
    for n, keys in (('food_acquired', ['i', 't', 'v', 'j', 'u', 's', 'visit']),
                    ('food_prices', None), ('food_prices_unitvalue', None), ('food_quantities', None)):
        b, c = L(BF, n), L(CF, n)
        rec = {'rows': [len(b), len(c)]}
        val = [x for x in b.columns if x in ('Price', 'Quantity', 'Expenditure')]
        keys = keys or [x for x in b.columns if x not in val]
        bi = b.set_index(keys)
        ci = c.set_index(keys)
        com = bi.index.intersection(ci.index)
        for v in val:
            rec[f'moved_{v}'] = int((~np.isclose(bi.loc[com, v].astype(float), ci.loc[com, v].astype(float), equal_nan=True)).sum())
        rec['only_cand'] = len(ci.index.difference(bi.index))
        rec['only_base'] = len(bi.index.difference(ci.index))
        out[n] = rec
    for n in ('interview_date', 'plot_inputs'):
        out[n] = eq(L(BF, n), L(CF, n))
    return out


sect('others', others)
save()
print('delta cmp complete', flush=True)
