"""GH #587 final: measure the bands / groups the new rulings act on (aggregates only).

argv: clone results_dir tag
"""
import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, Recorder  # noqa: E402

clone, res, tag = sys.argv[1], sys.argv[2], sys.argv[3]
head = assert_env(clone)
rec = Recorder(os.path.join(res, f'measure-{tag}.json'))
rec.data['head'] = head

from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library.countries.Nigeria._ import pp_community_prices as pp  # noqa: E402

NG = countries_root() / 'Nigeria'
RAW = {}


def raw(key):
    if key not in RAW:
        RAW[key] = get_dataframe(str(NG / pp.pp_source(key)['source_path']),
                                 convert_categoricals=False)
    return RAW[key]


def disp(key):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return pp.pp_dispositions(raw(key), key)


METRIC = ('Kg', 'g', 'l', 'ml')


def ucls(u):
    return u.where(u.isin(METRIC + ('Unknown',)), 'non-metric')


def band(n):
    return pd.cut(n, [-np.inf, 0.999999, 1.0, 5.0, 50.0, np.inf],
                  labels=['<1', '1', '2-5', '6-50', '>50']).astype('string').fillna('no count')


def w1_bands():
    r = raw('W1')
    d = disp('W1')
    n = pd.to_numeric(r['sc2q3b'], errors='coerce')
    price = pd.to_numeric(r['sc2q3a'], errors='coerce')
    priced = price.gt(0)
    out = {'rows': int(len(r)), 'priced': int(priced.sum()),
           'priced_N_eq_1': int((priced & n.eq(1)).sum()),
           'priced_N_gt_1': int((priced & n.gt(1)).sum()),
           'priced_no_count': int((priced & (n.isna() | n.lt(1))).sum()),
           'priced_N_nonintegral': int((priced & n.notna() & n.ne(np.floor(n))).sum()),
           'priced_N_in_(1,2)': int((priced & n.gt(1) & n.lt(2)).sum()),
           'priced_N_in_(5,6)': int((priced & n.gt(5) & n.lt(6)).sum()),
           'priced_N_in_(50,51)': int((priced & n.gt(50) & n.lt(51)).sum())}
    t = pd.DataFrame({'ucls': ucls(d['u']), 'u': d['u'], 'band': band(n),
                      'disp': d['Disposition']})[priced]
    out['by_class_band_disp'] = {f'{a}|{b}|{c}': int(v) for (a, b, c), v in
                                 t.groupby(['ucls', 'band', 'disp']).size().items() if v}
    out['by_class_band'] = {f'{a}|{b}': int(v) for (a, b), v in
                            t.groupby(['ucls', 'band']).size().items() if v}
    nm = t[t.ucls.eq('non-metric')]
    out['nonmetric_by_u_band'] = {f'{a}|{b}': int(v) for (a, b), v in
                                  nm.groupby(['u', 'band']).size().items() if v}
    out['disposition_counts'] = {k: int(v) for k, v in d['Disposition'].value_counts().items()}
    return out


def w5_groups():
    d = disp('W5')
    r = raw('W5')
    q = pd.to_numeric(r['c2q2a'], errors='coerce')
    metric = d['Size'].map(lambda s: np.nan if pd.isna(s) else (pp._ng._c8_size_metric_grams(s) or np.nan)).gt(0)
    h = d['Disposition'].eq('w5-quantity-hold')
    out = {'w5_quantity_hold': int(h.sum()),
           'by_u_metric': {f'{a}|{b}': int(v) for (a, b), v in
                           pd.DataFrame({'u': d['u'], 'm': metric})[h].groupby(['u', 'm']).size().items() if v},
           'disposition_counts': {k: int(v) for k, v in d['Disposition'].value_counts().items()},
           'c2q2a_present_priced': int((q.notna() & pd.to_numeric(r['c2q3'], errors='coerce').gt(0)).sum())}
    bc = h & d['u'].eq('Bottle/Can') & metric
    out['bottle_can_metric'] = int(bc.sum())
    out['bottle_can_metric_size_labels'] = {k: int(v) for k, v in d.loc[bc, 'Size'].value_counts().items()}
    # c2q2a vs the coded size quantity on those rows
    sq = d.loc[bc, 'Size'].map(pp._ng._c8_size_metric_grams)
    out['bottle_can_c2q2a_equals_size'] = int((q[bc].sub(sq).abs() < 1e-6).sum())
    return out


def w2_items():
    d = disp('W2')
    r = raw('W2')
    code = pd.to_numeric(r['item_cd'], errors='coerce')
    priced = pd.to_numeric(r['c2q3'], errors='coerce').gt(0)
    out = {}
    for c in (0, 15):
        m = code.eq(c)
        out[str(c)] = {'rows': int(m.sum()), 'priced': int((m & priced).sum()),
                       'disp_by_u': {f'{a}|{b}': int(v) for (a, b), v in
                                     d[m].groupby(['Disposition', 'u']).size().items() if v},
                       'j': {k: int(v) for k, v in d.loc[m, 'j'].value_counts(dropna=False).items()}}
    out['disposition_counts'] = {k: int(v) for k, v in d['Disposition'].value_counts().items()}
    return out


def w3_161():
    out = {}
    for key in ('W3-first', 'W3-second'):
        d = disp(key)
        code = pd.to_numeric(raw(key)['item_cd'], errors='coerce')
        m = code.eq(161)
        out[key] = {'native_label': pp._card_item(pp.pp_source(key)).get(161),
                    'j_by_disp': {f'{a}|{b}': int(v) for (a, b), v in
                                  d[m].groupby(['j', 'Disposition']).size().items() if v}}
    return out


def unknown_units():
    out = {}
    for key in ('W1', 'W2', 'W3-first', 'W3-second', 'W4', 'W5'):
        d = disp(key)
        m = d['u'].eq('Unknown')
        out[key] = {k: int(v) for k, v in d.loc[m, 'Disposition'].value_counts().items()}
    return out


HH_FILES = [
    '2010-11/Data/Post Planting Wave 1/Household/sect7b_plantingw1.dta',
    '2010-11/Data/Post Harvest Wave 1/Household/sect10b_harvestw1.dta',
    '2012-13/Data/Post Planting Wave 2/Household/sect7b_plantingw2.dta',
    '2012-13/Data/Post Harvest Wave 2/Household/sect10b_harvestw2.dta',
    '2015-16/Data/sect7b_plantingw3.dta',
    '2015-16/Data/sect10b_harvestw3.dta',
    '2018-19/Data/sect7b_plantingw4.dta',
    '2018-19/Data/sect10b_harvestw4.dta',
    '2023-24/Data/Post Planting Wave 5/Household/sect6b_plantingw5.dta',
    '2023-24/Data/Post Planting Wave 5/Household/sect7b_plantingw5.dta',
    '2023-24/Data/Post Harvest Wave 5/Household/sect6b_harvestw5.dta',
    '2018-19-NLSS/Data/Household/sect6b_food_cons.dta',
]
PAT = re.compile(r'semo|semov|semol', re.I)


def codebook_sweep():
    out = {}
    extra = sorted(str(p.relative_to(NG))[:-4] for p in NG.glob('*/Data/**/sectc8*.dta.dvc'))
    for rel in HH_FILES + extra:
        entry = {}
        try:
            lab = get_dataframe(str(NG / rel), convert_categoricals=True)
            num = get_dataframe(str(NG / rel), convert_categoricals=False)
        except Exception as e:  # noqa: BLE001
            out[rel] = {'error': f'{type(e).__name__}: {e}'[:300]}
            continue
        entry['rows'] = int(len(lab))
        hits = []
        for col in lab.columns:
            s = lab[col]
            cats = list(s.cat.categories) if isinstance(s.dtype, pd.CategoricalDtype) else []
            strs = [c for c in cats if isinstance(c, str) and PAT.search(c)]
            if not strs and s.dtype == object:
                vals = pd.Series(s.dropna().astype(str).unique())
                strs = [v for v in vals if PAT.search(v)][:10]
            for label in strs:
                m = s.astype('string').eq(label)
                codes = sorted(set(pd.to_numeric(num.loc[m.fillna(False), col], errors='coerce').dropna().astype(int)))[:5] \
                    if col in num else []
                hits.append({'column': col, 'label': label, 'rows': int(m.fillna(False).sum()), 'codes': codes})
        entry['semo_hits'] = hits
        items = [c for c in lab.columns if 'item' in c.lower()]
        for col in items[:2]:
            if col in num:
                m = pd.to_numeric(num[col], errors='coerce').eq(15)
                entry[f'{col}_code15'] = {
                    'rows': int(m.sum()),
                    'labels': {str(k): int(v) for k, v in lab.loc[m, col].astype('string').value_counts().items()}}
        out[rel] = entry
    return out


def quarter_labels():
    out = {}
    for t in ('2010Q3', '2011Q1', '2012Q3', '2013Q1', '2015Q3', '2016Q1', '2018Q3', '2019Q1'):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            lab = pp._ng.harmonized_food_labels(t=t)
        out[t] = {'15': lab.get(15), 'semovita_codes': sorted({k for k, v in lab.items()
                                                               if isinstance(k, int) and v == 'Semovita'})}
    contract = pp._ng.harmonized_food_labels(dirs=[f'{NG / "_"}/'])
    out['contract_15'] = contract.get(15)
    out['contract_semovita'] = sorted(k for k, v in contract.items() if v == 'Semovita')
    return out


def waves():
    import lsms_library as ll
    return list(ll.Country('Nigeria').waves)


rec.step('waves', waves)
rec.step('w1_bands', w1_bands)
rec.step('w5_groups', w5_groups)
rec.step('w2_items', w2_items)
rec.step('w3_161', w3_161)
rec.step('unknown_units', unknown_units)
rec.step('quarter_labels', quarter_labels)
rec.step('codebook_sweep', codebook_sweep)
print('MEASURE DONE', flush=True)
