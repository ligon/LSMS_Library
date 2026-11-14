"""GH #1560: read-only census of the sold-kilogram fields and their codes.

Reads the own sources through lsms_library.local_tools.get_dataframe only and
writes AGGREGATES (counts, sums, quantiles, distinct-value counts) to JSON.  No
raw rows are written.
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library
from lsms_library import paths
from lsms_library.local_tools import get_dataframe
from lsms_library.data_access import get_data_file

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)

root = Path(paths.data_root()).resolve()
assert '/private/' in str(root), root
print('data_root', root, 'lsms_library', lsms_library.__file__, flush=True)

CR = paths.countries_root()


def read(country, wave, name, cats):
    return get_dataframe(str(CR / country / wave / 'Data' / name), convert_categoricals=cats)


def labels(country, wave, name, cols):
    p = get_data_file(f'{country}/{wave}/Data/{name}')
    with pd.io.stata.StataReader(str(p)) as r:
        lab = r.variable_labels()
    return {c: lab.get(c) for c in cols}


def num(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def status(x, codes):
    x = num(x)
    out = pd.Series('positive', index=x.index)
    out[x.isna()] = 'NA'
    out[x.eq(0)] = 'zero'
    out[x.isin(codes)] = 'code'
    return out


def vc(s, n=40):
    s = pd.Series(s).astype('string').fillna('<NA>')
    return {str(k): int(v) for k, v in s.value_counts().head(n).items()}


def nines(x):
    x = num(x).dropna()
    out = {}
    for v in (9, 99, 999, 9999, 99999, 999999, 9999999, 98, 998, 9998, 99998, 999998, 9999998):
        c = int((x == v).sum())
        if c:
            out[str(v)] = c
    return out


def describe(x):
    x = num(x)
    nn = x.dropna()
    return {'nonmissing': int(nn.size), 'zero': int((nn == 0).sum()),
            'positive': int((nn > 0).sum()), 'sum': float(nn.sum()),
            'max': float(nn.max()) if nn.size else None,
            'q50_pos': float(nn[nn > 0].median()) if (nn > 0).any() else None,
            'q99_pos': float(nn[nn > 0].quantile(0.99)) if (nn > 0).any() else None,
            'nines': nines(x),
            'top_values': {str(k): int(v) for k, v in nn.value_counts().head(15).items()}}


def near(x, lo, hi):
    x = num(x)
    return int(((x >= lo) & (x <= hi)).sum())


def code_profile(frame, lab, kg, q, u, v, codes, qcodes, vcodes, harvest_kg=None, flag=None):
    """What sits beside each code value of the kg field."""
    k = num(frame[kg])
    res = {}
    for code in codes:
        m = k.eq(code)
        if not m.any():
            continue
        sub = frame[m]
        prof = {'rows': int(m.sum()),
                'sold_qty_status': vc(status(sub[q], qcodes)),
                'value_status': vc(status(sub[v], vcodes)),
                'sold_unit': vc(lab.loc[sub.index, u])}
        if flag is not None:
            prof['sold_flag'] = vc(lab.loc[sub.index, flag])
        qq = num(sub[q])
        pos = qq.gt(0) & ~qq.isin(qcodes)
        prof['implied_kg_per_unit_if_total'] = sorted(float(x) for x in (code / qq[pos]).round(1))
        if harvest_kg is not None:
            hk = num(harvest_kg.reindex(sub.index))
            prof['harvest_kg_beside'] = {
                'harvest_kg_na_or_code': int((hk.isna() | hk.isin((999999, 99999, 9999, 999998))).sum()),
                'sold_code_gt_harvest_kg': int((hk.notna() & ~hk.isin((999999, 99999, 9999, 999998)) & (hk < code)).sum()),
                'sold_code_le_harvest_kg': int((hk.notna() & ~hk.isin((999999, 99999, 9999, 999998)) & (hk >= code)).sum()),
                'harvest_kg_eq_code': int(hk.eq(code).sum()),
            }
        res[str(code)] = prof
    return res


def zero_profile(frame, kg, q, v, qcodes, vcodes):
    k = num(frame[kg])
    z = k.eq(0)
    sub = frame[z]
    return {'rows': int(z.sum()),
            'sold_qty_status': vc(status(sub[q], qcodes)),
            'value_status': vc(status(sub[v], vcodes))}


def kg_unit_control(frame, lab, kg, q, u, kg_labels, qcodes, kgcodes):
    k, qq = num(frame[kg]), num(frame[q])
    isk = lab[u].astype('string').str.strip().str.lower().isin(kg_labels).fillna(False)
    ok = isk & qq.gt(0) & ~qq.isin(qcodes) & k.gt(0) & ~k.isin(kgcodes)
    return {'kg_unit_rows': int(ok.sum()), 'kg_eq_qty': int((k[ok] == qq[ok]).sum()),
            'kg_eq_1': int((k[ok] == 1).sum()), 'qty_ne_1': int((qq[ok] != 1).sum())}


def ratio_by_unit(frame, lab, kg, q, u, qcodes, kgcodes, n=12):
    k, qq = num(frame[kg]), num(frame[q])
    ok = qq.gt(0) & ~qq.isin(qcodes) & k.gt(0) & ~k.isin(kgcodes)
    r = (k / qq)[ok]
    g = pd.DataFrame({'u': lab.loc[ok[ok].index, u].astype('string').values, 'r': r.values,
                      'q': qq[ok].values, 'k': k[ok].values})
    out = {}
    for unit, sub in g.groupby('u'):
        if len(sub) < 5:
            continue
        lq = np.log(sub['q']); lk = np.log(sub['k'])
        slope = float(np.polyfit(lq, lk, 1)[0]) if lq.nunique() > 2 else None
        out[str(unit)] = {'n': int(len(sub)), 'kg_per_q_median': float(sub['r'].median()),
                          'kg_per_q_p10': float(sub['r'].quantile(.1)),
                          'kg_per_q_p90': float(sub['r'].quantile(.9)),
                          'loglog_slope_kg_on_q': slope}
    return dict(sorted(out.items(), key=lambda kv: -kv[1]['n'])[:n])


report = {}

# ---------------- Niger 2011-12 rainy -------------------------------------
W = '2011-12'
f = 'NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta'
n = read('Niger', W, f, False)
l = read('Niger', W, f, True)
cols = [c for c in n.columns if c.startswith('as02eq1') or c.startswith('as02eq07')]
rep = {'labels': labels('Niger', W, f, cols), 'rows': int(len(n))}
rep['as02eq12c'] = describe(n['as02eq12c'])
rep['as02eq12c_near'] = {'5000-9998': near(n['as02eq12c'], 5000, 9998),
                         '9999': near(n['as02eq12c'], 9999, 9999),
                         '10000-99998': near(n['as02eq12c'], 10000, 99998)}
flag = 'as02eq11' if 'as02eq11' in n.columns else None
rep['codes'] = code_profile(n, l, 'as02eq12c', 'as02eq12a', 'as02eq12b', 'as02eq13',
                            (9999, 99999, 999999), (999999,), (9999999,),
                            harvest_kg=n['as02eq07c'], flag=flag)
rep['zeros'] = zero_profile(n, 'as02eq12c', 'as02eq12a', 'as02eq13', (999999,), (9999999,))
rep['kg_unit_control'] = kg_unit_control(n, l, 'as02eq12c', 'as02eq12a', 'as02eq12b',
                                         {'kg', 'kilogramme', 'kilogrammes'}, (999999,), (9999, 999999))
rep['ratio_by_sold_unit'] = ratio_by_unit(n, l, 'as02eq12c', 'as02eq12a', 'as02eq12b',
                                          (999999,), (9999, 999999))
k = num(n['as02eq12c']); hk = num(n['as02eq07c'])
ok = k.gt(0) & ~k.isin((9999,)) & hk.gt(0) & ~hk.isin((999999,))
rep['sold_vs_harvest_kg_noncode'] = {'pairs': int(ok.sum()), 'sold_gt_harvest': int((k > hk)[ok].sum()),
                                     'sold_eq_harvest': int((k == hk)[ok].sum())}
qa = num(n['as02eq12a']); vv = num(n['as02eq13'])
sale = (qa.gt(0) & ~qa.isin((999999,))) | (vv.gt(0) & ~vv.isin((9999999,)))
rep['kg_positive_not_sale'] = {'rows': int((k.gt(0) & ~sale).sum()),
                               'of_which_9999': int((k.eq(9999) & ~sale).sum()),
                               'kg_sum_excl_9999': float(k[k.gt(0) & ~sale & ~k.eq(9999)].sum())}
rep['sale_records'] = int(sale.sum())
rep['sale_with_kg_positive_noncode'] = int((sale & k.gt(0) & ~k.eq(9999)).sum())
report['Niger 2011-12 rainy as02eq12c'] = rep
print('rainy done', flush=True)

# ---------------- Niger 2011-12 dry ---------------------------------------
f = 'NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta'
n = read('Niger', W, f, False)
l = read('Niger', W, f, True)
keep = ~n.duplicated(keep='first')
n, l = n[keep], l[keep]
cols = [c for c in n.columns if c.startswith('as03eq')]
rep = {'labels': labels('Niger', W, f, cols), 'rows_after_dedup': int(len(n))}
rep['as03eq11c'] = describe(n['as03eq11c'])
rep['as03eq11c_near'] = {'5000-99998': near(n['as03eq11c'], 5000, 99998),
                         '99999': near(n['as03eq11c'], 99999, 99999)}
rep['codes'] = code_profile(n, l, 'as03eq11c', 'as03eq11a', 'as03eq11b', 'as03eq12',
                            (9999, 99999, 999999), (99999,), (9999999,),
                            harvest_kg=n['as03eq09c'], flag='as03eq10')
rep['zeros'] = zero_profile(n, 'as03eq11c', 'as03eq11a', 'as03eq12', (99999,), (9999999,))
rep['kg_unit_control'] = kg_unit_control(n, l, 'as03eq11c', 'as03eq11a', 'as03eq11b',
                                         {'kg', 'kilogramme', 'kilogrammes'}, (99999,), (99999,))
rep['ratio_by_sold_unit'] = ratio_by_unit(n, l, 'as03eq11c', 'as03eq11a', 'as03eq11b',
                                          (99999,), (99999,))
k = num(n['as03eq11c']); qa = num(n['as03eq11a']); vv = num(n['as03eq12'])
sale = (qa.gt(0) & ~qa.isin((99999,))) | (vv.gt(0) & ~vv.isin((9999999,)))
cyc = num(n['as03eq06'])
rep['kg_positive_not_sale'] = {'rows': int((k.gt(0) & ~sale & ~k.eq(99999)).sum()),
                               'kg_sum': float(k[k.gt(0) & ~sale & ~k.eq(99999)].sum()),
                               'sold_flag': vc(l.loc[(k.gt(0) & ~sale & ~k.eq(99999))[lambda s: s].index, 'as03eq10'])}
rep['sale_records'] = int(sale.sum())
rep['sale_multi_cycle'] = int((sale & cyc.gt(1)).sum())
rep['sale_with_kg_positive_noncode'] = int((sale & k.gt(0) & ~k.eq(99999)).sum())
hk = num(n['as03eq09c'])
ok = k.gt(0) & ~k.eq(99999) & hk.gt(0) & ~hk.eq(99999)
rep['sold_vs_harvest_kg_noncode'] = {'pairs': int(ok.sum()), 'sold_gt_harvest': int((k > hk)[ok].sum()),
                                     'sold_eq_harvest': int((k == hk)[ok].sum())}
report['Niger 2011-12 dry as03eq11c'] = rep
print('dry done', flush=True)

# ---------------- Niger 2014-15 -------------------------------------------
W = '2014-15'
f = 'NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E2P2.dta'
n = read('Niger', W, f, False)
l = read('Niger', W, f, True)
cols = [c for c in n.columns if c.startswith('AS02EQ1')]
rep = {'labels': labels('Niger', W, f, cols), 'rows': int(len(n))}
rep['AS02EQ12C'] = describe(n['AS02EQ12C'])
rep['AS02EQ12C_near'] = {'5000-9998': near(n['AS02EQ12C'], 5000, 9998),
                         '9999': near(n['AS02EQ12C'], 9999, 9999)}
rep['AS02EQ110E'] = describe(n['AS02EQ110E']) if 'AS02EQ110E' in n.columns else None
flag = 'AS02EQ11' if 'AS02EQ11' in n.columns else None
rep['codes'] = code_profile(n, l, 'AS02EQ12C', 'AS02EQ12A', 'AS02EQ12B', 'AS02EQ13',
                            (9999, 99999, 999999), (9999,), (9999999,),
                            harvest_kg=n['AS02EQ110E'] if 'AS02EQ110E' in n.columns else None,
                            flag=flag)
rep['zeros'] = zero_profile(n, 'AS02EQ12C', 'AS02EQ12A', 'AS02EQ13', (9999,), (9999999,))
rep['kg_unit_control'] = kg_unit_control(n, l, 'AS02EQ12C', 'AS02EQ12A', 'AS02EQ12B',
                                         {'kg', 'kilogramme', 'kilogrammes'}, (9999,), (9999,))
rep['ratio_by_sold_unit'] = ratio_by_unit(n, l, 'AS02EQ12C', 'AS02EQ12A', 'AS02EQ12B',
                                          (9999,), (9999,))
k = num(n['AS02EQ12C']); qa = num(n['AS02EQ12A']); vv = num(n['AS02EQ13'])
sale = (qa.gt(0) & ~qa.isin((9999,))) | (vv.gt(0) & ~vv.isin((9999999,)))
rep['kg_positive_not_sale'] = {'rows': int((k.gt(0) & ~sale).sum()),
                               'of_which_9999': int((k.eq(9999) & ~sale).sum())}
rep['sale_records'] = int(sale.sum())
rep['sale_with_kg_positive_noncode'] = int((sale & k.gt(0) & ~k.eq(9999)).sum())
hk = num(n['AS02EQ110E'])
ok = k.gt(0) & ~k.eq(9999) & hk.gt(0) & ~hk.isin((9999, 9998))
rep['sold_vs_crop_harvest_kg_noncode'] = {'pairs': int(ok.sum()), 'sold_gt_harvest': int((k > hk)[ok].sum()),
                                          'sold_eq_harvest': int((k == hk)[ok].sum())}
report['Niger 2014-15 AS02EQ12C'] = rep
print('2014 done', flush=True)

# ---------------- EHCVM 2021-22 -------------------------------------------
for country, name in (('Niger', 's16d_me_ner2021.dta'), ('Mali', 's16d_me_mli2021.dta')):
    W = '2021-22'
    n = read(country, W, name, False)
    l = read(country, W, name, True)
    rep = {'rows_raw': int(len(n))}
    if country == 'Niger':
        keep = ~n.duplicated(keep='first')
        n, l = n[keep], l[keep]
    rep['rows'] = int(len(n))
    cols = [c for c in n.columns if c.startswith('s16dq0')]
    rep['labels'] = labels(country, W, name, cols)
    rep['s16dq05c'] = describe(n['s16dq05c'])
    rep['codes'] = code_profile(n, l, 's16dq05c', 's16dq05a', 's16dq05b', 's16dq06',
                                (9999, 99999, 999999), (), (), flag='s16dq04')
    rep['zeros'] = zero_profile(n, 's16dq05c', 's16dq05a', 's16dq06', (), ())
    rep['kg_unit_control'] = kg_unit_control(n, l, 's16dq05c', 's16dq05a', 's16dq05b',
                                             {'kg', 'kilogramme', 'kilogrammes'}, (), ())
    rep['ratio_by_sold_unit'] = ratio_by_unit(n, l, 's16dq05c', 's16dq05a', 's16dq05b', (), ())
    k = num(n['s16dq05c']); qa = num(n['s16dq05a']); vv = num(n['s16dq06'])
    sale = qa.gt(0) | vv.gt(0)
    rep['kg_positive_not_sale'] = int((k.gt(0) & ~sale).sum())
    rep['sale_records'] = int(sale.sum())
    rep['sale_with_kg_positive'] = int((sale & k.gt(0)).sum())
    rep['sale_kg_sum'] = float(k[sale & k.gt(0)].sum())
    report[f'{country} 2021-22 s16dq05c'] = rep
    print(country, '2021 done', flush=True)

(OUT / 'raw_codes.json').write_text(json.dumps(report, indent=1, ensure_ascii=False, default=str))
print('wrote', OUT / 'raw_codes.json')
