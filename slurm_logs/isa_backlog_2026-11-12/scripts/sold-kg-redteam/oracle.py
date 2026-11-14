"""Source oracle for the sold-kg candidate: counts and aggregates only, no rows.

usage: oracle.py <clone(cand)> <out_json>
Reads sources only through the library (get_data_file / get_dataframe) into the
private root.  Prints and stores no household ids and no raw rows.
"""
import os
import re
import sys
import traceback
import warnings
from collections import Counter

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, dump  # noqa: E402

clone, out_json = sys.argv[1:3]
head = assert_env(clone)
import lsms_library as ll  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root()
R = {'head': head}
FILES = {
    'NER11_rainy': 'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
    'NER11_dry': 'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta',
    'NER14_sale': 'Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E2P2.dta',
    'NER21_sale': 'Niger/2021-22/Data/s16d_me_ner2021.dta',
    'MLI21_sale': 'Mali/2021-22/Data/s16d_me_mli2021.dta',
}
LABEL_PAT = {
    'NER11_rainy': r'^as02eq(0|0[0-9]|1[0-3])[a-z]?$',
    'NER11_dry': r'^as03eq',
    'NER14_sale': r'.',
    'NER21_sale': r'^s16dq',
    'MLI21_sale': r'^s16dq',
}


def save():
    dump(R, out_json)


def num(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def lab(s):
    return s.astype('string').str.strip()


def guard(name):
    def deco(fn):
        def run():
            try:
                R[name] = fn()
            except Exception as e:  # noqa: BLE001
                R[name] = {'error': f'{type(e).__name__}: {str(e)[:400]}',
                           'tb': traceback.format_exc()[-1500:]}
            print(name, 'done', flush=True)
            save()
        return run
    return deco


def read(key, cats):
    return get_dataframe(str(ROOT / FILES[key]), convert_categoricals=cats)


def unit_stats(kg, q, unit, mask):
    out = {}
    d = pd.DataFrame({'kg': kg, 'q': q, 'u': unit})[mask & kg.notna() & q.gt(0)]
    for u, g in d.groupby('u', dropna=False):
        if len(g) < 5:
            continue
        r = g['kg'] / g['q']
        out[str(u)] = dict(n=int(len(g)), kg_eq_q=int((g['kg'] == g['q']).sum()),
                           kg_eq_1=int((g['kg'] == 1).sum()),
                           median_kg_per_unit=float(r.median()),
                           p10=float(r.quantile(.1)), p90=float(r.quantile(.9)))
        pos = g[(g['kg'] > 0)]
        if len(pos) >= 10 and pos['q'].nunique() > 2:
            out[str(u)]['loglog_slope'] = float(np.polyfit(np.log(pos['q']), np.log(pos['kg']), 1)[0])
    return out


@guard('labels')
def labels():
    import pyreadstat
    out = {}
    for key, rel in FILES.items():
        p = get_data_file(rel)
        if p is None:
            out[key] = 'UNAVAILABLE'
            continue
        _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
        pat = re.compile(LABEL_PAT[key])
        out[key] = {
            'n_rows_meta': meta.number_rows,
            'labels': {v: l for v, l in meta.column_names_to_labels.items() if pat.search(v)},
            'value_label_sets': {v: meta.variable_to_label.get(v) for v in meta.column_names
                                 if pat.search(v) and meta.variable_to_label.get(v)},
            'in_data_root': str(p).startswith(os.environ['LSMS_DATA_DIR']),
        }
    return out


@guard('NER11_rainy')
def rainy():
    n, c = read('NER11_rainy', False), read('NER11_rainy', True)
    kg, q, v = num(n['as02eq12c']), num(n['as02eq12a']), num(n['as02eq13'])
    hk = num(n['as02eq07c'])
    qm, vm, kgm = q.where(q != 999999), v.where(v != 9999999), kg.where(kg != 9999)
    hkm = hk.where(hk != 999999)
    sale = qm.gt(0) | vm.gt(0)
    unit = lab(c['as02eq12b'])
    o = dict(rows=len(n), kg_notna=int(kg.notna().sum()), kg_eq_9999=int((kg == 9999).sum()),
             kg_5000_9998=int(((kg >= 5000) & (kg < 9999)).sum()), kg_gt_9999=int((kg > 9999).sum()),
             kg_max=float(kg.max()), kg_eq_0=int((kg == 0).sum()),
             kg_eq_99999=int((kg == 99999).sum()), kg_eq_999999=int((kg == 999999).sum()),
             sales=int(sale.sum()), sale_with_kg=int((sale & kgm.notna()).sum()),
             sale_without_kg=int((sale & kgm.isna()).sum()),
             kg_on_nonsale=int((kgm.notna() & ~sale).sum()),
             sold_unit_counts={str(k): int(x) for k, x in unit[sale].value_counts(dropna=False).head(15).items()})
    m9 = kg == 9999
    o['code9999'] = dict(
        q_coded=int((m9 & (q == 999999)).sum()), v_coded=int((m9 & (v == 9999999)).sum()),
        q_pos=int((m9 & qm.gt(0)).sum()),
        units={str(k): int(x) for k, x in unit[m9].value_counts(dropna=False).items()},
        beside_harvest_kg=int((m9 & hkm.notna()).sum()),
        exceeds_line_harvest_kg=int((m9 & hkm.notna() & (9999 > hkm)).sum()))
    others = kgm.gt(0) & hkm.gt(0)
    o['others_exceed_line_harvest_kg'] = [int((others & (kgm > hkm)).sum()), int(others.sum())]
    o['kg_eq_line_harvest_kg'] = int((others & (kgm == hkm)).sum())
    o['unit_stats'] = unit_stats(kgm, qm, unit, sale)
    # line-level sale repeated across a crop's lines?
    d = pd.DataFrame({'h': n['hid'], 'crop': n['as02eq06'], 'q': qm, 'v': vm, 'kg': kgm, 's': sale})
    d = d[d['s']]
    g = d.groupby(['h', 'crop'])
    multi = g.size()
    multi = multi[multi > 1]
    same = 0
    for key in multi.index:
        sub = d.loc[g.groups[key], ['q', 'v', 'kg']].fillna(-1)
        if len(sub.drop_duplicates()) == 1:
            same += 1
    o['crop_pairs_with_2plus_sale_lines'] = int(len(multi))
    o['of_which_identical_triple_on_all_lines'] = int(same)
    return o


@guard('NER11_dry')
def dry():
    n, c = read('NER11_dry', False), read('NER11_dry', True)
    dup = n.duplicated(keep='first')
    n, c = n[~dup], c[~dup]
    kg, q, v = num(n['as03eq11c']), num(n['as03eq11a']), num(n['as03eq12'])
    cyc, hkc, hqc = num(n['as03eq06']), num(n['as03eq09c']), num(n['as03eq09a'])
    qm, vm, kgm = q.where(q != 99999), v.where(v != 9999999), kg.where(kg != 99999)
    hkcm = hkc.where(hkc != 99999)
    hqcm = hqc.where(hqc != 99999)
    sale = qm.gt(0) | vm.gt(0)
    sunit, hunit = lab(c['as03eq11b']), lab(c['as03eq09b'])
    flag = lab(c['as03eq10'])
    o = dict(rows_after_dedup=int(len(n)), dup_dropped=int(dup.sum()),
             kg_notna=int(kg.notna().sum()), kg_eq_99999=int((kg == 99999).sum()),
             kg_eq_9999=int((kg == 9999).sum()), kg_eq_999999=int((kg == 999999).sum()),
             kg_max_after_code=float(kgm.max()),
             kg_eq_0=int((kg == 0).sum()), kg_pos=int(kgm.gt(0).sum()),
             code_beside_harvest_kg_code=int(((kg == 99999) & (hkc == 99999)).sum()),
             zero_beside_pos_q=int((kgm.eq(0) & qm.gt(0)).sum()),
             zero_beside_pos_q_and_pos_v=int((kgm.eq(0) & qm.gt(0) & vm.gt(0)).sum()),
             zero_q_not_pos_but_v_pos=int((kgm.eq(0) & ~qm.gt(0) & vm.gt(0)).sum()),
             zero_no_sale=int((kgm.eq(0) & ~sale).sum()),
             sales=int(sale.sum()), sale_with_kg_pos=int((sale & kgm.gt(0)).sum()))
    ns = kgm.notna() & ~sale
    o['no_sale'] = dict(
        n=int(ns.sum()), kg_sum=float(kgm[ns].sum()),
        flag={str(k): int(x) for k, x in flag[ns].value_counts(dropna=False).items()},
        q_na=int((ns & q.isna()).sum()), q_zero=int((ns & (q == 0)).sum()), q_coded=int((ns & (q == 99999)).sum()),
        v_na=int((ns & v.isna()).sum()), v_zero=int((ns & (v == 0)).sum()), v_coded=int((ns & (v == 9999999)).sum()),
        kg_eq_harvest_kg_per_cycle=int((ns & (kgm == hkcm)).sum()),
        cycles={str(k): int(x) for k, x in cyc[ns].value_counts(dropna=False).items()},
        sold_unit_na=int((ns & sunit.isna()).sum()),
        kg_zero=int((ns & kgm.eq(0)).sum()))
    # Any record with a CODED quantity or value and kg (a sale of unknown size)?
    o['coded_q_or_v_with_kg'] = int((kgm.notna() & ((q == 99999) | (v == 9999999))).sum())
    o['unit_stats_all'] = unit_stats(kgm, qm, sunit, sale)
    s = sale & kgm.gt(0)
    for name, m in (('multi_cycle', s & cyc.gt(1)), ('single_cycle', s & cyc.eq(1))):
        both = m & hkcm.gt(0)
        same_unit = m & (sunit == hunit).fillna(False) & hqcm.gt(0) & qm.gt(0)
        o[name] = dict(
            n=int(m.sum()), cycles={str(k): int(x) for k, x in cyc[m].value_counts().items()},
            with_harvest_kg=int(both.sum()),
            kg_le_per_cycle_harvest_kg=int((both & (kgm <= hkcm)).sum()),
            kg_gt_per_cycle_le_total=int((both & (kgm > hkcm) & (kgm <= hkcm * cyc)).sum()),
            kg_gt_total=int((both & (kgm > hkcm * cyc)).sum()),
            same_unit_n=int(same_unit.sum()),
            q_le_per_cycle_harvest_q=int((same_unit & (qm <= hqcm)).sum()),
            q_gt_per_cycle_le_total=int((same_unit & (qm > hqcm) & (qm <= hqcm * cyc)).sum()),
            q_gt_total=int((same_unit & (qm > hqcm * cyc)).sum()),
            kg_eq_q_in_kg_unit=[int((m & sunit.str.lower().eq('kg').fillna(False) & (kgm == qm)).sum()),
                                int((m & sunit.str.lower().eq('kg').fillna(False)).sum())],
            unit_stats=unit_stats(kgm, qm, sunit, m))
    return o


@guard('NER14_sale')
def y2014():
    n, c = read('NER14_sale', False), read('NER14_sale', True)
    keys = ['GRAPPE', 'MENAGE', 'EXTENSION', 'AS02EQ110B']
    kg, q, v = num(n['AS02EQ12C']), num(n['AS02EQ12A']), num(n['AS02EQ13'])
    hk = num(n['AS02EQ110E']) if 'AS02EQ110E' in n.columns else pd.Series(np.nan, index=n.index)
    qm, vm, kgm = q.where(q != 9999), v.where(v != 9999999), kg.where(kg != 9999)
    sale = qm.gt(0) | vm.gt(0)
    unit = lab(c['AS02EQ12B'])
    o = dict(rows=len(n), unique_keys=int(len(n.drop_duplicates(keys))),
             kg_notna=int(kg.notna().sum()), kg_eq_9999=int((kg == 9999).sum()), kg_max=float(kg.max()),
             kg_5000_9998=int(((kg >= 5000) & (kg < 9999)).sum()),
             kg_eq_0=int((kg == 0).sum()), kg_pos=int(kgm.gt(0).sum()),
             zero_beside_pos_q=int((kgm.eq(0) & qm.gt(0)).sum()),
             zero_beside_pos_q_pos_v=int((kgm.eq(0) & qm.gt(0) & vm.gt(0)).sum()),
             zero_not_pos_q=int((kgm.eq(0) & ~qm.gt(0)).sum()),
             zero_not_pos_q_but_pos_v=int((kgm.eq(0) & ~qm.gt(0) & vm.gt(0)).sum()),
             kg_on_nonsale=int((kgm.notna() & ~sale).sum()),
             kg_pos_on_nonsale=int((kgm.gt(0) & ~sale).sum()),
             sales=int(sale.sum()), sale_without_kg=int((sale & kgm.isna()).sum()),
             kg_eq_crop_harvest_kg=[int((kgm.gt(0) & (kgm == hk)).sum()), int((kgm.gt(0) & hk.gt(0)).sum())])
    m9 = kg == 9999
    o['code9999'] = dict(q_coded=int((m9 & (q == 9999)).sum()), v_coded=int((m9 & (v == 9999999)).sum()),
                         q_pos=int((m9 & qm.gt(0)).sum()), beside_harvest_kg_9999=int((m9 & (hk == 9999)).sum()),
                         units={str(k): int(x) for k, x in unit[m9].value_counts(dropna=False).items()})
    o['unit_stats'] = unit_stats(kgm, qm, unit, sale)
    return o


def s16d(key, dedup):
    n, c = read(key, False), read(key, True)
    o = {'rows_raw': int(len(n))}
    if dedup:
        dup = n.duplicated(keep='first')
        o['exact_dups_dropped'] = int(dup.sum())
        n, c = n[~dup], c[~dup]
    keys = ['grappe', 'menage', 's16dq01']
    kg, q, v = num(n['s16dq05c']), num(n['s16dq05a']), num(n['s16dq06'])
    sale = q.gt(0) | v.gt(0)
    unit = lab(c['s16dq05b'])
    o.update(rows=int(len(n)), unique_keys=int(len(n.drop_duplicates(keys))),
             kg_notna=int(kg.notna().sum()), kg_eq_0=int((kg == 0).sum()), kg_pos=int(kg.gt(0).sum()),
             kg_max=float(kg.max()), kg_ge_9999=int((kg >= 9999).sum()),
             kg_allnines=int(kg.isin([99, 999, 9999, 99999, 999999, 9999999, 99999999]).sum()),
             zero_beside_pos_q=int((kg.eq(0) & q.gt(0)).sum()),
             kg_on_nonsale=int((kg.notna() & ~sale).sum()), sales=int(sale.sum()),
             sale_without_kg=int((sale & kg.isna()).sum()))
    d = pd.DataFrame({k: n[k] for k in keys})
    d['kg'] = kg.notna()
    d['sale'] = sale
    g = d.groupby(keys)
    size = g.size()
    nkg = g['kg'].sum()
    nsale = g['sale'].sum()
    o['pairs'] = int(len(size))
    o['pairs_multi_row'] = int((size > 1).sum())
    o['pairs_with_gt1_kg_row'] = int((nkg > 1).sum())
    o['pairs_with_gt1_sale_row'] = int((nsale > 1).sum())
    o['multi_row_pairs_with_kg'] = int(((size > 1) & (nkg > 0)).sum())
    o['unit_stats'] = unit_stats(kg, q, unit, sale)
    return o, n, c


@guard('NER21_sale')
def ner21():
    return s16d('NER21_sale', True)[0]


@guard('MLI21_sale')
def mli21():
    return s16d('MLI21_sale', False)[0]


def triples(q, kg, v):
    f = lambda x: None if pd.isna(x) else round(float(x), 6)  # noqa: E731
    return Counter((f(a), f(b), f(c)) for a, b, c in zip(q, kg, v))


def cmp_counter(a, b):
    return dict(src=int(sum(a.values())), served=int(sum(b.values())),
                equal=a == b, src_minus_served=int(sum((a - b).values())),
                served_minus_src=int(sum((b - a).values())))


SERVED = {}


def served(country):
    if country not in SERVED:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            SERVED[country] = ll.Country(country).crop_production()
    return SERVED[country]


@guard('triples')
def triple_check():
    out = {}
    nd = served('Niger').reset_index()
    md = served('Mali').reset_index()
    for d in (nd, md):
        for col in ('Quantity_sold', 'Quantity_sold_kg', 'Value_sold'):
            d[col] = num(d[col])
    farm = nd['plot_id'].astype('string').eq('Farm-level').fillna(False).astype(bool)

    # 2011-12 rainy
    n = read('NER11_rainy', False)
    kg, q, v = num(n['as02eq12c']), num(n['as02eq12a']), num(n['as02eq13'])
    qm, vm, kgm = q.where(q != 999999), v.where(v != 9999999), kg.where(kg != 9999)
    kgm = kgm.mask(kgm.eq(0) & qm.gt(0))
    m = kgm.notna()
    s = nd[(nd.t == '2011-12') & ~farm & nd.Quantity_sold_kg.notna()]
    out['2011-12_rainy'] = cmp_counter(triples(qm[m], kgm[m], vm[m]),
                                       triples(s.Quantity_sold, s.Quantity_sold_kg, s.Value_sold))
    # 2011-12 dry
    n = read('NER11_dry', False)
    n = n[~n.duplicated(keep='first')]
    kg, q, v = num(n['as03eq11c']), num(n['as03eq11a']), num(n['as03eq12'])
    qm, vm, kgm = q.where(q != 99999), v.where(v != 9999999), kg.where(kg != 99999)
    kgm = kgm.mask(kgm.eq(0) & qm.gt(0)).where(qm.gt(0) | vm.gt(0))
    m = kgm.notna()
    s = nd[(nd.t == '2011-12') & farm & nd.Quantity_sold_kg.notna()]
    out['2011-12_dry'] = cmp_counter(triples(qm[m], kgm[m], vm[m]),
                                     triples(s.Quantity_sold, s.Quantity_sold_kg, s.Value_sold))
    # 2014-15
    n = read('NER14_sale', False)
    kg, q, v = num(n['AS02EQ12C']), num(n['AS02EQ12A']), num(n['AS02EQ13'])
    qm, vm, kgm = q.where(q != 9999), v.where(v != 9999999), kg.where(kg != 9999)
    kgm = kgm.mask(kgm.eq(0) & qm.gt(0))
    m = kgm.notna()
    s = nd[(nd.t == '2014-15') & nd.Quantity_sold_kg.notna()]
    out['2014-15'] = cmp_counter(triples(qm[m], kgm[m], vm[m]),
                                 triples(s.Quantity_sold, s.Quantity_sold_kg, s.Value_sold))
    out['2014-15_src_kg_rows_by_sale'] = dict(
        sale=int((m & (qm.gt(0) | vm.gt(0))).sum()), nonsale=int((m & ~(qm.gt(0) | vm.gt(0))).sum()))
    # 2021-22 Niger
    n = read('NER21_sale', False)
    n = n[~n.duplicated(keep='first')]
    kg, q, v = num(n['s16dq05c']), num(n['s16dq05a']), num(n['s16dq06'])
    kgm = kg.mask(kg.eq(0) & q.gt(0))
    m = kgm.notna()
    s = nd[(nd.t == '2021-22') & nd.Quantity_sold_kg.notna()]
    out['Niger_2021-22'] = cmp_counter(triples(q[m], kgm[m], v[m]),
                                       triples(s.Quantity_sold, s.Quantity_sold_kg, s.Value_sold))
    # Mali 2021-22
    n = read('MLI21_sale', False)
    kg, q, v = num(n['s16dq05c']), num(n['s16dq05a']), num(n['s16dq06'])
    sale = q.gt(0) | v.gt(0)
    kgm = kg.mask(kg.eq(0) & q.gt(0))
    m = kgm.notna() & sale
    s = md[(md.t == '2021-22') & md.Quantity_sold_kg.notna()]
    out['Mali_2021-22'] = cmp_counter(triples(q[m], kgm[m], v[m]),
                                      triples(s.Quantity_sold, s.Quantity_sold_kg, s.Value_sold))
    # Every served sale row's (q, v) has its own kg: served sale rows with kg missing
    for name, d in (('Niger', nd), ('Mali', md)):
        sale = d.Quantity_sold.fillna(0).gt(0) | d.Value_sold.fillna(0).gt(0)
        out[f'{name}_sale_rows_without_kg_by_t'] = {
            str(t): int(x) for t, x in (sale & d.Quantity_sold_kg.isna()).groupby(d.t).sum().items()}
    return out


def keyset(x):
    if x is None or (not isinstance(x, str) and pd.isna(x)):
        return frozenset()
    return frozenset(k for k in str(x).split('+') if k)


def skeys(frame, on):
    frame = frame.copy()
    for col in on:
        frame[col] = frame[col].astype(str)
    return frame


@guard("inputs_join")
def inputs_join():
    out = {}
    for country in ('Niger', 'Mali'):
        df = served(country)
        c = ll.Country(country)
        d = df.reset_index()
        der = d['Derivation'].map(keyset)
        est = f'{country}::crop_production::sold-kg-estimate'
        keyed = d[der.map(lambda s: est in s).astype(bool)]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            inp = c.derivation_inputs(est).reset_index()
        on = ['t', 'i', 'crop']
        keyed, inp = skeys(keyed, on), skeys(inp, on)
        j = keyed.merge(inp, on=on, how='outer', indicator=True, suffixes=('', '_inp'))
        both = j[j['_merge'] == 'both']
        out[f'{country}_sold_kg_estimate'] = dict(
            inputs=int(len(inp)), inputs_unique_keys=int(len(inp.drop_duplicates(on))),
            served_keyed=int(len(keyed)), both=int(len(both)),
            served_only=int((j['_merge'] == 'left_only').sum()),
            inputs_only=int((j['_merge'] == 'right_only').sum()),
            value_mismatch=int((num(both['Quantity_sold_kg']) != num(both['s16dq05c'])).sum()),
            input_columns=[str(x) for x in inp.columns])
        # the attachment key's inputs now carry the kg, and still join
        hh = f'{country}::crop_production::hh-crop-sale-single-row'
        for wave in (('2014-15', '2021-22') if country == 'Niger' else ('2021-22',)):
            kgcol = 'AS02EQ12C' if wave == '2014-15' else 's16dq05c'
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                hin = c.derivation_inputs(hh, wave=wave).reset_index()
            hk = d[(d.t == wave) & der.map(lambda s: hh in s).astype(bool)]
            hk, hin = skeys(hk, on), skeys(hin, on)
            jj = hk.merge(hin, on=on, how='outer', indicator=True, suffixes=('', '_inp'))
            b = jj[jj['_merge'] == 'both']
            raw = num(b[kgcol]) if kgcol in b.columns else pd.Series(np.nan, index=b.index)
            if wave == '2014-15':
                raw = raw.where(raw != 9999)
            raw = raw.mask(raw.eq(0) & num(b['Quantity_sold']).gt(0))
            srv = num(b['Quantity_sold_kg'])
            out[f'{country}_{wave}_hh_crop_sale'] = dict(
                inputs=int(len(hin)), served_keyed=int(len(hk)), both=int(len(b)),
                served_only=int((jj['_merge'] == 'left_only').sum()),
                inputs_only=int((jj['_merge'] == 'right_only').sum()),
                has_kg_column=kgcol in hin.columns,
                kg_mismatch=int(((srv != raw) & ~(srv.isna() & raw.isna())).sum()))
        attrs = df.attrs.get('derivations')
        out[f'{country}_attrs_derivations'] = (
            {k: v for k, v in attrs.items() if 'sold' in str(k) or 'sale' in str(k)}
            if isinstance(attrs, dict) else repr(attrs)[:1500])
    # Niger no-sale mask
    c = ll.Country('Niger')
    d = served('Niger').reset_index()
    ns = 'Niger::crop_production::dry-season-sold-kg-no-sale'
    keyed = d[d['Derivation'].map(keyset).map(lambda s: ns in s).astype(bool)]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        inp = c.derivation_inputs(ns).reset_index()
    on = ['t', 'i', 'plot_id', 'production_obs']
    keyed, inp = skeys(keyed, on), skeys(inp, on)
    j = keyed.merge(inp, on=on, how='outer', indicator=True, suffixes=('', '_inp'))
    out['Niger_no_sale'] = dict(inputs=int(len(inp)), served_keyed=int(len(keyed)),
                                both=int((j['_merge'] == 'both').sum()),
                                served_keyed_kg_all_na=bool(keyed['Quantity_sold_kg'].isna().all()),
                                inputs_kg_sum=float(num(inp['as03eq11c']).sum()))
    return out


@guard('siteq_variants')
def siteq_variants():
    from lsms_library import quantity_audit as qa
    out = {}
    tails = (qa._TAILS['high'], qa._TAILS['low'])
    for country in ('Niger', 'Mali'):
        d = served(country).reset_index()
        vals = qa._numeric(d['Quantity_sold_kg'])
        ax = {a: qa._axis(d, a) for a in ('t', 'u', 'j')}
        ax['Unit_sold'] = qa._axis(d, 'Unit_sold')
        variants = {
            'A_t_harvestu_j (as served)': {'t': ax['t'], 'u': ax['u'], 'j': ax['j']},
            'B_t_Unit_sold_j': {'t': ax['t'], 'u': ax['Unit_sold'], 'j': ax['j']},
            'C_t_j': {'t': ax['t'], 'j': ax['j']},
        }
        pos = np.isfinite(vals) & (vals > 0)
        farm = d['plot_id'].astype('string').eq('Farm-level').fillna(False).to_numpy()
        u = d['u'].astype('string')
        out[country] = {'positive_rows': int(pos.sum()),
                        'positive_on_farm_level': int((pos & farm).sum()),
                        'positive_harvest_u_counts_on_farm_level': {
                            str(k): int(x) for k, x in u[pos & farm].value_counts(dropna=False).items()}}
        for name, keys in variants.items():
            refs, rung = qa._cell_references(vals, keys, tails)
            with np.errstate(divide='ignore', invalid='ignore'):
                high = pos & (vals / refs[0] > qa.QUANTITY_OUTLIER_K)
                low = pos & (refs[1] / vals > qa.QUANTITY_OUTLIER_K)
            offenders = []
            for p in np.flatnonzero(high | low):
                offenders.append(dict(
                    t=str(d['t'].iloc[p]), crop=str(d['crop'].iloc[p]), harvest_u=str(d['u'].iloc[p]),
                    Unit_sold=str(d['Unit_sold'].iloc[p]), farm_level=bool(farm[p]),
                    value=float(vals[p]), tail='high' if high[p] else 'low',
                    ref=float(refs[0][p] if high[p] else refs[1][p]),
                    ratio=round(float(vals[p] / refs[0][p] if high[p] else refs[1][p] / vals[p]), 1),
                    rung=str(rung[p])))
            out[country][name] = dict(
                high=int(high.sum()), low=int(low.sum()),
                unjudged=int((pos & np.isnan(refs[0])).sum()),
                rung_counts={str(k): int(x) for k, x in Counter(rung[pos]).items()},
                offenders=offenders)
    return out


for fn in (labels, rainy, dry, y2014, ner21, mli21, triple_check, inputs_join, siteq_variants):
    fn()
print('ORACLE DONE', flush=True)
