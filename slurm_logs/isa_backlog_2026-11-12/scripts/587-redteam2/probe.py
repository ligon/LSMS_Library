"""GH #587 delta probes on the candidate (warm private root after build.py).

usage: python probe.py <clone> <framesdir> <resultsdir>
Aggregates only (codebook labels and counts); no raw rows are written.
"""
import re
import sys
import warnings
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env, Recorder  # noqa: E402

clone, frames, results = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
head = assert_env(clone)

import importlib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library.local_tools import get_dataframe, df_from_orgfile  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

pp = importlib.import_module('lsms_library.countries.Nigeria._.pp_community_prices')
ng = pp._ng
D = importlib.import_module('lsms_library.derivations')
R = Recorder(results / 'cand.probe.json')
R.data['head'] = head
NG = countries_root() / 'Nigeria'
CM = NG / '_' / 'categorical_mapping.org'
c = ll.Country('Nigeria')
cp = pd.read_parquet(frames / 'cand.community_prices.parquet')
cpr = cp.reset_index()
PP_Q = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']
is_pp = cpr['t'].isin(PP_Q)
K = 'Nigeria::community_prices::'


def vc(s):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).items()}


def strip_prefix(label):
    return re.sub(r'^\s*\d+\s*[.)]?\s*', '', str(label)).strip()


def unit_class(label):
    """My own metric-unit classifier over the NATIVE value label."""
    if label is None or (isinstance(label, float) and np.isnan(label)) or pd.isna(label):
        return 'none'
    b = strip_prefix(label).lower()
    pats = (('kg', r'kilo ?grams?( \(kgs?\))?|kgs?'), ('g', r'grams?( \(g\))?|g|gms?'),
            ('l', r'lit(re|er)s?( \(l\))?|l'), ('cl', r'cent[i]?lit(re|er)s?( \(cl\))?|cl'),
            ('ml', r'milli ?lit(re|er)s?( \(ml\))?|ml'))
    for name, pat in pats:
        if re.fullmatch(pat, b):
            return name
    return 'other'


def read_pair(rel):
    path = str(NG / rel)
    raw_n = get_dataframe(path, convert_categoricals=False)
    try:
        raw_c = get_dataframe(path)
    except Exception as e:  # noqa: BLE001
        raw_c = None
        print('labelled read failed', rel, e, flush=True)
    return raw_n, raw_c


def labels_of(raw_n, raw_c, col):
    """Value labels of `col` (string), from the labelled read; NaN where no code."""
    if raw_c is None or col not in raw_c:
        return None
    s = raw_c[col]
    if pd.api.types.is_numeric_dtype(s):
        return None
    return s.astype('string')


def norm_tuple(row):
    out = []
    for x in row:
        if x is None or (not isinstance(x, str) and pd.isna(x)):
            out.append(None)
        elif isinstance(x, (int, float, np.integer, np.floating)):
            out.append(round(float(x), 6))
        else:
            out.append(str(x).strip())
    return tuple(out)


def multiset(df, cols):
    return Counter(norm_tuple(r) for r in df[cols].itertuples(index=False, name=None))


with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    W1N, W1C = read_pair(pp.pp_source('W1')['source_path'])
    W5N, W5C = read_pair(pp.pp_source('W5')['source_path'])
    W2N, W2C = read_pair(pp.pp_source('W2')['source_path'])
    DISP = {key: pp.pp_dispositions(r, key) for key, r in (('W1', W1N), ('W5', W5N), ('W2', W2N))}

W1COLS = ['state', 'lga', 'ea', 'item_cd', 'sc2q3a', 'sc2q3b', 'sc2q3c']
W1MASK = {}


# ------------------------------------------------------------- W1 bands
def w1_bands():
    ops = pp.pp_source('W1')['operand_columns']
    price = pd.to_numeric(W1N[ops['price']], errors='coerce')
    n = pd.to_numeric(W1N[ops['quantity']], errors='coerce')
    ucode = pd.to_numeric(W1N[ops['unit']], errors='coerce')
    ulab = labels_of(W1N, W1C, ops['unit'])
    out = {'labelled_unit_read': ulab is not None}
    if ulab is None:   # fall back to the sealed card, and say so
        ulab = ucode.map(pp._card(pp.pp_source('W1'), 'unit')).astype('string')
    # a labelled value that is still the bare code means no value label (off-card)
    lab_is_code = ulab.astype('string').str.fullmatch(r'\s*\d+(\.0+)?\s*', na=False)
    offcard = ucode.notna() & (ulab.isna() | lab_is_code)
    cls = pd.Series([unit_class(x) for x in ulab], index=W1N.index)
    cls = cls.where(ucode.notna(), 'none').where(~offcard, 'offcard')
    out['unit_code_label_class'] = {
        f'{int(k)}|{lab}|{cl}': int(v) for (k, lab, cl), v in
        pd.DataFrame({'k': ucode, 'lab': ulab.fillna('<NA>'), 'cl': cls})[price.gt(0) & ucode.notna()]
        .groupby(['k', 'lab', 'cl']).size().items()}
    priced = price.gt(0)
    icode = pd.to_numeric(W1N['item_cd'], errors='coerce')
    imap = pp.pp_item_map('W1')
    item_hold = icode.notna() & ~icode.isin(list(imap))
    exp = pd.Series('served', index=W1N.index, dtype='object')
    decided = pd.Series(False, index=W1N.index)

    def put(name, m):
        nonlocal decided
        m = m.fillna(False).astype(bool) & ~decided
        exp[m] = name
        decided |= m
    gml, kgl = cls.isin(['g', 'ml']), cls.isin(['kg', 'l'])
    put('unpriced', ~priced)
    put('item-hold', item_hold)
    put('unit-unplaced', offcard)
    put('w1-no-count', n.isna() | n.lt(1))
    put('w1-metric-package-hold', gml & n.eq(1))
    put('w1-count-as-size-hold', kgl & n.gt(50))
    put('w1-metric-small-count-hold', gml & n.gt(1) & n.le(5))
    put('w1-kg-mid-count-hold', cls.eq('kg') & n.gt(5) & n.le(50))
    put('w1-nonmetric-count-as-size-hold', cls.eq('other') & n.gt(50))
    put('w1-divided-unvalidated', n.gt(1) & cls.isin(['other', 'none', 'kg']))
    put('w1-divided', n.gt(1))
    d = DISP['W1']
    assert len(d) == len(exp)
    theirs = pd.Series(d['Disposition'].values, index=W1N.index)
    out['expected'] = vc(exp)
    out['theirs'] = vc(theirs)
    out['agree_rows'] = int((exp.values == theirs.values).sum())
    out['n_rows'] = int(len(exp))
    conf = pd.crosstab(exp, theirs)
    out['offdiag'] = {f'{a}->{b}': int(conf.loc[a, b]) for a in conf.index for b in conf.columns
                      if a != b and conf.loc[a, b]}
    band = pd.cut(n, [-np.inf, 0.999999, 1.000001, 5.000001, 50.000001, np.inf],
                  labels=['<1', '1', '2-5', '6-50', '>50']).astype('string').fillna('NaN')
    out['nonintegral_N_priced'] = int((priced & n.notna() & n.ne(np.floor(n))).sum())
    out['N_in_(1,2)_priced'] = int((priced & n.gt(1) & n.lt(2)).sum())
    out['class_band_priced'] = {f'{a}|{b}': int(v) for (a, b), v in
                                pd.DataFrame({'c': cls[priced], 'b': band[priced]})
                                .groupby(['c', 'b']).size().items()}
    out['class_band_disposition'] = {f'{a}|{b}|{x}': int(v) for (a, b, x), v in
                                     pd.DataFrame({'c': cls[priced], 'b': band[priced],
                                                   'x': theirs[priced]}).groupby(['c', 'b', 'x']).size().items()}
    edge = priced & n.isin([5, 6, 50, 51])
    out['edge_N_class_disposition'] = {f'N={int(k)}|{a}|{x}': int(v) for (k, a, x), v in
                                       pd.DataFrame({'n': n[edge], 'c': cls[edge], 'x': theirs[edge]})
                                       .groupby(['n', 'c', 'x']).size().items()}
    out['class_vs_their_u'] = {f'{a}|{b}': int(v) for (a, b), v in
                               pd.DataFrame({'c': cls[priced], 'u': pd.Series(d['u'].values, index=W1N.index)[priced]})
                               .groupby(['c', 'u']).size().items()}
    # nonmetric N > 50 by their u (the registry lists it)
    m = exp.eq('w1-nonmetric-count-as-size-hold')
    out['nonmetric_gt50_by_u'] = vc(pd.Series(d['u'].values, index=W1N.index)[m])
    out['unknown_by_band_disposition'] = vc((band.astype(str) + '|' + theirs.astype(str))[priced & cls.eq('none')])
    # served Price on divided rows == sc2q3a / sc2q3b
    div = theirs.isin(['w1-divided', 'w1-divided-unvalidated'])
    pr = pd.Series(d['Price'].values, index=W1N.index)
    out['divided_price_equals_a_over_b'] = bool(np.allclose(pr[div], (price / n)[div], rtol=1e-12))
    for k in set(exp):
        W1MASK[k] = exp.eq(k)
    W1MASK['_cls'] = cls
    W1MASK['_band'] = band
    return out


R.step('w1_bands', w1_bands)


# ------------------------------------------------ derivation inputs, every PP key
def pp_keys():
    out = {}
    recs = D.derivation_records('Nigeria')
    keys = sorted(k for k in recs if '::pp-' in k)
    out['keys'] = keys
    served_idx = set(cp.index)
    key_disp = {v: k for k, v in pp.PP_OMISSION_KEYS.items()}
    construct_disp = {K + 'pp-w1-number-of-units': 'w1-divided',
                      K + 'pp-w1-number-of-units-unvalidated': 'w1-divided-unvalidated'}
    ucode_lab = None
    if '_cls' in W1MASK:
        ucode = pd.to_numeric(W1N['sc2q3c'], errors='coerce')
        ucode_lab = pd.DataFrame({'k': ucode, 'c': W1MASK['_cls']}).dropna(subset=['k']) \
            .drop_duplicates('k').set_index('k')['c'].to_dict()
    for key in keys:
        rec = {'rows_field': getattr(recs[key], 'rows', None), 'waves': getattr(recs[key], 'waves', None)}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                inp = c.derivation_inputs(key)
        except Exception as e:  # noqa: BLE001
            out[key] = {'error': f'{type(e).__name__}: {e}'[:600]}
            continue
        rec['n'] = int(len(inp))
        rec['unique'] = bool(inp.index.is_unique)
        flat = inp.reset_index()
        rec['by_t'] = vc(flat['t']) if 't' in flat else None
        rec['by_t_u'] = {f'{a}|{b}': int(v) for (a, b), v in flat.groupby(['t', 'u']).size().items()} \
            if len(flat) and {'t', 'u'} <= set(flat) else {}
        if 'Disposition' in flat:
            rec['inputs_disposition'] = vc(flat['Disposition'])
        disp = construct_disp.get(key) or key_disp.get(key)
        if key in key_disp:
            rec['disjoint_from_served'] = not (set(inp.index) & served_idx)
        else:
            stamped = cp[cp['Derivation'].eq(key)]
            rec['stamped'] = int(len(stamped))
            rec['index_set_equal_stamped'] = set(inp.index) == set(stamped.index)
            try:
                fn = D.resolve_callable(recs[key].function)
                if 'number-of-units' in key:
                    again = fn(inp['sc2q3a'], inp['sc2q3b'])
                elif 'metric-unit-quantity' in key:
                    again = fn(inp['c2q3'], inp['c2q2a'])
                else:
                    again = None
                if again is not None:
                    got = cp.loc[inp.index, 'Price'].to_numpy()
                    rec['price_reproduced'] = bool(np.allclose(np.asarray(again, dtype=float), got, rtol=1e-12))
            except Exception as e:  # noqa: BLE001
                rec['reproduce_error'] = f'{type(e).__name__}: {e}'[:300]
        # W1: multiset against my own mask, and class x band of the inputs
        if disp and disp in W1MASK and set(rec.get('by_t') or {}) <= {'2010Q3'}:
            mine = W1N.loc[W1MASK[disp], W1COLS]
            rec['my_mask_rows'] = int(len(mine))
            if len(inp):
                rec['multiset_equal_my_mask'] = multiset(mine, W1COLS) == multiset(flat, W1COLS)
            else:
                rec['multiset_equal_my_mask'] = len(mine) == 0
            if len(flat) and ucode_lab is not None:
                cls = pd.to_numeric(flat['sc2q3c'], errors='coerce').map(ucode_lab).fillna('none')
                nn = pd.to_numeric(flat['sc2q3b'], errors='coerce')
                band = pd.cut(nn, [-np.inf, 0.999999, 1.000001, 5.000001, 50.000001, np.inf],
                              labels=['<1', '1', '2-5', '6-50', '>50']).astype('string').fillna('NaN')
                rec['inputs_class_band'] = vc(cls.astype(str) + '|' + band.astype(str))
        out[key] = rec
    return out


R.step('pp_keys', pp_keys)


# ------------------------------------------------------------- W5 Bottle/Can
def w5_bottle():
    spec = pp.pp_source('W5')
    ops = spec['operand_columns']
    price = pd.to_numeric(W5N[ops['price']], errors='coerce')
    q = pd.to_numeric(W5N[ops['quantity']], errors='coerce')
    ucode = pd.to_numeric(W5N[ops['unit']], errors='coerce')
    ulab = labels_of(W5N, W5C, ops['unit'])
    slab = labels_of(W5N, W5C, ops['size'])
    out = {'labelled_reads': [ulab is not None, slab is not None]}
    if ulab is None:
        ulab = ucode.map(pp._card(spec, 'unit')).astype('string')
    if slab is None:
        scode = pd.to_numeric(W5N[ops['size']], errors='coerce')
        slab = scode.map(pp._card(spec, 'size')).astype('string')
    size_q = slab.map(lambda s: np.nan if pd.isna(s) else (ng._c8_size_metric_grams(s) or np.nan)).astype('float64')
    my_metric = slab.astype('string').str.contains(
        r'\d+(\.\d+)?\s*(GRAMS?|GMS?|G\b|KGS?|KILO|CL|ML|LITRES?|LTRS?|L\b)', case=False, na=False)
    priced = price.gt(0)
    bottle = ulab.astype('string').str.contains('BOTTLE', case=False, na=False)
    d = DISP['W5']
    theirs = pd.Series(d['Disposition'].values, index=W5N.index)
    der = pd.Series(d['Derivation'].values, index=W5N.index)
    u_their = pd.Series(d['u'].values, index=W5N.index)
    pr = pd.Series(d['Price'].values, index=W5N.index)
    m63 = priced & bottle & q.notna() & q.ne(1) & size_q.gt(0)
    out['mask63'] = int(m63.sum())
    out['mask63_my_metric_regex'] = int((priced & bottle & q.notna() & q.ne(1) & my_metric).sum())
    out['mask63_disposition'] = vc(theirs[m63])
    out['mask63_derivation'] = vc(der[m63])
    out['mask63_u'] = vc(u_their[m63])
    out['mask63_price_eq_c2q3_over_size'] = bool(np.allclose(pr[m63], (price / size_q)[m63], rtol=1e-12))
    out['mask63_size_labels'] = vc(slab[m63])
    out['mask63_q_eq_size'] = int((m63 & np.isclose(q, size_q)).sum())
    qb = pd.cut(q[m63], [1, 5, 12, 24, 50, 100, np.inf], labels=['2-5', '6-12', '13-24', '25-50', '51-100', '>100'])
    out['mask63_q_bins'] = vc(qb.astype('string'))
    # the rows still held: c2q2a present, not 1, not routed anywhere else
    gcl = ulab.map(unit_class).isin(['g', 'cl'])
    one_size = slab.astype('string').str.match(r'^\s*99\.', na=False)
    nosize_gcl = gcl & ~size_q.gt(0) & ~one_size
    held_mask = priced & q.notna() & q.ne(1) & ~m63 & ~nosize_gcl
    out['held_mask'] = int(held_mask.sum())
    out['held_mask_by_unit'] = vc(ulab[held_mask])
    out['held_mask_vs_theirs'] = vc(theirs[held_mask])
    out['theirs_w5_quantity_hold'] = int(theirs.eq('w5-quantity-hold').sum())
    out['theirs_w5_quantity_hold_equals_mask'] = bool((theirs.eq('w5-quantity-hold') == held_mask).all())
    out['theirs_metric_size_w5'] = int(theirs.eq('metric-size').sum())
    # inputs of the c2 key contain the 63 (multiset)
    cols = ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2a', 'c2q2b', 'c2q2c', 'c2q3']
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        inp = c.derivation_inputs(K + 'pp-c2-metric-size').reset_index()
    inp5 = inp[inp['t'].eq('2023Q3')]
    mine = multiset(W5N.loc[theirs.eq('metric-size') | m63], cols)
    out['c2_inputs_2023Q3_rows'] = int(len(inp5))
    out['c2_inputs_2023Q3_multiset_eq_metric_size_or_63'] = mine == multiset(inp5, cols)
    six3 = multiset(W5N.loc[m63], cols)
    have = multiset(inp5, cols)
    out['c2_inputs_contain_63'] = all(have[k] >= v for k, v in six3.items())
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        hold = c.derivation_inputs(K + 'pp-w5-quantity-not-one-hold').reset_index()
    cols2 = [x for x in cols if x in hold.columns]
    out['hold_inputs_rows'] = int(len(hold))
    out['hold_inputs_multiset_eq_mask'] = multiset(W5N.loc[held_mask], cols2) == multiset(hold, cols2)
    # price check: served 63 vs the round's other pp-c2-metric-size rows, same (j, u)
    key63 = pd.MultiIndex.from_frame(d.loc[m63.values, ['t', 'v', 'j', 'u', 'variety', 'obs']])
    s = cpr[cpr['t'].eq('2023Q3') & cpr['Derivation'].eq(K + 'pp-c2-metric-size')].set_index(
        ['t', 'v', 'j', 'u', 'variety', 'obs'])
    out['served_63_present'] = int(key63.isin(s.index).sum())
    others = s[~s.index.isin(key63)].reset_index()
    ref = others.groupby(['j', 'u'])['Price'].median().rename('ref')
    s63 = s[s.index.isin(key63)].reset_index().merge(ref.reset_index(), on=['j', 'u'], how='left')
    s63 = s63.assign(r=s63['Price'] / s63['ref'])
    qq = pd.Series(q[m63].values, index=pd.MultiIndex.from_frame(d.loc[m63.values, ['v', 'j', 'u', 'variety', 'obs']]))
    eqs = pd.Series(np.isclose(q[m63], size_q[m63]),
                    index=qq.index)
    s63 = s63.set_index(['v', 'j', 'u', 'variety', 'obs'])
    s63['q'] = qq.reindex(s63.index).values
    s63['eqsize'] = eqs.reindex(s63.index).values

    def qs(x):
        x = pd.Series(x).dropna()
        return {'n': int(len(x)), 'median': round(float(x.median()), 3) if len(x) else None,
                'p10_p90': [round(float(x.quantile(.1)), 3), round(float(x.quantile(.9)), 3)] if len(x) else None,
                'within2x': round(float(((x > .5) & (x < 2)).mean()), 3) if len(x) else None}
    out['ratio_vs_same_round_item_median'] = qs(s63['r'])
    out['ratio_eqsize'] = qs(s63.loc[s63['eqsize'].astype(bool), 'r'])
    out['ratio_other'] = qs(s63.loc[~s63['eqsize'].astype(bool), 'r'])
    ok = s63['r'].gt(0) & s63['q'].gt(0)
    if ok.sum() > 3:
        lr, lq = np.log(s63.loc[ok, 'r']), np.log(s63.loc[ok, 'q'])
        out['slope_log_ratio_on_log_c2q2a'] = round(float(np.polyfit(lq, lr, 1)[0]), 3)
    ph = cpr[cpr['t'].eq('2024Q1') & cpr['Derivation'].eq(K + 'c8-metric-size')].groupby(['j', 'u'])['Price'].median()
    m2 = s63.reset_index().merge(ph.rename('ph').reset_index(), on=['j', 'u'])
    out['ratio_vs_PH2024Q1_c8'] = qs(m2['Price'] / m2['ph'])
    return out


R.step('w5_bottle', w5_bottle)


# ------------------------------------------------------------- Semovita
HH_FILES = [
    ('W1 PP', '2010-11/Data/Post Planting Wave 1/Household/sect7b_plantingw1.dta'),
    ('W1 PH', '2010-11/Data/Post Harvest Wave 1/Household/sect10b_harvestw1.dta'),
    ('W2 PP', '2012-13/Data/Post Planting Wave 2/Household/sect7b_plantingw2.dta'),
    ('W2 PH', '2012-13/Data/Post Harvest Wave 2/Household/sect10b_harvestw2.dta'),
    ('W2 PP csv', '2012-13/Data/sect7b_plantingw2.csv'),
    ('W2 PH csv', '2012-13/Data/sect10b_harvestw2.csv'),
    ('W3 PP', '2015-16/Data/sect7b_plantingw3.dta'),
    ('W3 PH', '2015-16/Data/sect10b_harvestw3.dta'),
    ('W4 PP', '2018-19/Data/sect7b_plantingw4.dta'),
    ('W4 PH', '2018-19/Data/sect10b_harvestw4.dta'),
    ('W5 PP', '2023-24/Data/Post Planting Wave 5/Household/sect6b_plantingw5.dta'),
    ('W5 PH', '2023-24/Data/Post Harvest Wave 5/Household/sect6b_harvestw5.dta'),
    ('NLSS', '2018-19-NLSS/Data/Household/sect6b_food_cons.dta'),
    ('W1 PH C8', '2010-11/Data/Post Harvest Wave 1/Community/sectc8_harvestw1.dta'),
    ('W2 PH C8', '2012-13/Data/Post Harvest Wave 2/Community/sectc8_harvestw2.dta'),
]


def semovita_files():
    out = {}
    for name, rel in HH_FILES:
        rec = {}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                rn, rc = read_pair(rel)
        except Exception as e:  # noqa: BLE001
            out[name] = {'error': f'{type(e).__name__}: {e}'[:300]}
            continue
        rec['rows'] = int(len(rn))
        item = 'item_cd' if 'item_cd' in rn.columns else next((x for x in rn.columns if 'item' in x.lower()), None)
        rec['item_col'] = item
        if item is not None:
            code = pd.to_numeric(rn[item], errors='coerce')
            rec['code15_rows'] = int(code.eq(15).sum())
            lab = labels_of(rn, rc, item)
            if lab is not None:
                for k in (15, 23, 38):
                    vals = lab[code.eq(k)].dropna().unique().tolist()
                    rec[f'code{k}_label'] = [str(v) for v in vals[:3]]
        hits = {}
        src = rc if rc is not None else rn
        for col in src.columns:
            s = src[col]
            if pd.api.types.is_numeric_dtype(s):
                continue
            m = s.astype('string').str.contains(r'semo', case=False, na=False)
            if not m.any():
                continue
            h = {'rows': int(m.sum()), 'categorical': bool(isinstance(s.dtype, pd.CategoricalDtype))}
            if h['categorical']:
                h['labels'] = sorted({str(v) for v in s[m].unique()})[:5]
            if item is not None:
                h['by_item_cd'] = vc(pd.to_numeric(rn.loc[m.values, item], errors='coerce'))
            hits[col] = h
        rec['semo_columns'] = hits
        out[name] = rec
    return out


R.step('semovita_files', semovita_files)


def semovita_served():
    out = {}
    icode = pd.to_numeric(W2N['item_cd'], errors='coerce')
    price = pd.to_numeric(W2N[pp.pp_source('W2')['operand_columns']['price']], errors='coerce')
    out['w2_pp_code15_priced'] = int((icode.eq(15) & price.gt(0)).sum())
    d = DISP['W2']
    out['w2_code15_disposition'] = vc(pd.Series(d['Disposition'].values)[icode.eq(15).values & price.gt(0).values])
    out['w2_code15_j'] = vc(pd.Series(d['j'].values)[icode.eq(15).values & price.gt(0).values])
    out['cp_j_semovita_by_t'] = vc(cpr.loc[cpr['j'].eq('Semovita'), 't'])
    out['cp_j_bread_by_t'] = vc(cpr.loc[cpr['j'].eq('Bread'), 't'])
    agg = pd.read_parquet(frames / 'cand.community_prices_aggregate.parquet').reset_index()
    pr = agg[agg['t'].eq('2012Q3')]
    out['agg_2012Q3_other_grains'] = int(pr['j'].eq('Other grains and flour').sum())
    out['agg_2012Q3_semovita'] = int(pr['j'].eq('Semovita').sum())
    fa = pd.read_parquet(frames / 'cand.food_acquired.parquet').reset_index()
    out['fa_j_semovita'] = int(fa['j'].eq('Semovita').sum())
    out['fa_j_bread_by_t'] = vc(fa.loc[fa['j'].eq('Bread'), 't'])
    out['fa_j_other_grains_by_t'] = vc(fa.loc[fa['j'].eq('Other grains and flour'), 't'])
    out['fa_u_ml_cl_by_t'] = {f'{t}|{u}': int(n) for (t, u), n in
                              fa[fa['u'].isin(['ml', 'cl'])].groupby(['t', 'u']).size().items()}
    for side in ('base', 'cand'):
        p = frames / f'{side}.food_acquired_2012Q3.parquet'
        if p.exists():
            f = pd.read_parquet(p).reset_index()
            out[f'{side}_fa_2012Q3_semovita_bread_by_t'] = {
                f'{t}|{j}': int(n) for (t, j), n in
                f[f['j'].isin(['Semovita', 'Bread'])].groupby(['t', 'j']).size().items()}
    tab = df_from_orgfile(str(CM), name='harmonize_food')
    out['hf_rows'] = int(len(tab))
    out['hf_preferred_semovita'] = int(tab['Preferred Label'].astype(str).str.strip().eq('Semovita').sum())
    out['hf_aggregate_other_grains_rows'] = int(tab['Aggregate Label'].astype(str).str.strip()
                                                .eq('Other grains and flour').sum())
    q = [x for x in tab.columns if re.fullmatch(r'\d{4}Q[1-4]', str(x))]
    cells = tab[q].astype('string').apply(lambda s: s.str.strip().str.casefold().eq('semovita'))
    out['hf_quarter_cells_semovita'] = {str(col): int(cells[col].sum()) for col in q if cells[col].any()}
    out['labels_quarter_2012Q3_code15'] = ng.harmonized_food_labels(t='2012Q3').get(15)
    out['code_contract_15'] = ng.harmonized_food_labels(
        dirs=[f"{countries_root() / 'Nigeria' / '_'}/"]).get(15)
    return out


R.step('semovita_served', semovita_served)


# ------------------------------------------------------------- labels
def labels():
    hf = df_from_orgfile(str(CM), name='harmonize_food')
    ut = df_from_orgfile(str(CM), name='u')
    pref_j = {str(x).strip() for x in hf['Preferred Label'] if pd.notna(x)}
    pref_u = {str(x).strip() for x in ut['Preferred Label'] if pd.notna(x)}
    out = {}
    for label, mask in (('PP', is_pp), ('PH', ~is_pp)):
        s = cpr[mask]
        out[label] = {
            'j_not_preferred': {f'{a}|{b}': int(n) for (a, b), n in
                                s.loc[~s['j'].isin(pref_j), ['t', 'j']].value_counts().items()},
            'u_not_preferred_or_unknown': {f'{a}|{b}': int(n) for (a, b), n in
                                           s.loc[~s['u'].isin(pref_u | {'Unknown'}), ['t', 'u']]
                                           .value_counts().items()},
            'u_unknown_by_t': vc(s.loc[s['u'].eq('Unknown'), 't'])}
    codes = pd.to_numeric(ut['Code'], errors='coerce')
    out['u_code_dupes'] = int(codes.dropna().duplicated().sum())
    out['u_pref_dupes'] = sorted(ut['Preferred Label'][ut['Preferred Label'].duplicated(keep=False)]
                                 .astype(str).unique().tolist())
    out['u_new_rows'] = {str(int(k)): str(v) for k, v in zip(codes, ut['Preferred Label']) if k in (1020, 1021, 1022)}
    wave_cols = [x for x in ut.columns if x not in ('Code', 'Preferred Label')]
    for lab in ('Congo/Mudu', 'ml', 'Tomatoes cup'):
        hits = {col: int(ut[col].astype('string').str.strip().eq(lab).sum()) for col in wave_cols}
        out[f'u_wave_cells_equal_{lab}'] = {k: v for k, v in hits.items() if v}
        out[f'u_wave_cells_casefold_{lab}'] = {
            col: int(ut[col].astype('string').str.strip().str.casefold().eq(lab.casefold()).sum())
            for col in wave_cols if ut[col].astype('string').str.strip().str.casefold().eq(lab.casefold()).any()}
        out[f'u_pref_casefold_{lab}'] = int(ut['Preferred Label'].astype('string').str.strip()
                                            .str.casefold().eq(lab.casefold()).sum())
    return out


R.step('labels', labels)


def attrs_now():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        df = c.community_prices()
        agg = c.community_prices(labels='Aggregate')
    der = df.attrs.get('derivations', {})
    der = der.get('Nigeria', der)
    out = {'derivation_rows': {k: (v.get('rows') if isinstance(v, dict) else v)
                               for k, v in der.items() if 'community_prices' in k}}
    out['labels_unmatched_aggregate'] = agg.attrs.get('labels_unmatched')
    out['warn_cats'] = vc([x.category.__name__ for x in w])
    return out


R.step('attrs_now', attrs_now)
R.data['done'] = True
R.flush()
print('PROBE DONE', flush=True)
