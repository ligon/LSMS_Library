"""GH #587 PP probes on the candidate (warm private root after build.py).

usage: python probe.py <clone> <framesdir> <resultsdir>
Aggregates only (codebook labels and counts); no raw rows are written.
"""
import re
import sys
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
SOURCES = ('W1', 'W2', 'W3-first', 'W3-second', 'W4', 'W5')
PP_Q = {'W1': '2010Q3', 'W2': '2012Q3', 'W3-first': '2015Q3', 'W3-second': '2015Q3',
        'W4': '2018Q3', 'W5': '2023Q3'}
CM = countries_root() / 'Nigeria' / '_' / 'categorical_mapping.org'
c = ll.Country('Nigeria')
cp = pd.read_parquet(frames / 'cand.community_prices.parquet')
cpr = cp.reset_index()
is_pp = cpr['t'].isin(set(PP_Q.values()))

RAW = {}
for key in SOURCES:
    spec = pp.pp_source(key)
    RAW[key] = get_dataframe(str(countries_root() / 'Nigeria' / spec['source_path']),
                             convert_categoricals=False)


def vc(s):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).items()}


def body(label):
    if pd.isna(label):
        return ''
    return ng._strip_code_prefix(str(label)).strip().lower()


def unit_class(label):
    """My own metric-unit classifier over the NATIVE card label (not their maps)."""
    b = body(label)
    if not b:
        return 'none'
    pats = (('kg', r'kilograms?( \(kg\))?'), ('g', r'grams?( \(g\))?'),
            ('l', r'litres?( \(l\))?'), ('cl', r'cent[i]?litres?( \(cl\))?'),
            ('ml', r'millilitres?( \(ml\))?'))
    for name, pat in pats:
        if re.fullmatch(pat, b):
            return name
    return 'other'


def metric_in_size(label):
    """True if a SIZE label states a number with a unit word (my own regex)."""
    if pd.isna(label):
        return False
    b = ng._strip_code_prefix(str(label)).upper()
    return bool(re.search(r'\d+(\.\d+)?\s*(GRAMS?|GMS?|G\b|KGS?|KILO|CL|ML|LITRES?|LTRS?|L\b)', b))


# ---------------------------------------------------------------- partition
def partition():
    out = {}
    for key in SOURCES:
        spec = pp.pp_source(key)
        raw = RAW[key]
        ops = spec['operand_columns']
        price = pd.to_numeric(raw[ops['price']], errors='coerce')
        priced = price.gt(0)
        ucard = pp._card(spec, 'unit')
        ucode = pd.to_numeric(raw[ops['unit']], errors='coerce')
        ulab = ucode.map(ucard)
        ucls = ulab.map(unit_class).where(ucode.notna(), 'none')
        # off-card unit codes
        offcard = ucode.notna() & ulab.isna()
        icard = pp._card_item(spec)
        icode = pd.to_numeric(raw['item_cd'], errors='coerce')
        imap = pp.pp_item_map(key)
        item_hold = icode.notna() & ~icode.isin(list(imap))
        if 'size' in ops:
            scard = pp._card(spec, 'size')
            scode = pd.to_numeric(raw[ops['size']], errors='coerce')
            slab = scode.map(scard)
            size_metric = slab.map(metric_in_size)
            size_q = slab.map(lambda s: np.nan if pd.isna(s) else (ng._c8_size_metric_grams(s) or np.nan)).astype('float64')
            size_blank = scode.isna()
            one_size = slab.astype('string').str.match(r'^\s*99\.', na=False)
        else:
            size_metric = pd.Series(False, index=raw.index)
            size_q = pd.Series(np.nan, index=raw.index)
            size_blank = pd.Series(True, index=raw.index)
            one_size = pd.Series(False, index=raw.index)
        exp = pd.Series('served', index=raw.index, dtype='object')
        decided = pd.Series(False, index=raw.index)

        def put(name, m):
            nonlocal decided
            m = m.fillna(False).astype(bool) & ~decided
            exp[m] = name
            decided |= m
        put('unpriced', ~priced)
        put('item-hold', item_hold)
        put('unit-unplaced', offcard)
        g = pp._group(key)
        if g == 'W1':
            n = pd.to_numeric(raw[ops['quantity']], errors='coerce')
            put('w1-no-count', n.isna() | n.lt(1))
            put('w1-metric-package-hold', ucls.isin(['g', 'ml']) & n.eq(1))
            put('w1-count-as-size-hold', ucls.isin(['kg', 'l']) & n.gt(50))
            put('w1-divided', n.gt(1))
        elif g in ('W2', 'W3'):
            put('unrecorded-size-hold', ucls.isin(['g', 'cl']))
        elif g == 'W5':
            q = pd.to_numeric(raw[ops['quantity']], errors='coerce')
            gcl = ucls.isin(['g', 'cl']) & size_blank & ~one_size
            put('w5-metric-quantity', gcl & q.gt(0))
            put('unrecorded-size-hold', gcl)
            put('w5-quantity-hold', q.notna() & q.ne(1))
            put('metric-size', size_q.gt(0))
        elif g == 'W4':
            put('metric-size', size_q.gt(0))
        with __import__('warnings').catch_warnings():
            __import__('warnings').simplefilter('ignore')
            d = pp.pp_dispositions(raw, key)
        conf = pd.crosstab(exp, d['Disposition'])
        rec = {
            'raw_rows': int(len(raw)), 'priced': int(priced.sum()),
            'expected': vc(exp), 'theirs': vc(d['Disposition']),
            'agree': bool((exp.values == d['Disposition'].values).all()),
            'confusion_offdiag': {f'{a}->{b}': int(conf.loc[a, b]) for a in conf.index
                                  for b in conf.columns if a != b and conf.loc[a, b]},
            'offcard_unit_rows_priced': int((offcard & priced).sum()),
            'item_hold_priced_by_code': vc(icode[item_hold & priced]),
            'unit_class_priced': vc(ucls[priced]),
            'priced_metric_class_by_disposition': {
                f'{a}|{b}': int(n) for (a, b), n in
                pd.DataFrame({'c': ucls[priced], 'd': d['Disposition'][priced]})
                .groupby(['c', 'd']).size().items() if a in ('g', 'cl', 'ml', 'kg', 'l')},
            'served_rows': int(d['Disposition'].isin(pp.PP_SERVED_DISPOSITIONS).sum()),
            'served_derivation': vc(d.loc[d['Disposition'].isin(pp.PP_SERVED_DISPOSITIONS), 'Derivation']),
        }
        if 'size' in ops:
            rec['size_labels_with_number_but_no_metric_decode'] = sorted(
                {str(s) for s, m, qq in zip(slab[priced], size_metric[priced], size_q[priced])
                 if m and not (qq > 0)})
            rec['size_labels_metric_decoded'] = vc(slab[priced & size_q.gt(0)])
            rec['g_cl_served_undivided_one_size'] = int(
                (d['Disposition'].eq('served') & d['u'].isin(['g', 'cl', 'ml']) & one_size).sum())
        if g == 'W3':
            oth = raw[ops['other']].astype('string').str.strip()
            rec['w3_gcl_priced_other_text_nonblank'] = int(
                (priced & ucls.isin(['g', 'cl']) & oth.notna() & oth.ne('')).sum())
            rec['w3_kgl_priced_other_text_nonblank'] = int(
                (priced & ucls.isin(['kg', 'l']) & oth.notna() & oth.ne('')).sum())
        if g == 'W1':
            n = pd.to_numeric(raw[ops['quantity']], errors='coerce')
            pn = n[priced]
            cats = pd.cut(pn, [-np.inf, 0, 0.999999, 1.000001, 2 - 1e-9, 5.000001, 50.000001, np.inf],
                          labels=['<=0', '(0,1)', '=1', '(1,2)', '[2,5]', '(5,50]', '>50'])
            rec['w1_N_priced'] = vc(cats.astype('string').fillna('NaN'))
            rec['w1_N_nonint_priced'] = int((pn.notna() & pn.ne(np.floor(pn))).sum())
            rec['w1_N_by_class'] = {f'{a}|{b}': int(k) for (a, b), k in pd.DataFrame(
                {'c': ucls[priced], 'n': cats.astype('string').fillna('NaN')}).groupby(['c', 'n']).size().items()}
            rec['w1_no_unit_priced'] = int((ucode.isna() & priced).sum())
            rec['w1_no_unit_priced_N'] = vc(cats[ucode[priced].isna()].astype('string').fillna('NaN'))
            rec['w1_gml_N2to5_divided'] = int((d['Disposition'].eq('w1-divided') & ucls.isin(['g', 'ml'])
                                               & n.between(2, 5)).sum())
        if g == 'W5':
            q = pd.to_numeric(raw[ops['quantity']], errors='coerce')
            rec['w5_c2q2a_present_priced'] = int((q.notna() & priced).sum())
            rec['w5_c2q2a_eq1_priced'] = int((q.eq(1) & priced).sum())
            rec['w5_c2q2a_eq1_by_unit'] = vc(ulab[q.eq(1) & priced])
            hold = d['Disposition'].eq('w5-quantity-hold')
            rec['w5_quantity_hold_by_unit'] = vc(ulab[hold])
            rec['w5_quantity_hold_metric_size'] = int((hold & size_q.gt(0)).sum())
            # BOTTLE/CAN with coded metric size: c2q2a == size quantity?
            bc = hold & ulab.astype('string').str.contains('BOTTLE', na=False) & size_q.gt(0)
            rec['w5_bottle_metric_size'] = int(bc.sum())
            rec['w5_bottle_c2q2a_eq_size_q'] = int((bc & np.isclose(q, size_q)).sum())
            lit = slab.astype('string').str.contains(r'(?i)\bCL\b|LITRE', na=False)
            # size q is in cl for litre labels; if c2q2a is a count of cl, == size_q
            rec['w5_bottle_c2q2a_vs_size'] = vc(pd.Series(np.where(
                np.isclose(q[bc], size_q[bc]), 'eq', np.where(q[bc] == 1, 'one', 'other'))))
            rec['w5_bottle_litre_labels'] = int((bc & lit).sum())
        out[key] = rec
    return out


PART = R.step('partition', partition)


# ------------------------------------------------- served table vs partition
def served_vs_partition():
    out = {}
    for t in sorted(set(PP_Q.values())):
        s = cpr[cpr['t'].eq(t)]
        out[t] = {'rows': int(len(s)), 'by_obs': vc(s['obs']), 'derivation': vc(s['Derivation']),
                  'u': vc(s['u']), 'size_nonnull': int(s['Size'].notna().sum()),
                  'price_na': int(s['Price'].isna().sum()), 'price_nonpos': int(s['Price'].le(0).sum())}
    out['ph_size_nonnull'] = int(cpr.loc[~is_pp, 'Size'].notna().sum())
    out['index_unique'] = bool(cp.index.is_unique)
    out['index_names'] = list(cp.index.names)
    for lvl in cp.index.names:
        out.setdefault('null_index', {})[lvl] = int(cpr[lvl].isna().sum())
    return out


R.step('served', served_vs_partition)


# ---------------------------------------------------------------- labels
def labels():
    hf = df_from_orgfile(str(CM), name='harmonize_food')
    ut = df_from_orgfile(str(CM), name='u')
    pref_j = {str(x).strip() for x in hf['Preferred Label'] if pd.notna(x)}
    pref_u = {str(x).strip() for x in ut['Preferred Label'] if pd.notna(x)}
    out = {}
    for label, mask in (('PP', is_pp), ('PH', ~is_pp)):
        s = cpr[mask]
        jn = s.loc[~s['j'].isin(pref_j), ['t', 'j']].value_counts()
        un = s.loc[~s['u'].isin(pref_u | {'Unknown'}), ['t', 'u']].value_counts()
        out[label] = {'j_not_preferred': {f'{a}|{b}': int(n) for (a, b), n in jn.items()},
                      'u_not_preferred': {f'{a}|{b}': int(n) for (a, b), n in un.items()},
                      'u_unknown_rows': vc(s.loc[s['u'].eq('Unknown'), 't'])}
    # quarter resolver fallbacks, with observed (carded) codes passed
    import warnings
    fb = {}
    for key in SOURCES:
        spec = pp.pp_source(key)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                ng._food_item_labels(pp.PP_ITEM_QUARTER[key],
                                     observed_codes=list(pp._card_item(spec)))
            except Exception as e:  # noqa: BLE001
                fb[key] = f'ERR {e}'
                continue
        fb[key] = [str(x.message)[:1500] for x in w if 'FoodItemDecode' in x.category.__name__]
    out['quarter_resolver_warnings_with_carded_codes'] = fb
    # item map labels that are not Preferred Labels (per source)
    nonpref = {}
    for key in SOURCES:
        imap = pp.pp_item_map(key)
        raw = RAW[key]
        spec = pp.pp_source(key)
        price = pd.to_numeric(raw[spec['operand_columns']['price']], errors='coerce')
        icode = pd.to_numeric(raw['item_cd'], errors='coerce')
        nonpref[key] = {f'{code}:{lab}': int(((icode == code) & price.gt(0)).sum())
                        for code, lab in imap.items() if lab not in pref_j}
    out['item_map_not_preferred'] = nonpref
    umap_nonpref = {}
    for key in SOURCES:
        um = pp.pp_unit_map(key)
        umap_nonpref[key] = sorted({f'{k}:{v}' for k, v in um.items() if v not in pref_u})
    out['unit_map_not_preferred'] = umap_nonpref
    out['u_preferred_has'] = {x: (x in pref_u) for x in
                              ('Tomatoes cup', 'Congo/Mudu', 'ml', 'cl', 'Bag of 20 Sachets', 'Unknown',
                               'Mudu', 'Derica', 'Congo', 'g', 'Kg', 'l')}
    return out


R.step('labels', labels)


# ---------------------------------------------- W1/W2 quarter-card checks
def quarter_cards():
    hf = df_from_orgfile(str(CM), name='harmonize_food')
    out = {}
    contract = ng.harmonized_food_labels(dirs=[f"{countries_root() / 'Nigeria' / '_'}/"])

    def norm(s):
        s = str(s)
        s = re.sub(r'^[^:]*:\s*', '', s) if ':' in s else s
        s = re.sub(r'\betc\.?', '', s.lower())
        return re.sub(r'[^a-z0-9]', '', s)
    for key, col in (('W1', '2010Q3'), ('W2', '2012Q3'), ('W3-first', '2015Q3'),
                     ('W4', '2018Q3'), ('W5', '2018Q3')):
        card = pp._card_item(pp.pp_source(key))
        codecol = {}
        for code, lab in zip(hf['Code'], hf[col] if col in hf else []):
            try:
                codecol[int(float(code))] = lab
            except (TypeError, ValueError):
                pass
        same, diff = 0, []
        for code, lab in card.items():
            q = codecol.get(code)
            if q is not None and pd.notna(q) and norm(q) == norm(lab):
                same += 1
            else:
                diff.append(f'{code}: card={lab!r} hf[{col}]={q!r}')
        imap = pp.pp_item_map(key)
        vs_contract = [f'{c_}: quarter={imap[c_]!r} contract={contract.get(c_)!r}'
                       for c_ in imap if contract.get(c_) != imap[c_]]
        out[key] = {'carded': len(card), 'same_label_as_quarter_column': same,
                    'differ': diff, 'quarter_vs_code_contract_differs': vs_contract}
    return out


R.step('quarter_cards', quarter_cards)


# -------------------------------------------- MAPPING.org vs implementation
def mapping_vs_code():
    path = Path(clone) / '.coder/ledger/587-nigeria-pp-mapping/MAPPING.org'
    text = path.read_text().splitlines()
    sect, hdr, tabs = None, None, {}
    for line in text:
        m = re.match(r'^\* (W[0-9A-Za-z-]+) \(.*\) (items|units)', line)
        if m:
            sect = (m.group(1), m.group(2)); hdr = None
            continue
        if line.startswith('* '):
            sect = None
            continue
        if sect and line.startswith('|') and not line.startswith('|-'):
            cells = [x.strip() for x in line.strip('|').split('|')]
            if hdr is None:
                hdr = cells
                continue
            tabs.setdefault(sect, []).append(dict(zip(hdr, cells)))
    out = {}
    for (src, kind), rows in tabs.items():
        if src not in SOURCES:
            continue
        mp = pp.pp_item_map(src) if kind == 'items' else pp.pp_unit_map(src)
        col = 'proposed j' if kind == 'items' else 'proposed u'
        mism, flags = [], {}
        for r in rows:
            try:
                code = int(float(r['code']))
            except (KeyError, ValueError):
                continue
            prop = r.get(col, '')
            got = mp.get(code)
            flags[r.get('flag', '')] = flags.get(r.get('flag', ''), 0) + 1
            if (prop or None) != got:
                mism.append(f"{code}: proposed={prop!r} code={got!r} flag={r.get('flag')}")
        out[f'{src}/{kind}'] = {'rows': len(rows), 'flags': flags, 'mismatch': mism}
    return out


R.step('mapping_vs_code', mapping_vs_code)


# -------------------------------------------------- W5 c2q2a evidence check
def w5_evidence():
    spec = pp.pp_source('W5')
    raw = RAW['W5']
    ops = spec['operand_columns']
    price = pd.to_numeric(raw[ops['price']], errors='coerce')
    ucode = pd.to_numeric(raw[ops['unit']], errors='coerce')
    ulab = ucode.map(pp._card(spec, 'unit'))
    scode = pd.to_numeric(raw[ops['size']], errors='coerce')
    q = pd.to_numeric(raw[ops['quantity']], errors='coerce')
    txt = raw['c2q2b_os'].astype('string').str.strip()
    cvn = pd.to_numeric(raw['c2q2_cvn'], errors='coerce') if 'c2q2_cvn' in raw else None
    cls = ulab.map(unit_class)
    base = price.gt(0) & cls.isin(['g', 'cl']) & scode.isna()
    out = {'g_cl_blank_size_priced': vc(cls[base]),
           'text_nonblank': vc(cls[base & txt.notna() & txt.ne('')]),
           'c2q2a_present': vc(cls[base & q.gt(0)])}

    def first_num(s):
        if pd.isna(s):
            return np.nan
        m = re.search(r'(\d+(?:\.\d+)?)', str(s))
        return float(m.group(1)) if m else np.nan

    def text_unit(s):
        if pd.isna(s):
            return 'none'
        u = str(s).upper()
        if re.search(r'\d\s*ML|\bML\b|MILLI', u):
            return 'ml'
        if re.search(r'\d\s*CL|\bCL\b|CENTI', u):
            return 'cl'
        if re.search(r'\d\s*KG|\bKG\b|KILO', u):
            return 'kg'
        if re.search(r'\d\s*G\b|\d\s*GM|\bG\b|GRAM|\d\s*GR', u):
            return 'g'
        if re.search(r'\d\s*L\b|LITRE|LITER|LTR', u):
            return 'l'
        return 'none'
    num = txt.map(first_num)
    tu = txt.map(text_unit)
    for cl_ in ('g', 'cl'):
        m = base & cls.eq(cl_) & q.gt(0)
        rec = {'rows': int(m.sum()), 'text_has_number': int((m & num.notna()).sum()),
               'text_unit': vc(tu[m]),
               'c2q2a_eq_textnum': int((m & np.isclose(q, num, rtol=1e-4)).sum()),
               'c2q2a_eq_textnum_div10': int((m & np.isclose(q, num / 10, rtol=1e-4)).sum()),
               'c2q2a_eq_textnum_rtol1e-2': int((m & np.isclose(q, num, rtol=1e-2)).sum())}
        ratio = (num / q)[m & num.notna() & ~np.isclose(q, num, rtol=1e-2) & ~np.isclose(q, num / 10, rtol=1e-2)]
        rec['unmatched_ratio_text_over_c2q2a'] = [round(float(x), 3) for x in ratio.dropna()][:20]
        if cvn is not None:
            rec['cvn'] = vc(cvn[m].round(6))
        out[cl_] = rec
    m0 = base & ~q.gt(0)
    out['no_c2q2a'] = {'rows': int(m0.sum()), 'text_has_number': int((m0 & num.notna()).sum()),
                       'by_class': vc(cls[m0])}
    # CUBE scaling: price / median(price | same item,unit,size, q in {NaN,1}) vs q
    cube = price.gt(0) & ulab.astype('string').str.contains('CUBE', na=False)
    key = pd.DataFrame({'i': raw['item_cd'], 'u': ucode, 's': scode.fillna(-1)})
    ref = price.where(cube & (q.isna() | q.eq(1))).groupby([key['i'], key['u'], key['s']]).transform('median')
    mm = cube & q.gt(1) & ref.notna()
    rr = (price / ref / q)[mm]
    out['cube_q_gt1'] = {'rows': int((cube & q.gt(1)).sum()), 'with_ref': int(mm.sum()),
                         'price_over_ref_over_q_quantiles': [round(float(x), 3) for x in
                                                             rr.quantile([.1, .25, .5, .75, .9])] if len(rr) else []}
    # served price for divided rows vs pp-c2-metric-size per-unit rows (item medians)
    s = cpr[cpr['t'].eq('2023Q3')]
    a = s[s['Derivation'].eq(pp.PP_W5_METRIC_QUANTITY_KEY)].groupby(['j', 'u'])['Price'].median()
    b = s[s['Derivation'].eq(pp.PP_METRIC_SIZE_KEY)].groupby(['j', 'u'])['Price'].median()
    jj = a.index.intersection(b.index)
    out['divided_vs_metric_size_item_median_ratio'] = {
        u: {'items': int(len([k for k in jj if k[1] == u])),
            'median_ratio': float(np.median([a[k] / b[k] for k in jj if k[1] == u])) if
            [k for k in jj if k[1] == u] else None} for u in ('g', 'cl')}
    ph = cpr[cpr['t'].eq('2024Q1')].groupby(['j', 'u'])['Price'].median()
    jj = a.index.intersection(ph.index)
    out['divided_vs_PH2024Q1_item_median_ratio'] = {
        u: {'items': int(len([k for k in jj if k[1] == u])),
            'median_ratio': float(np.median([a[k] / ph[k] for k in jj if k[1] == u])) if
            [k for k in jj if k[1] == u] else None} for u in ('g', 'cl')}
    return out


R.step('w5_evidence', w5_evidence)


# ------------------------------------------- derivation inputs, all PP keys
def pp_inputs():
    out = {}
    recs = D.derivation_records('Nigeria')
    keys = [k for k in recs if '::pp-' in k]
    served_idx = set(cp.index)
    disp = {}
    for key_ in SOURCES:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            disp[key_] = pp.pp_dispositions(RAW[key_], key_)
    allc = pd.concat(disp.values())
    for key in keys:
        rec = {'rows_field': getattr(recs[key], 'rows', None)}
        try:
            inp = c.derivation_inputs(key)
        except Exception as e:  # noqa: BLE001
            out[key] = {'error': f'{type(e).__name__}: {e}'[:500]}
            continue
        rec['n'] = int(len(inp))
        rec['index'] = list(map(str, inp.index.names))
        rec['unique'] = bool(inp.index.is_unique)
        flat = inp.reset_index()
        rec['by_t'] = vc(flat['t']) if 't' in flat else None
        rec['by_t_u'] = {f'{a}|{b}': int(n) for (a, b), n in flat.groupby(['t', 'u']).size().items()} \
            if {'t', 'u'} <= set(flat) else None
        dispo = [d_ for d_, k_ in pp.PP_OMISSION_KEYS.items() if k_ == key]
        if dispo:
            rec['disposition_count'] = int(allc['Disposition'].isin(dispo).sum())
            rec['independent_expected_count'] = int(sum(
                (PART or {}).get(k_, {}).get('expected', {}).get(dispo[0], 0) for k_ in SOURCES))
            rec['inputs_disjoint_from_served'] = not (set(inp.index) & served_idx)
            rec['inputs_disposition_values'] = vc(flat['Disposition']) if 'Disposition' in flat else None
        else:
            stamped = cp[cp['Derivation'].eq(key)]
            rec['stamped'] = int(len(stamped))
            rec['index_set_equal'] = set(inp.index) == set(stamped.index)
            rec['disposition_count'] = int(allc['Derivation'].eq(key).sum())
        out[key] = rec
    return out


R.step('pp_inputs', pp_inputs)


# ---------------------------------- community vs household unit value
def household_comparator():
    fa = pd.read_parquet(frames / 'cand.food_acquired.parquet').reset_index()
    out = {'fa_columns': list(fa.columns)}
    s_col = 's' if 's' in fa else None
    if s_col:
        out['s_values'] = vc(fa[s_col])
        fa = fa[fa[s_col].astype('string').str.contains('purchase', case=False, na=False)]
    if not {'Expenditure', 'Quantity', 'v', 'j', 'u', 't'} <= set(fa):
        out['skip'] = 'missing columns'
        return out
    fa = fa[fa['Quantity'].gt(0) & fa['Expenditure'].gt(0)]
    fa['uv'] = fa['Expenditure'] / fa['Quantity']
    hh = fa.groupby(['t', 'v', 'j', 'u'])['uv'].median().rename('hh')
    com = cpr[is_pp & cpr['obs'].eq(1)].copy()
    raw1 = RAW['W1']
    n_map = None
    m = com.merge(hh.reset_index(), on=['t', 'v', 'j', 'u'], how='inner')
    m['r'] = m['Price'] / m['hh']
    m['der'] = m['Derivation'].fillna('none')
    grp = m.groupby(['t', 'u', 'der'])['r']
    res = {}
    for (t, u, der), r in grp:
        if u not in ('g', 'cl', 'ml', 'Kg', 'l'):
            continue
        res[f'{t}|{u}|{der}'] = {'cells': int(len(r)), 'median': round(float(r.median()), 3),
                                 'within2x': round(float(((r > .5) & (r < 2)).mean()), 3)}
    out['strata'] = res
    # W1 divided rows by N band, for g/ml/Kg/l
    d1 = pp.pp_dispositions(raw1, 'W1') if True else None
    n = pd.to_numeric(raw1['sc2q3b'], errors='coerce')
    d1 = d1.assign(N=n.values)
    d1 = d1[d1['Disposition'].isin(pp.PP_SERVED_DISPOSITIONS)]
    d1 = d1.merge(hh.reset_index(), on=['t', 'v', 'j', 'u'], how='inner')
    d1['r'] = d1['Price'] / d1['hh']
    d1['band'] = pd.cut(d1['N'], [0, 1.0001, 5.0001, 50.0001, np.inf], labels=['1', '2-5', '6-50', '>50'])
    res = {}
    for (u, band), r in d1.groupby(['u', 'band'], observed=True)['r']:
        if u in ('g', 'ml', 'Kg', 'l'):
            res[f'{u}|N{band}'] = {'cells': int(len(r)), 'median': round(float(r.median()), 3),
                                   'within2x': round(float(((r > .5) & (r < 2)).mean()), 3)}
    out['w1_served_by_N'] = res
    return out


R.step('household_comparator', household_comparator)


# ------------------------------------------------ PP vs PH price sanity
def pp_vs_ph():
    out = {}
    pairs = {'2010Q3': '2011Q1', '2012Q3': '2013Q1', '2015Q3': '2016Q1', '2018Q3': '2019Q1',
             '2023Q3': '2024Q1'}
    for tpp, tph in pairs.items():
        a = cpr[cpr['t'].eq(tpp) & cpr['obs'].eq(1)]
        b = cpr[cpr['t'].eq(tph) & cpr['obs'].eq(1)].groupby(['v', 'j', 'u'])['Price'].median().rename('ph')
        m = a.merge(b.reset_index(), on=['v', 'j', 'u'])
        m['r'] = m['Price'] / m['ph']
        m['der'] = m['Derivation'].fillna('none')
        out[tpp] = {f'{d_}': {'cells': int(len(r)), 'median': round(float(r.median()), 3),
                              'within2x': round(float(((r > .5) & (r < 2)).mean()), 3)}
                    for d_, r in m.groupby('der')['r']}
    return out


R.step('pp_vs_ph', pp_vs_ph)
R.data['done'] = True
R.flush()
print('PROBE DONE', flush=True)
