import warnings, json, os
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T
warnings.simplefilter('ignore')
OLD = {k: v for k, v in T._NITROGEN_CONTENT.items() if k not in ('nps', 'd compound')}
res = {}
for c in ['Ethiopia', 'Malawi']:
    pi = ll.Country(c).plot_inputs()
    pf = ll.Country(c).plot_features()
    b = T.nitrogen_kg(pi, nitrogen_content=OLD); a = T.nitrogen_kg(pi)
    bw = b.groupby('t')['Nitrogen_kg'].agg(['sum', 'size']); aw = a.groupby('t')['Nitrogen_kg'].agg(['sum', 'size'])
    print(c, 'per wave (before sum, plots | after sum, plots):')
    print(pd.concat({'before': bw, 'after': aw}, axis=1).round(1).to_string())
    # product basis
    pb = T.nitrogen_kg(pi, nitrogen_content={k: 1.0 for k in OLD})['Nitrogen_kg'].sum()
    pa = T.nitrogen_kg(pi, nitrogen_content={k: 1.0 for k in T._NITROGEN_CONTENT})['Nitrogen_kg'].sum()
    print(c, f'fertilizer PRODUCT kg (nutrient=product numerator): {pb:,.1f} -> {pa:,.1f} ({pa-pb:+,.1f})')
    rb = T.fertilizer_rate(pi, pf, nutrient='product'); ra = T.fertilizer_rate(pi, pf, nutrient='product')
    nb = T.fertilizer_rate(pi, pf) if False else None
    # rates: compute with old map by monkeypatching
    saved = dict(T._NITROGEN_CONTENT)
    try:
        T._NITROGEN_CONTENT.clear(); T._NITROGEN_CONTENT.update(OLD)
        rN_b = T.fertilizer_rate(pi, pf); rP_b = T.fertilizer_rate(pi, pf, nutrient='product')
    finally:
        T._NITROGEN_CONTENT.clear(); T._NITROGEN_CONTENT.update(saved)
    rN_a = T.fertilizer_rate(pi, pf); rP_a = T.fertilizer_rate(pi, pf, nutrient='product')
    print(c, f"fertilizer_rate N: rows {len(rN_b):,} -> {len(rN_a):,}; median {rN_b.iloc[:,0].median():.2f} -> {rN_a.iloc[:,0].median():.2f}; sum {rN_b.iloc[:,0].sum():,.1f} -> {rN_a.iloc[:,0].sum():,.1f}")
    print(c, f"fertilizer_rate product: rows {len(rP_b):,} -> {len(rP_a):,}; median {rP_b.iloc[:,0].median():.2f} -> {rP_a.iloc[:,0].median():.2f}; sum {rP_b.iloc[:,0].sum():,.1f} -> {rP_a.iloc[:,0].sum():,.1f}")
    if c == 'Malawi':
        m = pi.index.get_level_values('input').astype(str) == 'D Compound Fertilizer'
        dk = T.nitrogen_kg(pi[m], nitrogen_content={'d compound': 1.0})['Nitrogen_kg'].sum()
        print(c, f'D Compound product kg {dk:,.1f}; N at 0.08 {0.08*dk:,.1f}; at WB 0.07 {0.07*dk:,.1f}')
        m2 = pi.index.get_level_values('input').astype(str) == 'NPK Fertilizer'
        nk = T.nitrogen_kg(pi[m2], nitrogen_content={'npk': 1.0})['Nitrogen_kg'].sum()
        print(c, f'NPK product kg {nk:,.1f}; N at map 0.17 {0.17*nk:,.1f}; at 23:21:0+4S 0.23 {0.23*nk:,.1f}')
