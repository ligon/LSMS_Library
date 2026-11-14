"""Raw probe (no build) for the Togo 2021-22 inversion, the CIV 2021-22 H/L split and the
Togo 2018 unit codes 659/660/662.  usage: python probe.py OUT_JSON

Aggregates only; no microdata rows are written.
"""
import json, math, os, sys, collections
import numpy as np
import pandas as pd
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
from lsms_library import paths
root = os.path.realpath(str(paths.data_root()))
assert root.startswith('/local/job'), root
from lsms_library.local_tools import get_dataframe
R = paths.countries_root()
OUT = {}


def rd(p, labels):
    return get_dataframe(str(p), convert_categoricals=labels)


def grid(x, step, tol=1e-6):
    x = np.asarray(x, dtype=float)
    k = x / step
    return np.abs(k - np.round(k)) <= tol * np.maximum(1.0, np.abs(k))


def decimals(x, maxd=7, tol=1e-6):
    """Fewest decimals d with x*10^d an integer (relative tolerance tol); maxd+1 if none."""
    x = np.asarray(x, dtype=float)
    out = np.full(x.shape, maxd + 1)
    for d in range(maxd, -1, -1):
        y = x * 10 ** d
        ok = np.abs(y - np.round(y)) <= tol * np.maximum(1.0, np.abs(y))
        out = np.where(ok, d, out)
    return out


def vc(s, n=40):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).head(n).items()}


# --------------------------------------------------------------- Togo 2021-22
T = R / 'Togo' / '2021-22' / 'Data'
c7 = rd(T / 's07b_me_tgo2021.dta', False)
l7 = rd(T / 's07b_me_tgo2021.dta', True)
cov = rd(T / 's00_me_tgo2021.dta', True)
o = OUT['togo21'] = {}
o['s07b_columns'] = list(c7.columns)
o['dtypes'] = {k: str(c7[k].dtype) for k in c7.columns if k.startswith('s07bq0')}
o['rows'] = len(c7)
o['dup_grappe_menage_item'] = int(c7.duplicated(['grappe', 'menage', 's07bq01']).sum())
o['cover_region_labels'] = vc(cov['s00q01'])
reg_by_hh = cov.set_index(['grappe', 'menage'])['s00q01'].astype('string')
o['cover_dup_hh'] = int(reg_by_hh.index.duplicated().sum())
o['region_per_grappe_max'] = int(cov.groupby('grappe')['s00q01'].nunique().max())
key = pd.MultiIndex.from_arrays([c7['grappe'], c7['menage']])
region = pd.Series(reg_by_hh.reindex(key).to_numpy(), index=c7.index, dtype='string')
o['rows_without_region'] = int(region.isna().sum())
q3 = c7['s07bq03a'].astype(float); q4 = c7['s07bq04'].astype(float).fillna(0)
q5 = c7['s07bq05'].astype(float).fillna(0); q7 = c7['s07bq07a'].astype(float)
unit_l = l7['s07bq03b'].astype('string'); unit_c = c7['s07bq03b']
size_l = l7['s07bq03c'].astype('string')
when = l7['s07bq06'].astype('string')
o['s07bq06_labels'] = vc(when)
o['unit_label_by_code'] = {str(k): str(v) for k, v in
                           pd.DataFrame({'c': unit_c, 'l': unit_l}).dropna().drop_duplicates('c').set_index('c')['l'].items()}
o['q3_zero_rows'] = int((q3 == 0).sum()); o['q3_pos_rows'] = int((q3 > 0).sum()); o['q3_nan_rows'] = int(q3.isna().sum())
o['q3_zero_by_region'] = vc(region[q3 == 0])
own_or_gift = (q4 > 0) | (q5 > 0)
MULT = {'Centrale': 1.5, 'Savanes': 1.2, 'Plateaux': 1.1, 'Kara': 0.9, 'Maritime': 0.9}
regs = sorted(region.dropna().unique())
o['regions'] = regs
lome = [r for r in regs if 'Lom' in r]
o['lome_label'] = lome


def factor(reg, og):
    if reg in MULT:
        return 1.115 * MULT[reg]
    if reg in lome:
        return 1.2 if og else 1.115 * 0.82
    return np.nan


f = np.array([factor(r, g) for r, g in zip(region.fillna(''), own_or_gift)])
is_unite = unit_l.str.lower().str.strip().eq('unité').fillna(False).to_numpy()
pos = (q3 > 0).to_numpy()
inv = q3.to_numpy() / f
o['factor_values'] = vc(np.round(f[pos], 6))
# --- the forward check on own-production-only / gift-only rows
own_only = pos & (q4 > 0).to_numpy() & (q5 == 0).to_numpy()
gift_only = pos & (q4 == 0).to_numpy() & (q5 > 0).to_numpy()
tab = []
for reg in regs:
    for og in (False, True):
        for un in (False, True):
            m = pos & (region == reg).fillna(False).to_numpy() & (own_or_gift.to_numpy() == og) & (is_unite == un)
            if not m.any():
                continue
            fr = factor(reg, og)
            row = {'region': reg, 'own_or_gift': og, 'unite': un, 'rows': int(m.sum()), 'factor': fr,
                   'q3_integer': round(float(grid(q3[m], 1).mean()), 4),
                   'q3_on_quarter': round(float(grid(q3[m], 0.25).mean()), 4),
                   'inv_on_quarter': round(float(grid(inv[m], 0.25).mean()), 4),
                   'inv_on_001': round(float(grid(inv[m], 0.01).mean()), 4),
                   'inv_decimals': vc(decimals(inv[m]))}
            for nm, mm, base in (('own_only', own_only, q4), ('gift_only', gift_only, q5)):
                k = m & mm
                if k.any():
                    b = base.to_numpy()[k]; x = q3.to_numpy()[k]
                    row[f'{nm}_rows'] = int(k.sum())
                    row[f'{nm}_q3_eq_base_x_f'] = round(float(np.isclose(x, b * fr, rtol=1e-6, atol=0).mean()), 4)
                    row[f'{nm}_q3_eq_floor_base_x_f'] = round(float((np.abs(x - np.floor(b * fr + 1e-9)) <= 1e-6).mean()), 4)
                    row[f'{nm}_q3_eq_base'] = round(float((np.abs(x - b) <= 1e-6 * np.maximum(1, b)).mean()), 4)
                    if nm == 'own_only':
                        row['own_only_mode_q3_over_q4'] = vc(np.round(x / b, 4), 3)
            tab.append(row)
o['by_region_class_unit'] = tab
# --- Which units are truncated?  Own-only rows in the three regions with f >= 1.2.
big = pos & own_only & region.isin(['Centrale', 'Savanes', 'Plateaux']).fillna(False).to_numpy()
trunc = {}
for u in pd.unique(unit_l[big].dropna()):
    k = big & (unit_l == u).fillna(False).to_numpy()
    b = q4.to_numpy()[k]; x = q3.to_numpy()[k]; fr = f[k]
    exact = np.isclose(x, b * fr, rtol=1e-6, atol=0)
    floored = (np.abs(x - np.floor(b * fr + 1e-9)) <= 1e-6) & ~exact
    trunc[str(u)] = {'rows': int(k.sum()), 'exact': int(exact.sum()), 'floored_not_exact': int(floored.sum()),
                     'neither': int((~exact & ~floored).sum())}
o['truncation_by_unit_bigf_own_only'] = dict(sorted(trunc.items(), key=lambda kv: -kv[1]['rows']))
# --- unité round trip, f >= 1
un_rows = pos & is_unite
geq = un_rows & (f >= 1)
k = np.ceil(q3.to_numpy() / f - 1e-9)
rt = np.abs(np.floor(k * f + 1e-9) - q3.to_numpy()) <= 1e-9
q3int = grid(q3, 1)
o['unite_f_ge_1'] = {'rows': int(geq.sum()), 'q3_integer': int((geq & q3int).sum()),
                     'roundtrip_ok': int((geq & q3int & rt).sum()),
                     'roundtrip_fail_integer': int((geq & q3int & ~rt).sum()),
                     'non_integer_q3': int((geq & ~q3int).sum()),
                     'non_integer_q3_on_grid_after_division': int((geq & ~q3int & grid(inv, 0.25)).sum()),
                     'k_differs_from_q3': int((geq & q3int & rt & (k != q3.to_numpy())).sum()),
                     'by_region': {r: int((geq & (region == r).fillna(False).to_numpy()).sum()) for r in regs},
                     'fail_by_region': {r: int((geq & q3int & ~rt & (region == r).fillna(False).to_numpy()).sum()) for r in regs},
                     'lome_own_gift_unite_exact_1p2': int((geq & np.isin(region.fillna(''), lome) & own_or_gift.to_numpy()
                                                           & grid(inv, 1)).sum())}
# Lomé own/gift unité rows: exact x1.2 (no floor)?
lg = un_rows & np.isin(region.fillna(''), lome) & own_or_gift.to_numpy()
o['lome_own_gift_unite'] = {'rows': int(lg.sum()), 'inv_integer': int((lg & grid(inv, 1)).sum()),
                            'inv_on_quarter': int((lg & grid(inv, 0.25)).sum()),
                            'q3_integer': int((lg & q3int).sum())}
lp = un_rows | ((q3 == 0).to_numpy() & is_unite)
lp = lp & np.isin(region.fillna(''), lome) & ~own_or_gift.to_numpy()
o['lome_purchase_only_unite'] = {'rows_q3_ge_0': int(lp.sum()), 'q3_zero': int((lp & (q3 == 0).to_numpy()).sum()),
                                 'q3_pos': int((lp & pos).sum()), 'q3_integer': int((lp & q3int).sum()),
                                 'q3_values': vc(q3[lp], 15), 'when': vc(when[lp], 10),
                                 'q7_same_unit_is_int': int((lp & (c7['s07bq07b'] == unit_c).to_numpy() & grid(q7, 1)).sum()),
                                 'q3_eq_floor_q7_x_f': int((lp & (c7['s07bq07b'] == unit_c).to_numpy()
                                                            & (np.abs(q3.to_numpy() - np.floor(q7.to_numpy() * 0.9143 + 1e-9)) <= 1e-9)).sum())}
# q3 == 0 rows elsewhere
z = (q3 == 0).to_numpy()
o['q3_zero'] = {'rows': int(z.sum()), 'unite': int((z & is_unite).sum()),
                'by_region_unite': vc(region[z & is_unite]), 'by_region_other': vc(region[z & ~is_unite]),
                'own_or_gift': int((z & own_or_gift.to_numpy()).sum()), 'when': vc(when[z], 10)}
# --- phantoms on Jamais rows
jam = when.str.strip().str.lower().eq('jamais').fillna(False).to_numpy()
res_filed = (q3.fillna(0) - q4 - q5).to_numpy()
res_inv = np.where(np.isnan(inv), np.nan, inv) - q4.to_numpy() - q5.to_numpy()
o['jamais'] = {'rows': int(jam.sum()), 'q3_pos': int((jam & pos).sum()),
               'filed_residual_pos': int((jam & (res_filed > 0)).sum()),
               'inv_residual_pos_raw': int((jam & pos & (res_inv > 0)).sum()),
               'inv_residual_gt_1e-9': int((jam & pos & (res_inv > 1e-9)).sum()),
               'inv_residual_gt_1e-6rel': int((jam & pos & (res_inv > 1e-6 * np.maximum(1, inv))).sum()),
               'inv_residual_gt_1e-4rel': int((jam & pos & (res_inv > 1e-4 * np.maximum(1, inv))).sum()),
               'inv_residual_gt_0.01': int((jam & pos & (res_inv > 0.01)).sum()),
               'abs_residual_quantiles_small': [float(x) for x in np.nanquantile(
                   np.abs(res_inv[jam & pos & (np.abs(res_inv) < 1e-3)]), [0.5, 0.9, 0.99, 1.0])]
               if (jam & pos & (np.abs(res_inv) < 1e-3)).any() else None,
               'by_region_after': {r: int((jam & pos & (res_inv > 1e-6 * np.maximum(1, inv)) & (region == r).fillna(False).to_numpy()).sum()) for r in regs},
               'by_region_before': {r: int((jam & (res_filed > 0) & (region == r).fillna(False).to_numpy()).sum()) for r in regs}}
# rows where q3 < q4 + q5 after inversion (beyond float noise): constraint violation created by dividing
viol = pos & (res_inv < -1e-6 * np.maximum(1, inv))
o['inv_below_q4_plus_q5'] = {'rows': int(viol.sum()), 'by_region': {r: int((viol & (region == r).fillna(False).to_numpy()).sum()) for r in regs},
                             'filed_below': int((pos & (res_filed < -1e-9)).sum())}
# float noise of inverted value
o['inv_float_noise'] = {'rel_dev_from_6sig_q50_q99_max': [float(x) for x in np.nanquantile(
    np.abs(inv[pos] - np.array([float(f'{v:.6g}') for v in inv[pos]])) / np.maximum(1e-12, inv[pos]), [0.5, 0.99, 1.0])]}
# Totals by region (purchased residual, filed vs inverted, clipped)
o['purchased_residual_by_region'] = {r: {'filed': float(np.clip(res_filed[(region == r).fillna(False).to_numpy() & pos], 0, None).sum()),
                                         'inverted': float(np.clip(np.nan_to_num(res_inv[(region == r).fillna(False).to_numpy() & pos]), 0, None).sum())}
                                     for r in regs}
# Codes 659/660/662 in 2021-22 s07b, by item
for code in (659, 660, 662):
    m = (unit_c == code).to_numpy()
    o[f'code_{code}'] = {'label': vc(unit_l[m], 3), 'items': vc(l7['s07bq01'][m].astype('string'), 12), 'rows': int(m.sum())}
# NSU file
nsu = rd(T / 'ehcvm_nsu_tgo2021.dta', True)
nsuc = rd(T / 'ehcvm_nsu_tgo2021.dta', False)
o['nsu_columns'] = list(nsu.columns)[:40]
ucol = 'uniteID' if 'uniteID' in nsu.columns else None
if ucol:
    pcol = [c for c in nsu.columns if c.lower().startswith('produit')]
    pcol = pcol[0] if pcol else None
    for code in (659, 660, 662):
        m = (nsuc[ucol] == code).to_numpy()
        o[f'nsu_{code}'] = {'label': vc(nsu[ucol][m].astype('string'), 3),
                            'items': vc(nsu[pcol][m].astype('string'), 12) if pcol else None, 'rows': int(m.sum())}

# ------------------------------------------------------------------- Togo 2018
T18 = R / 'Togo' / '2018' / 'Data1'
c18 = rd(T18 / 's07b_me_tgo2018.dta', False)
l18 = rd(T18 / 's07b_me_tgo2018.dta', True)
o18 = OUT['togo18'] = {'dtypes': {k: str(c18[k].dtype) for k in c18.columns if k.startswith('s07bq0')}}
o18['s07bq03b_dtype_labels'] = str(l18['s07bq03b'].dtype)
for code in (114, 659, 660, 662):
    m = (pd.to_numeric(c18['s07bq03b'], errors='coerce') == code).to_numpy()
    q = c18['s07bq03a'].astype(float)
    o18[f'code_{code}'] = {'rows': int(m.sum()), 'rows_q3_pos': int((m & (q > 0).to_numpy()).sum()),
                           'items': vc(l18['s07bq01'][m].astype('string'), 15),
                           'sizes': vc(l18['s07bq03c'][m].astype('string'), 10),
                           'label_in_file': vc(l18['s07bq03b'][m].astype('string'), 3),
                           'purchase_unit_same_code': int((m & (pd.to_numeric(c18['s07bq07b'], errors='coerce') == code).to_numpy()).sum()),
                           'own': int((m & (c18['s07bq04'].fillna(0) > 0).to_numpy()).sum()),
                           'gift': int((m & (c18['s07bq05'].fillna(0) > 0).to_numpy()).sum())}

# ------------------------------------------------------------------ CIV 2021-22
C = R / 'CotedIvoire' / '2021-22' / 'Data'
cc = rd(C / 's07b_me_civ2021.dta', False)
oc = OUT['civ21'] = {'dtypes': {k: str(cc[k].dtype) for k in cc.columns if k.startswith('s07bq0')},
                     'rows': len(cc), 'dup_grappe_menage_item': int(cc.duplicated(['grappe', 'menage', 's07bq01']).sum())}


def hl(frame, label):
    q3 = frame['s07bq03a'].astype(float).to_numpy()
    q4 = frame['s07bq04'].astype(float).fillna(0).to_numpy()
    q5 = frame['s07bq05'].astype(float).fillna(0).to_numpy()
    pos = q3 > 0
    on = grid(q3, 0.25)
    out = {}
    for tol in (1e-9, 1e-7, 1e-6, 1e-5):
        d = decimals(q3, tol=tol)
        H = pos & ~on & (d >= 4)
        L = pos & ~on & (d <= 3)
        out[f'tol_{tol:g}'] = {'on_grid': int((pos & on).sum()), 'H': int(H.sum()), 'L': int(L.sum())}
    d = decimals(q3, tol=1e-6)
    H = pos & ~on & (d >= 4); L = pos & ~on & (d <= 3)
    res = q3 - q4 - q5
    out['H_filed_residual_le0'] = int((H & (res <= 1e-9)).sum())
    out['H_filed_residual_le0_with_own_or_gift'] = int((H & (res <= 1e-9) & ((q4 > 0) | (q5 > 0))).sum())
    out['H_q4_pos'] = int((H & (q4 > 0)).sum()); out['H_q4_offgrid'] = int((H & (q4 > 0) & ~grid(q4, 0.25)).sum())
    out['H_q4_4dec'] = int((H & (q4 > 0) & (decimals(q4) >= 4)).sum())
    out['H_q5_pos'] = int((H & (q5 > 0)).sum()); out['H_q5_offgrid'] = int((H & (q5 > 0) & ~grid(q5, 0.25)).sum())
    out['L_q4_pos'] = int((L & (q4 > 0)).sum()); out['L_q4_offgrid'] = int((L & (q4 > 0) & ~grid(q4, 0.25)).sum())
    out['L_q5_pos'] = int((L & (q5 > 0)).sum())
    out['L_filed_residual_le0'] = int((L & (res <= 1e-9)).sum())
    out['decimals_hist_offgrid'] = vc(d[pos & ~on])
    # borderline: decimals differing between tolerances 1e-7 and 1e-5
    d7, d5 = decimals(q3, tol=1e-7), decimals(q3, tol=1e-5)
    out['borderline_H_changes_1e-7_vs_1e-5'] = int((pos & ~on & ((d7 >= 4) != (d5 >= 4))).sum())
    return out


oc['hl'] = hl(cc, 'civ21')
c18c = rd(R / 'CotedIvoire' / '2018-19' / 'Data' / 'Menage' / 's07b_me_CIV2018.dta', False)
oc['hl_2018_control'] = hl(c18c, 'civ18')
oc['dtypes_2018'] = {k: str(c18c[k].dtype) for k in c18c.columns if k.startswith('s07bq03a')}

json.dump(OUT, open(sys.argv[1], 'w'), indent=1, default=str)
print('DONE')
