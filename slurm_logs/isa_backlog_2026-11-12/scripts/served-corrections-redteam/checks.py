"""Candidate-side raw checks and derivation_inputs (runs BEFORE build.py in the cand root).

Usage: python checks.py <frames_dir>
Aggregates only -> $RESULTS/cand_checks.json; record-level frames (private) -> <frames_dir>.
Every source is read through lsms_library.local_tools.get_dataframe.
"""
import json
import os
import sys
import traceback
import warnings

import numpy as np
import pandas as pd

frames = sys.argv[1]
import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.paths import data_root, countries_root
assert os.path.realpath(str(data_root())).startswith(
    '/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/')
import lsms_library as ll
from lsms_library import local_tools as lt

warnings.simplefilter('ignore')
R = countries_root()
OUT = {}


def raw(country, wave, name, cats=False):
    return lt.get_dataframe(str(R / country / wave / 'Data' / name), convert_categoricals=cats)


def num(s):
    return pd.to_numeric(s, errors='coerce').to_numpy(dtype=float)


def save(df, name):
    df.to_pickle(os.path.join(frames, name + '.pkl'))


def keyset(df):
    return sorted(map(tuple, df.index.to_frame().astype(str).values.tolist()))


def run(name, f):
    o = {}
    try:
        f(o)
    except Exception as e:
        o['ERROR'] = f'{type(e).__name__}: {e}'
        o['tb'] = traceback.format_exc()[-3000:]
    OUT[name] = o
    print(name, json.dumps(o, default=str)[:4000], flush=True)
    json.dump(OUT, open(os.path.join(os.environ['RESULTS'], 'cand_checks.json'), 'w'), indent=1, default=str)


def ndecimals(x):
    out = []
    for v in x:
        if not np.isfinite(v):
            out.append(-1)
            continue
        r = repr(float(v)).lower()
        if 'e' in r:
            m, e = r.split('e')
            d = len(m.split('.')[1]) if '.' in m else 0
            out.append(max(0, d - int(e)))
        else:
            out.append(len(r.split('.')[1].rstrip('0')) if '.' in r else 0)
    return np.array(out)


def inputs7(country, wave, n, q3):
    key = f'{country}::food_acquired::7day-purchase-at-last-purchase-unit-value'
    inp = ll.Country(country).derivation_inputs(key, wave=wave).reset_index()
    assert len(inp) == n, (len(inp), n)
    assert np.array_equal(num(inp['s07bq03a']), q3, equal_nan=True)
    return inp


def code_labels(country, wave, name, var):
    codes = raw(country, wave, name)[var]
    labs = raw(country, wave, name, cats=True)[var]
    t = pd.DataFrame({'c': num(codes), 'l': labs.astype(str).str.strip()}).dropna(subset=['c'])
    return {str(int(c)): sorted(set(g['l']))[:3] for c, g in t.groupby('c')}


# ----------------------------------------------------------------- Mali
def mali(o):
    s = raw('Mali', '2018-19', 's07b_me_mli2018.dta')
    vague = num(s['vague'])
    block = np.isnan(vague)
    prod, unit, size = num(s['s07bq01']), num(s['s07bq03b']), num(s['s07bq03c'])
    q3, q4, q5, q6 = num(s['s07bq03a']), num(s['s07bq04']), num(s['s07bq05']), num(s['s07bq06'])
    q7, q8 = num(s['s07bq07a']), num(s['s07bq08'])
    fp = (prod == 1) & (unit == 100) & (size == 0)
    genuine_rice = fp & ~block & (q3 > 0)
    inp = inputs7('Mali', '2018-19', len(s), q3)
    rec = pd.DataFrame({'t': inp['t'].astype(str), 'i': inp['i'].astype(str), 'j': inp['j'].astype(str),
                        'block': block, 'genuine_rice_kg': genuine_rice, 'q3': q3, 'unit': unit,
                        'purch': np.clip(np.nan_to_num(q3) - np.nan_to_num(q4) - np.nan_to_num(q5), 0, None)})
    save(rec, 'mali_records')
    o['rows'] = len(s)
    o['vague_nan'] = int(block.sum())
    o['block_all_rice_kg_sizefree'] = bool(fp[block].all())
    o['block_s07bq06_values'] = {str(k): int(v) for k, v in pd.Series(q6[block]).value_counts(dropna=False).items()}
    o['s07bq06_labels'] = code_labels('Mali', '2018-19', 's07b_me_mli2018.dta', 's07bq06')
    o['product1_label'] = code_labels('Mali', '2018-19', 's07b_me_mli2018.dta', 's07bq01').get('1')
    for nm, arr in (('q4', q4), ('q5', q5), ('q7a', q7), ('q8', q8)):
        o[f'block_{nm}_nonzero'] = int((np.nan_to_num(arr[block]) != 0).sum())
    m = q3 / 1.4
    whole = (q3 > 0) & (np.abs(m - np.round(m)) < 1e-9) & (np.round(m) >= 1)
    o['block_whole_multiple_1p4'] = int((whole & block).sum())
    w = raw('Mali', '2018-19', 'ehcvm_welfare_mli2018.dta')
    hh = pd.Series(num(w['hhsize']), index=pd.MultiIndex.from_arrays([num(w['grappe']), num(w['menage'])]))
    o['welfare_key_unique'] = bool(not hh.index.duplicated().any())
    hhs = hh.reindex(pd.MultiIndex.from_arrays([num(s['grappe']), num(s['menage'])])).to_numpy()
    eq = np.abs(q3 - 1.4 * hhs) < 1e-9
    o['block_eq_1p4_hhsize'] = int((eq & block).sum())
    o['block_no_welfare'] = int((np.isnan(hhs) & block).sum())
    o['genuine_rice_kg_records'] = int(genuine_rice.sum())
    o['genuine_rice_kg_eq_1p4_hhsize'] = int((eq & genuine_rice).sum())
    o['genuine_rice_kg_whole_multiple_1p4'] = int((whole & genuine_rice).sum())
    bhh = set(zip(num(s['grappe'])[block], num(s['menage'])[block]))
    ghh = set(zip(num(s['grappe'])[genuine_rice], num(s['menage'])[genuine_rice]))
    o['block_households'] = len(bhh)
    o['households_with_block_and_genuine_rice_kg'] = len(bhh & ghh)
    o['block_q3_sum'] = round(float(np.nansum(q3[block])), 3)
    # test_ehcvm_unit_weights._withheld drops every record sharing (i, j) with a withheld one
    bij = set(zip(rec.loc[rec['block'], 'i'], rec.loc[rec['block'], 'j']))
    shares = rec['block'].eq(False) & pd.Series(list(zip(rec['i'], rec['j']))).isin(bij).to_numpy()
    o['withheld_ij_filter_drops_genuine_records'] = int(shares.sum())
    o['withheld_ij_filter_drops_genuine_records_q3pos'] = int((shares & (q3 > 0)).sum())
    key = 'Mali::food_acquired::rice-ration-block-withheld'
    ri = ll.Country('Mali').derivation_inputs(key, wave='2018-19')
    save(ri, 'mali_rice_inputs')
    o['inputs_rows'] = len(ri)
    o['inputs_index'] = list(ri.index.names)
    o['inputs_all_vague_na'] = bool(pd.to_numeric(ri['vague'], errors='coerce').isna().all())
    o['inputs_q3_sum'] = round(float(pd.to_numeric(ri['s07bq03a']).sum()), 3)
    o['inputs_index_equals_block_records'] = keyset(ri) == keyset(rec[rec['block']].set_index(['t', 'i', 'j']))
    o['inputs_index_unique'] = bool(not ri.index.duplicated().any())
    o['inputs_j_values'] = sorted(set(map(str, ri.index.get_level_values('j'))))
    o['registry_rows'] = getattr(ll.Country('Mali').derivations()[key], 'rows', None)


# --------------------------------------------------------------- Senegal
def salt_codes(wave, fname):
    lab = code_labels('Senegal', wave, fname, 's07bq01')
    return sorted(int(c) for c, ls in lab.items()
                  if any(pd.Series([l]).str.lower().str.replace(r'^\d+\s*[.\-]\s*', '', regex=True).iloc[0] == 'sel' for l in ls))


def senegal_salt(o):
    s = raw('Senegal', '2021-22', 's07b_me_sen2021.dta')
    prod, unit, size, q3 = num(s['s07bq01']), num(s['s07bq03b']), num(s['s07bq03c']), num(s['s07bq03a'])
    w = raw('Senegal', '2021-22', 'ehcvm_welfare_sen2021.dta')
    hh = pd.Series(num(w['hhsize']), index=pd.MultiIndex.from_arrays([num(w['grappe']), num(w['menage'])]))
    hhs = hh.reindex(pd.MultiIndex.from_arrays([num(s['grappe']), num(s['menage'])])).to_numpy()
    cell = (prod == 139) & (unit == 100) & (size == 0) & (q3 > 0)
    d = np.abs(q3 - 0.0199997 * hhs)
    sel = cell & (d < 1e-9)
    inp = inputs7('Senegal', '2021-22', len(s), q3)
    rec = pd.DataFrame({'t': inp['t'].astype(str), 'i': inp['i'].astype(str), 'j': inp['j'].astype(str),
                        'salt_sel': sel, 'salt_cell': cell})
    save(rec, 'senegal21_records')
    o['welfare_key_unique'] = bool(not hh.index.duplicated().any())
    o['sel_codes_from_labels'] = salt_codes('2021-22', 's07b_me_sen2021.dta')
    o['unit100_label'] = code_labels('Senegal', '2021-22', 's07b_me_sen2021.dta', 's07bq03b').get('100')
    o['salt_kg_records_q3pos'] = int(cell.sum())
    o['salt_kg_no_hhsize'] = int((cell & np.isnan(hhs)).sum())
    for tol in (1e-12, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3):
        o[f'salt_kg_within_{tol:g}'] = int((cell & (d < tol)).sum())
        o[f'any_record_within_{tol:g}'] = int(((q3 > 0) & (d < tol)).sum())
    d02 = np.abs(q3 - 0.02 * hhs)
    o['salt_kg_exact_0p02'] = int((cell & (d02 == 0)).sum())
    o['salt_kg_within_1e-6_of_0p02'] = int((cell & (d02 < 1e-6)).sum())
    o['selected'] = int(sel.sum())
    o['selected_max_abs_dev'] = float(np.nanmax(d[sel])) if sel.any() else None
    o['selected_hhsize_distribution'] = {str(int(k)): int(v) for k, v in pd.Series(hhs[sel]).value_counts().sort_index().items()}
    o['selected_households'] = len(set(zip(num(s['grappe'])[sel], num(s['menage'])[sel])))
    # hhsize-free alternative: a whole multiple of 0.0199997
    mm = q3 / 0.0199997
    mult = cell & (np.abs(mm - np.round(mm)) < 1e-6) & (np.round(mm) >= 1)
    o['hhsize_free_whole_multiple_selects'] = int(mult.sum())
    o['hhsize_free_equals_selected'] = bool(np.array_equal(mult, sel))
    o['hhsize_free_extra_not_selected'] = int((mult & ~sel).sum())
    o['hhsize_free_multiple_equals_hhsize_on_selected'] = int((np.isclose(np.round(mm), hhs) & sel).sum())
    for tol in (1e-9, 1e-7, 1e-5):
        mt = cell & (np.abs(mm - np.round(mm)) < tol) & (np.round(mm) >= 1)
        o[f'hhsize_free_multiple_tol_{tol:g}'] = int(mt.sum())
    pc = pd.Series((q3 / hhs)[cell]).round(7).value_counts().head(5)
    o['salt_kg_per_capita_top5'] = {str(k): int(v) for k, v in pc.items()}
    q4, q5, q8 = num(s['s07bq04']), num(s['s07bq05']), num(s['s07bq08'])
    o['selected_with_q4'] = int((np.nan_to_num(q4[sel]) > 0).sum())
    o['selected_with_q5'] = int((np.nan_to_num(q5[sel]) > 0).sum())
    o['selected_whole_gift'] = int((np.nan_to_num(q5[sel]) >= q3[sel]).sum())
    o['selected_with_q8'] = int((np.nan_to_num(q8[sel]) > 0).sum())
    other = (prod == 139) & ~((unit == 100) & (size == 0)) & (q3 > 0) & np.isfinite(hhs)
    vc = pd.Series(q3[other] / hhs[other]).round(7).value_counts()
    o['sel_other_units_top_per_capita'] = {str(k): int(v) for k, v in vc.head(5).items()}
    sij = set(zip(rec.loc[rec['salt_sel'], 'i'], rec.loc[rec['salt_sel'], 'j']))
    shares = (~rec['salt_sel']).to_numpy() & pd.Series(list(zip(rec['i'], rec['j']))).isin(sij).to_numpy()
    o['withheld_ij_filter_drops_other_records'] = int((shares & (q3 > 0)).sum())
    # same constant in 2018-19
    s8 = raw('Senegal', '2018-19', 's07b_me_sen2018.dta')
    w8 = raw('Senegal', '2018-19', 'ehcvm_welfare_sen2018.dta')
    hh8 = pd.Series(num(w8['hhsize']), index=pd.MultiIndex.from_arrays([num(w8['grappe']), num(w8['menage'])]))
    hhs8 = hh8.reindex(pd.MultiIndex.from_arrays([num(s8['grappe']), num(s8['menage'])])).to_numpy()
    codes8 = salt_codes('2018-19', 's07b_me_sen2018.dta')
    o['sel_codes_2018'] = codes8
    q38 = num(s8['s07bq03a'])
    cell8 = np.isin(num(s8['s07bq01']), codes8) & (num(s8['s07bq03b']) == 100) & (q38 > 0)
    o['2018_salt_kg_records'] = int(cell8.sum())
    o['2018_salt_kg_within_1e-9_of_0p0199997_hh'] = int((cell8 & (np.abs(q38 - 0.0199997 * hhs8) < 1e-9)).sum())
    o['2018_salt_kg_within_1e-9_of_0p02_hh'] = int((cell8 & (np.abs(q38 - 0.02 * hhs8) < 1e-9)).sum())
    key = 'Senegal::food_acquired::salt-per-capita-withheld'
    ri = ll.Country('Senegal').derivation_inputs(key, wave='2021-22')
    save(ri, 'senegal_salt_inputs')
    o['inputs_rows'] = len(ri)
    o['inputs_index_equals_selected'] = keyset(ri) == keyset(rec[rec['salt_sel']].set_index(['t', 'i', 'j']))


def senegal_near_zero(o):
    s = raw('Senegal', '2018-19', 's07b_me_sen2018.dta')
    q3, q7, q8 = num(s['s07bq03a']), num(s['s07bq07a']), num(s['s07bq08'])
    hit = (q3 > 0) & (q3 < 1e-3) & (q7 > 0) & (q7 < 1e-3)
    inp = inputs7('Senegal', '2018-19', len(s), q3)
    rec = pd.DataFrame({'t': inp['t'].astype(str), 'i': inp['i'].astype(str), 'j': inp['j'].astype(str), 'hit': hit})
    save(rec, 'senegal18_records')
    o['hit'] = int(hit.sum())
    o['q3_pos_below_1e-3'] = int(((q3 > 0) & (q3 < 1e-3)).sum())
    o['q7_pos_below_1e-3'] = int(((q7 > 0) & (q7 < 1e-3)).sum())
    o['q3_pos_below_1e-2'] = int(((q3 > 0) & (q3 < 1e-2)).sum())
    o['ratio_q3_q7_all_40'] = bool(np.allclose(q3[hit] / q7[hit], 40.0))
    o['products'] = sorted(set(num(s['s07bq01'])[hit].tolist()))
    o['q8_present'] = int(np.isfinite(q8[hit]).sum())
    o['q4_q5_nonzero'] = int((np.nan_to_num(num(s['s07bq04'])[hit]) != 0).sum() + (np.nan_to_num(num(s['s07bq05'])[hit]) != 0).sum())
    same = (num(s['s07bq03b'])[hit] == num(s['s07bq07b'])[hit]) & (num(s['s07bq03c'])[hit] == num(s['s07bq07c'])[hit])
    o['same_unit_and_size_as_purchase'] = int(same.sum())
    imp = q3[hit] / q7[hit] * q8[hit]
    o['implied_expenditure_finite'] = int(np.isfinite(imp[same]).sum())
    o['implied_expenditure_sum_same_unit'] = float(np.nansum(imp[same]))
    o['households'] = len(set(zip(num(s['grappe'])[hit], num(s['menage'])[hit])))
    s21 = raw('Senegal', '2021-22', 's07b_me_sen2021.dta')
    a, b = num(s21['s07bq03a']), num(s21['s07bq07a'])
    o['2021_hit'] = int(((a > 0) & (a < 1e-3) & (b > 0) & (b < 1e-3)).sum())
    key = 'Senegal::food_acquired::near-zero-quantity-withheld'
    ri = ll.Country('Senegal').derivation_inputs(key, wave='2018-19')
    save(ri, 'senegal_nz_inputs')
    o['inputs_rows'] = len(ri)
    o['inputs_index_equals_hit'] = keyset(ri) == keyset(rec[rec['hit']].set_index(['t', 'i', 'j']))


# ----------------------------------------------------------------- Niger
def niger(o):
    s = raw('Niger', '2018-19', 's07b_me_ner2018.dta')
    q3, q4, q5, q6 = num(s['s07bq03a']), num(s['s07bq04']), num(s['s07bq05']), num(s['s07bq06'])
    q7, q8 = num(s['s07bq07a']), num(s['s07bq08'])
    pos = q7 > 0
    off = pos & (np.abs(4 * q7 - np.round(4 * q7)) >= 1e-6)
    inp = inputs7('Niger', '2018-19', len(s), q3)
    rec = pd.DataFrame({'t': inp['t'].astype(str), 'i': inp['i'].astype(str), 'j': inp['j'].astype(str),
                        'flag': off, 'purch': np.clip(np.nan_to_num(q3) - np.nan_to_num(q4) - np.nan_to_num(q5), 0, None)})
    save(rec, 'niger_records')
    o['rows'] = len(s)
    o['s07bq06_labels'] = code_labels('Niger', '2018-19', 's07b_me_ner2018.dta', 's07bq06')
    o['q7_positive'] = int(pos.sum())
    o['flagged'] = int(off.sum())
    o['flagged_share'] = round(float(off.sum() / pos.sum()), 4)
    o['flagged_s07bq06'] = {str(k): int(v) for k, v in pd.Series(q6[off]).value_counts(dropna=False).items()}
    enabled = np.isin(q6, [1, 2, 3])
    o['flagged_enabled'] = int((off & enabled).sum())
    o['flagged_not_enabled'] = int((off & ~enabled).sum())
    dis = pos & ~enabled
    o['q7_filled_where_disabled'] = int(dis.sum())
    o['q7_filled_where_disabled_by_q6'] = {str(k): int(v) for k, v in pd.Series(q6[dis]).value_counts(dropna=False).items()}
    o['disabled_off_grid'] = int((dis & off).sum())
    o['disabled_q8_present'] = int((dis & np.isfinite(q8) & (q8 > 0)).sum())
    o['disabled_q7_eq_q3'] = int((dis & (q7 == q3)).sum())
    nd = ndecimals(q7[off])
    o['flagged_decimals_ge13'] = int((nd >= 13).sum())
    o['flagged_decimals_le3'] = int((nd <= 3).sum())
    q8int = np.isfinite(q8) & (np.abs(q8 - np.round(q8)) < 1e-9)
    o['flagged_q8_present'] = int((off & np.isfinite(q8) & (q8 > 0)).sum())
    o['flagged_q8_integer'] = int((off & q8int & (q8 > 0)).sum())
    o['flagged_q8_noninteger'] = int((off & np.isfinite(q8) & (q8 > 0) & ~q8int).sum())
    o['ongrid_q8_present'] = int((pos & ~off & np.isfinite(q8) & (q8 > 0)).sum())
    o['ongrid_q8_noninteger'] = int((pos & ~off & np.isfinite(q8) & (q8 > 0) & ~q8int).sum())
    w = raw('Niger', '2018-19', 'ehcvm_welfare_ner2018.dta')
    reg = pd.Series(num(w['region']), index=pd.MultiIndex.from_arrays([num(w['grappe']), num(w['menage'])]))
    region = reg.reindex(pd.MultiIndex.from_arrays([num(s['grappe']), num(s['menage'])])).to_numpy()
    df = pd.DataFrame({'p': num(s['s07bq01']), 'u': num(s['s07bq07b']), 'z': num(s['s07bq07c']), 'r': region,
                       'uv': q8 / q7, 'q8': q8, 'off': off, 'q8int': q8int})
    f = df[df['off'] & np.isfinite(df['uv']) & (df['uv'] > 0)].copy()
    f['uvk'] = f['uv'].map(lambda x: float(f'{x:.6g}'))
    mode = f.groupby(['p', 'u', 'z', 'r'])['uvk'].agg(lambda x: x.value_counts().index[0])
    f = f.join(mode.rename('P'), on=['p', 'u', 'z', 'r'])
    at = f['uvk'] == f['P']
    o['flagged_valued'] = len(f)
    o['cells_with_flagged'] = int(mode.size)
    o['flagged_at_cell_mode_P'] = int(at.sum())
    o['flagged_at_P_q8_integer'] = int((at & f['q8int']).sum())
    o['flagged_at_P_q8_noninteger'] = int((at & ~f['q8int']).sum())
    o['flagged_noninteger_q8_at_P'] = int((~f['q8int'] & at).sum())
    o['flagged_noninteger_q8_total'] = int((~f['q8int']).sum())
    g = f[at].groupby(['p', 'u', 'z', 'r'])['q8'].nunique()
    o['P_cells_with_3plus_distinct_q8'] = int((g >= 3).sum())
    o['P_integer_cells'] = int((np.abs(mode - np.round(mode)) < 1e-9).sum())
    og = df[(~df['off']) & (df['uv'] > 0) & np.isfinite(df['uv'])].copy()
    og['uvk'] = og['uv'].map(lambda x: float(f'{x:.6g}'))
    og = og.join(mode.rename('P'), on=['p', 'u', 'z', 'r'])
    o['ongrid_valued_in_flagged_cells'] = int(og['P'].notna().sum())
    o['ongrid_at_flagged_P'] = int((og['uvk'] == og['P']).sum())
    # P is a per-cell constant: compare its dispersion against the on-grid unit values of the cell
    ratio = (og['uv'] / og['P']).dropna()
    o['ongrid_uv_over_P_p5_p50_p95'] = [round(float(x), 3) for x in ratio.quantile([0.05, 0.5, 0.95])]
    s21 = raw('Niger', '2021-22', 's07b_me_ner2021.dta')
    a = num(s21['s07bq07a'])
    o['2021_q7_positive'] = int((a > 0).sum())
    o['2021_off_grid'] = int(((a > 0) & (np.abs(4 * a - np.round(4 * a)) >= 1e-6)).sum())
    key = 'Niger::food_acquired::computed-last-purchase-quantity'
    ri = ll.Country('Niger').derivation_inputs(key, wave='2018-19')
    save(ri, 'niger_flag_inputs')
    o['inputs_rows'] = len(ri)
    o['inputs_has_s07bq06'] = 's07bq06' in ri.columns
    o['inputs_index_equals_flagged'] = keyset(ri) == keyset(rec[rec['flag']].set_index(['t', 'i', 'j']))


# ---------------------------------------------------------------- Uganda
def uganda(o):
    g = raw('Uganda', '2009-10', 'GSEC1.dta')
    cols = [c for c in ('HHID', 'comm', 'hh_status') if c in g.columns]
    small = g[cols].copy()
    small['HHID'] = small['HHID'].map(lt.format_id)
    save(small, 'uganda_gsec1_2009')
    o['gsec1_has_hh_status'] = 'hh_status' in g.columns
    if 'hh_status' in g.columns:
        o['hh_status_labels'] = code_labels('Uganda', '2009-10', 'GSEC1.dta', 'hh_status')
    key = 'Uganda::sample::ea-same-household'
    C = ll.Country('Uganda')
    ri = C.derivation_inputs(key, wave='2009-10')
    save(ri, 'uganda_ea_inputs')
    o['inputs_rows'] = len(ri)
    o['inputs_households'] = int(ri.index.get_level_values('i').nunique())
    o['inputs_by_source'] = {str(k): int(v) for k, v in ri['_source'].value_counts().items()}
    der = C.derivations()[key].resolve()(ri)
    save(der.to_frame(), 'uganda_ea_derived')
    o['derived_rows'] = len(der)
    o['derived_notna'] = int(der.notna().sum())
    for w in ('2005-06', '2010-11', '2011-12', '2013-14'):
        x = raw('Uganda', w, 'GSEC1.dta')
        o[f'{w}_gsec1_cols'] = [c for c in ('HHID', 'HHID_old', 'comm', 'ea') if c in x.columns]
    o['updated_ids_waves'] = sorted(C.updated_ids.keys())


for name, f in (('mali', mali), ('senegal_salt', senegal_salt), ('senegal_near_zero', senegal_near_zero),
                ('niger', niger), ('uganda', uganda)):
    run(name, f)
print('done checks', flush=True)
