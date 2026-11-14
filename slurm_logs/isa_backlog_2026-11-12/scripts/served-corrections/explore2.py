"""Follow-up exploration (read-only, AGGREGATES ONLY).

usage: python explore2.py <clone> <out.json> [S N X]

S  Senegal 2021-22 salt: why the exact 0.02 x hhsize test finds 0 where the live
   check's 4-significant-digit key found 100
N  Niger 2018-19 s07bq07a: discriminating tests for how the values were built
X  GH #1519: s07bq03a and s07bq07a both in (0, 1e-3), every EHCVM wave
"""
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, out = sys.argv[1], Path(sys.argv[2])
which = sys.argv[3:] or ['S', 'N', 'X']
head = assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

R = {'head': head}


def dump():
    out.write_text(json.dumps(R, indent=1, default=str))


def raw(C, rel, cats=False):
    return get_dataframe(str(countries_root() / C / rel), convert_categoricals=cats)


def num(s):
    return pd.to_numeric(s, errors='coerce').astype(float)


def on_grid(x):
    x = np.asarray(x, dtype=float)
    return np.abs(4 * x - np.round(4 * x)) < 1e-6


def vc(s, n=15):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).head(n).items()}


def q(s, qs=(0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1)):
    s = pd.Series(s).dropna()
    return [round(float(x), 6) for x in s.quantile(list(qs))] if len(s) else None


def hhkey(df):
    return (num(df['grappe']).astype('Int64').astype(str) + '-'
            + num(df['menage']).astype('Int64').astype(str))


def sigkey(x, k):
    x = np.asarray(x, dtype='float64')
    o = np.full(len(x), np.nan)
    ok = np.isfinite(x) & (x > 0)
    e = np.floor(np.log10(x[ok]))
    o[ok] = np.round(x[ok] / 10 ** e * 10 ** (k - 1)) * 10 ** (e - (k - 1))
    return o


def section(name, fn):
    t0 = time.time()
    try:
        R[name] = fn()
    except Exception as e:  # noqa: BLE001
        R[name] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
    R.setdefault('seconds', {})[name] = round(time.time() - t0, 1)
    print('SECTION', name, R['seconds'][name], flush=True)
    dump()


def senegal():
    o = {}
    s = raw('Senegal', '2021-22/Data/s07b_me_sen2021.dta')
    W = raw('Senegal', '2021-22/Data/ehcvm_welfare_sen2021.dta')
    o['welfare_columns'] = [c for c in W.columns][:60]
    o['s07b_columns'] = list(s.columns)
    wk = hhkey(W)
    o['welfare_rows'] = int(len(W))
    o['welfare_key_dup'] = int(wk.duplicated().sum())
    hh = hhkey(s)
    o['s07b_households'] = int(hh.nunique())
    o['s07b_households_in_welfare'] = int(hh.drop_duplicates().isin(set(wk)).sum())
    hs = pd.Series(num(W['hhsize']).to_numpy(), index=wk.to_numpy())
    hs = hs[~hs.index.duplicated()]
    hsz = hh.map(hs)
    item, unit = num(s['s07bq01']), num(s['s07bq03b'])
    q3 = num(s['s07bq03a'])
    salt = (item == 139) & (unit == 100) & (q3 > 0)
    r = q3 / hsz
    sk = sigkey(r.to_numpy(), 4)
    m4 = salt & (np.abs(sk - 0.02) < 1e-12)
    o['salt_kg_rows'] = int(salt.sum())
    o['sig4_match'] = int(m4.sum())
    o['sig4_match_off_grid'] = int((m4 & ~on_grid(q3)).sum())
    d = (q3 - 0.02 * hsz).abs()
    o['sig4_match_absdiff_quantiles'] = q(d[m4])
    o['sig4_match_reldiff_quantiles'] = q((r[m4] / 0.02 - 1).abs())
    o['sig4_match_q3_top'] = vc(q3[m4].round(6), 20)
    o['sig4_match_hhsize_top'] = vc(hsz[m4], 20)
    o['sig4_match_q3_float32_exact'] = int((m4 & (q3.astype('float32').astype('float64') == q3)).sum())
    o['salt_ratio_top_sig4'] = vc(pd.Series(sk[salt.to_numpy()]), 15)
    # other household-size measures: roster count, eqadu1/2
    r1 = raw('Senegal', '2021-22/Data/s01_me_sen2021.dta')
    rn = hh.map(hhkey(r1).value_counts())
    o['salt_roster_eq_hhsize'] = int((salt & (rn == hsz)).sum())
    for col in ('eqadu1', 'eqadu2'):
        if col in W:
            e = hh.map(pd.Series(num(W[col]).to_numpy(), index=wk.to_numpy())[lambda x: ~x.index.duplicated()])
            o[f'salt_sig4_0_02_{col}'] = int((salt & (np.abs(sigkey((q3 / e).to_numpy(), 4) - 0.02) < 1e-12)).sum())
    o['salt_sig4_0_02_roster'] = int((salt & (np.abs(sigkey((q3 / rn).to_numpy(), 4) - 0.02) < 1e-12)).sum())
    o['salt_exact_0_02_roster'] = int((salt & ((q3 - 0.02 * rn).abs() < 1e-9)).sum())
    # rows the live check counted: off grid, sig4 ratio 0.02, and repeated 3+ times with 3+ distinct q3
    return o


def niger():
    o = {}
    s = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta')
    W = raw('Niger', '2018-19/Data/ehcvm_welfare_ner2018.dta')
    hh = hhkey(s)
    wk = hhkey(W)
    reg = hh.map(pd.Series(W['region'].astype(object).astype(str).to_numpy(), index=wk.to_numpy()))
    mil = hh.map(pd.Series(W['milieu'].astype(object).astype(str).to_numpy(), index=wk.to_numpy()))
    q3, q4, q5 = num(s['s07bq03a']), num(s['s07bq04']).fillna(0), num(s['s07bq05']).fillna(0)
    q7, q8, q6 = num(s['s07bq07a']), num(s['s07bq08']), num(s['s07bq06'])
    item, u7, z7 = num(s['s07bq01']), num(s['s07bq07b']), num(s['s07bq07c'])
    u3, z3 = num(s['s07bq03b']), num(s['s07bq03c'])
    pos = q7 > 0
    off = pos & ~on_grid(q7)
    on = pos & on_grid(q7)
    q8int = (q8 - np.round(q8)).abs() < 1e-9
    uv = q8 / q7.where(pos)
    f = pd.DataFrame({'item': item, 'u7': u7, 'z7': z7, 'reg': reg, 'mil': mil, 'uv': uv,
                      'off': off, 'on': on, 'q8int': q8int, 'q7': q7, 'q8': q8, 'q3': q3,
                      'grappe': num(s['grappe']), 'vague': num(s['vague'])})
    f['uvk'] = sigkey(f['uv'].to_numpy(), 6)
    cellk = ['item', 'u7', 'z7', 'reg']
    offv = f[f['off'] & f['uv'].notna()]
    mode = offv.groupby(cellk)['uvk'].agg(lambda x: x.value_counts().index[0]).rename('P')
    f = f.join(mode, on=cellk)
    at_mode = f['off'] & (f['uvk'] == f['P'])
    o['off_at_region_mode'] = int(at_mode.sum())
    o['off_rows'] = int(f['off'].sum())
    # P itself: round or fractional
    P = mode.dropna()
    o['P_cells'] = int(len(P))
    o['P_integer'] = int(((P - np.round(P)).abs() < 1e-9).sum())
    o['P_multiple_of_5'] = int(((P / 5 - np.round(P / 5)).abs() < 1e-9).sum())
    o['P_multiple_of_0_025'] = int(((P / 0.025 - np.round(P / 0.025)).abs() < 1e-6).sum())
    o['P_rows_weighted_integer'] = int((at_mode & ((f['P'] - np.round(f['P'])).abs() < 1e-9)).sum())
    # q7 = q8 / P: crosstab q8 integer x decimals for at-mode rows
    o['at_mode_q8_integer'] = int((at_mode & f['q8int']).sum())
    o['at_mode_q8_nonint'] = int((at_mode & ~f['q8int'] & f['q8'].notna()).sum())
    # on-grid rows in the same cells, relative to P: a truncation would show as a sharp band
    rat = (f['uv'] / f['P'])[f['on'] & f['P'].notna()]
    o['on_uv_over_P_quantiles'] = q(rat)
    o['on_uv_over_P_log_abs_gt'] = {str(k): int((np.log(rat).abs() > np.log(k)).sum()) for k in (1.5, 2, 3, 5, 10)}
    o['on_rows_in_P_cells'] = int(len(rat))
    # the off-grid rows' OWN q8 relative to on-grid q8 in the cell: are they outliers by value?
    medq8 = f[f['on']].groupby(cellk)['q8'].median().rename('medq8')
    f = f.join(medq8, on=cellk)
    o['off_q8_over_on_median_q8'] = q((f['q8'] / f['medq8'])[f['off']])
    o['on_q8_over_on_median_q8'] = q((f['q8'] / f['medq8'])[f['on']])
    # q7 relative to on-grid q7 in the cell (the replaced quantity vs typical)
    medq7 = f[f['on']].groupby(cellk)['q7'].median().rename('medq7')
    f = f.join(medq7, on=cellk)
    o['off_q7_over_on_median_q7'] = q((f['q7'] / f['medq7'])[f['off']])
    # P versus statistics of the on-grid unit values in the same cell
    st = f[f['on']].groupby(cellk)['uv'].agg(['median', 'mean', 'min', 'max', 'count'])
    pj = pd.DataFrame({'P': P}).join(st, how='left')
    o['P_vs_on_median_log_quantiles'] = q(np.log(pj['P'] / pj['median']))
    o['P_within_on_range'] = int(((pj['P'] >= pj['min']) & (pj['P'] <= pj['max'])).sum())
    o['P_cells_with_on'] = int(pj['median'].notna().sum())
    # P versus the median of on-grid unit values one level up: (item, unit, size) national
    st2 = f[f['on']].groupby(['item', 'u7', 'z7'])['uv'].median().rename('nat_med')
    pj2 = pd.DataFrame({'P': P}).reset_index().join(st2, on=['item', 'u7', 'z7'])
    o['P_eq_national_on_median'] = int(((pj2['P'] / pj2['nat_med'] - 1).abs() < 1e-6).sum())
    # community prices: mean / median per (product, unit-size, region), both readings
    try:
        cp = raw('Niger', '2018-19/Data/s05_co_ner2018.dta')
        cpl = raw('Niger', '2018-19/Data/s05_co_ner2018.dta', True)
        s0 = raw('Niger', '2018-19/Data/s00_me_ner2018.dta', True)
        greg = pd.Series(s0['s00q01'].astype(object).astype(str).to_numpy(), index=num(s0['grappe']).to_numpy())
        greg = greg[~greg.index.duplicated()]
        c = pd.DataFrame({'grappe': num(cp['grappe']), 'prod_code': num(cp['s05q01']),
                          'prod': cpl['s05q01'].astype(object).astype(str).str.strip().str.lower(),
                          'us': num(cp['s05q02']), 'p1': num(cp['s05q03']), 'p2': num(cp['s05q04'])})
        c['reg'] = c['grappe'].map(greg)
        c['unit'] = np.floor(c['us'] / 10)
        c['size'] = c['us'] % 10
        o['s05_reg_values'] = vc(c['reg'], 10)
        o['welfare_reg_values'] = vc(reg, 10)
        lab = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta', True)['s07bq01'].astype(object).astype(str).str.strip().str.lower()
        code2lab = dict(zip(item, lab))
        long = pd.concat([c.assign(p=c['p1']), c.assign(p=c['p2'])])
        long = long[long['p'] > 0]
        stats = {}
        for how in ('mean', 'median'):
            g = long.groupby(['prod', 'unit', 'size', 'reg'])['p'].agg(how)
            stats[how] = g
        gm = c.assign(p=(c['p1'] + c['p2']) / 2).groupby(['prod', 'unit', 'size', 'reg'])['p'].mean()
        stats['mean_of_pair_means'] = gm
        pr = pd.DataFrame({'P': P}).reset_index()
        pr['prod'] = pr['item'].map(code2lab)
        o['P_cells_with_label'] = int(pr['prod'].notna().sum())
        res = {}
        for how, g in stats.items():
            jj = pr.join(g.rename('cp'), on=['prod', 'u7', 'z7', 'reg'])
            res[how] = {'with_cp': int(jj['cp'].notna().sum()),
                        'eq_1e-6': int(((jj['P'] / jj['cp'] - 1).abs() < 1e-6).sum()),
                        'within_1pct': int(((jj['P'] / jj['cp'] - 1).abs() < 0.01).sum()),
                        'log_ratio_quantiles': q(np.log(jj['P'] / jj['cp']))}
        o['P_vs_community'] = res
        # regional label alignment between welfare and s00
        o['reg_label_join_example'] = sorted(set(reg.dropna()))[:10]
        o['s05_reg_labels'] = sorted(set(c['reg'].dropna()))[:10]
    except Exception as e:  # noqa: BLE001
        o['s05_error'] = f'{type(e).__name__}: {e}'
        o['s05_tb'] = traceback.format_exc()[-1500:]
    # q8 non-integer off rows: is q8 = q7 x P and q7 = something?
    ni = f['off'] & ~f['q8int'] & f['q8'].notna()
    o['nonint_off_rows'] = int(ni.sum())
    o['nonint_off_at_mode'] = int((ni & at_mode).sum())
    o['nonint_q8_decimals_top'] = vc(f.loc[ni, 'q8'].map(lambda v: len(repr(float(v)).split('.')[1]) if '.' in repr(float(v)) else 0), 10)
    o['nonint_q7_over_q3_top'] = vc(sigkey((f['q7'] / f['q3'])[ni].to_numpy(), 4), 10)
    o['int_off_q7_over_q3_top'] = vc(sigkey((f['q7'] / f['q3'])[f['off'] & f['q8int']].to_numpy(), 4), 10)
    # the uv=200 rows: what do q7 and q8 look like
    t = f['off'] & (np.abs(f['uvk'] - 200) < 1e-9)
    o['uv200_rows'] = int(t.sum())
    o['uv200_q8_integer'] = int((t & f['q8int']).sum())
    o['uv200_q8_top'] = vc(f.loc[t, 'q8'].round(4), 10)
    o['uv200_q7_top'] = vc(f.loc[t, 'q7'].round(6), 10)
    # disabled-but-filled q7 (s07bq06 in 4,5): is it q3?
    dis = pos & ~q6.isin([1, 2, 3])
    o['disabled_filled'] = int(dis.sum())
    o['disabled_q7_eq_q3'] = int((dis & ((q7 - q3).abs() < 1e-9)).sum())
    o['disabled_unit7_eq_unit3'] = int((dis & (u7 == u3) & (z7 == z3)).sum())
    o['disabled_q8_notna'] = int((dis & q8.notna()).sum())
    o['enabled_on_q7_eq_q3'] = int((on & q6.isin([1, 2, 3]) & ((q7 - q3).abs() < 1e-9)).sum())
    o['enabled_on'] = int((on & q6.isin([1, 2, 3])).sum())
    return o


def tiny():
    o = {}
    from lsms_library.ehcvm import NSU_COUNTRIES  # noqa: F401
    cells = {'Senegal': [('2018-19', 's07b_me_sen2018.dta'), ('2021-22', 's07b_me_sen2021.dta')],
             'Mali': [('2018-19', 's07b_me_mli2018.dta'), ('2021-22', 's07b_me_mli2021.dta')],
             'Niger': [('2018-19', 's07b_me_ner2018.dta'), ('2021-22', 's07b_me_ner2021.dta')],
             'Burkina_Faso': [('2018-19', 's07b_me_bfa2018.dta'), ('2021-22', 's07b_me_bfa2021.dta')],
             'Benin': [('2018-19', 's07b_me_ben2018.dta'), ('2021-22', 's07b_me_ben2021.dta')],
             'Togo': [('2018', '../Data1/s07b_me_tgo2018.dta')],
             'Guinea-Bissau': [('2018-19', 's07b_me_gnb2018.dta')],
             'CotedIvoire': [('2018-19', 'Menage/s07b_me_CIV2018.dta')]}
    for C, ws in cells.items():
        for w, f in ws:
            key = f'{C} {w}'
            try:
                rel = f'{w}/Data/{f}' if not f.startswith('..') else f'{w}/{f[3:]}'
                s = raw(C, rel)
                q3, q7 = num(s['s07bq03a']), num(s['s07bq07a'])
                both = (q3 > 0) & (q3 < 1e-3) & (q7 > 0) & (q7 < 1e-3)
                r = {'rows': int(len(s)), 'rule_rows': int(both.sum()),
                     'q3_tiny': int(((q3 > 0) & (q3 < 1e-3)).sum()),
                     'q7_tiny': int(((q7 > 0) & (q7 < 1e-3)).sum())}
                if both.any():
                    r['products'] = vc(num(s.loc[both, 's07bq01']))
                    r['q3_over_q7'] = vc((q3 / q7)[both].round(9))
                    r['q3_values'] = [float(x) for x in q3[both]]
                    r['q7_values'] = [float(x) for x in q7[both]]
                    r['q4'] = vc(num(s.loc[both, 's07bq04'])); r['q5'] = vc(num(s.loc[both, 's07bq05']))
                    r['q8'] = vc(num(s.loc[both, 's07bq08']))
                    r['u3'] = vc(num(s.loc[both, 's07bq03b'])); r['u7'] = vc(num(s.loc[both, 's07bq07b']))
                    r['s07bq06'] = vc(num(s.loc[both, 's07bq06']))
                    r['households'] = int(hhkey(s)[both].nunique())
                o[key] = r
            except Exception as e:  # noqa: BLE001
                o[key] = {'error': f'{type(e).__name__}: {e}'}
    return o


for k, fn in (('S', senegal), ('N', niger), ('X', tiny)):
    if k in which:
        section(k, fn)
print('EXPLORE2 DONE', flush=True)
