"""Third read-only pass (AGGREGATES ONLY).  usage: python explore3.py <clone> <out.json>

S  Senegal 2021-22 salt: the exact per-capita constant behind the 100 rows
N  Niger 2018-19: reference unit value P against community prices (region labels
   joined correctly) and the unit-mismatch rates of the two waves
"""
import json
import sys
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, out = sys.argv[1], Path(sys.argv[2])
head = assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

R = {'head': head}


def raw(C, rel, cats=False):
    return get_dataframe(str(countries_root() / C / rel), convert_categoricals=cats)


def num(s):
    return pd.to_numeric(s, errors='coerce').astype(float)


def on_grid(x):
    x = np.asarray(x, dtype=float)
    return np.abs(4 * x - np.round(4 * x)) < 1e-6


def vc(s, n=15):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).head(n).items()}


def q(s, qs=(0, 0.05, 0.25, 0.5, 0.75, 0.95, 1)):
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


def senegal():
    o = {}
    s = raw('Senegal', '2021-22/Data/s07b_me_sen2021.dta')
    W = raw('Senegal', '2021-22/Data/ehcvm_welfare_sen2021.dta')
    hs = pd.Series(num(W['hhsize']).to_numpy(), index=hhkey(W).to_numpy())
    hsz = hhkey(s).map(hs)
    q3 = num(s['s07bq03a'])
    salt = (num(s['s07bq01']) == 139) & (num(s['s07bq03b']) == 100) & (q3 > 0)
    r = q3 / hsz
    m4 = salt & (np.abs(sigkey(r.to_numpy(), 4) - 0.02) < 1e-12)
    vals = r[m4]
    o['ratio_distinct_repr'] = vc(vals.map(lambda x: repr(float(x))), 10)
    o['ratio_min_max'] = [repr(float(vals.min())), repr(float(vals.max()))]
    c = float(vals.median())
    o['c'] = repr(c)
    for tol in (1e-12, 1e-10, 1e-9, 1e-8):
        o[f'salt_kg_rows_within_{tol}_of_c_hhsize'] = int((salt & ((q3 - c * hsz).abs() < tol)).sum())
        o[f'all_rows_within_{tol}_of_c_hhsize'] = int(((q3 > 0) & ((q3 - c * hsz).abs() < tol)).sum())
    o['c_over_0_02'] = repr(c / 0.02)
    o['q3_float32_of_0_02_hhsize'] = int((m4 & (q3 == (0.02 * hsz).astype('float32').astype('float64'))).sum())
    # q3 as stored: hex of a few values to see the precision
    o['q3_hex_examples'] = [float(x).hex() for x in q3[m4].head(5)]
    o['other_items_within_1e-9_c_hhsize'] = vc(num(s.loc[(q3 > 0) & ((q3 - c * hsz).abs() < 1e-9) & ~salt, 's07bq01']))
    o['c_times_hhsize_on_salt_other_units'] = int(((num(s['s07bq01']) == 139) & (num(s['s07bq03b']) != 100)
                                                   & ((q3 - c * hsz).abs() < 1e-9) & (q3 > 0)).sum())
    return o


def niger():
    o = {}
    s = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta')
    Wl = raw('Niger', '2018-19/Data/ehcvm_welfare_ner2018.dta', True)
    wk = hhkey(Wl)
    hh = hhkey(s)
    reg = hh.map(pd.Series(Wl['region'].astype(object).astype(str).str.strip().str.lower().to_numpy(), index=wk.to_numpy()))
    o['welfare_region_labels'] = sorted(set(reg.dropna()))
    q7, q8 = num(s['s07bq07a']), num(s['s07bq08'])
    item, u7, z7 = num(s['s07bq01']), num(s['s07bq07b']), num(s['s07bq07c'])
    u3, z3 = num(s['s07bq03b']), num(s['s07bq03c'])
    pos = q7 > 0
    off = pos & ~on_grid(q7)
    uv = q8 / q7.where(pos)
    f = pd.DataFrame({'item': item, 'u7': u7, 'z7': z7, 'reg': reg, 'uv': uv, 'off': off, 'on': pos & ~off})
    f['uvk'] = sigkey(f['uv'].to_numpy(), 6)
    keys = ['item', 'u7', 'z7', 'reg']
    P = f[f['off'] & f['uv'].notna()].groupby(keys)['uvk'].agg(lambda x: x.value_counts().index[0]).rename('P')
    cp = raw('Niger', '2018-19/Data/s05_co_ner2018.dta')
    cpl = raw('Niger', '2018-19/Data/s05_co_ner2018.dta', True)
    s0 = raw('Niger', '2018-19/Data/s00_me_ner2018.dta', True)
    greg = pd.Series(s0['s00q01'].astype(object).astype(str).str.strip().str.lower().to_numpy(), index=num(s0['grappe']).to_numpy())
    greg = greg[~greg.index.duplicated()]
    c = pd.DataFrame({'grappe': num(cp['grappe']), 'prodcode': num(cp['s05q01']),
                      'prod': cpl['s05q01'].astype(object).astype(str).str.strip().str.lower(),
                      'us': num(cp['s05q02']), 'p1': num(cp['s05q03']), 'p2': num(cp['s05q04'])})
    c['reg'] = c['grappe'].map(greg)
    c['u'] = np.floor(c['us'] / 10)
    c['z'] = c['us'] % 10
    lab = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta', True)['s07bq01'].astype(object).astype(str).str.strip().str.lower()
    code2lab = dict(zip(item, lab))
    o['s05_products'] = int(c['prod'].nunique())
    pr = P.reset_index()
    pr['prod'] = pr['item'].map(code2lab)
    o['P_cells'] = int(len(pr))
    o['P_cells_product_in_s05'] = int(pr['prod'].isin(set(c['prod'])).sum())
    o['P_cells_code_in_s05'] = int(pr['item'].isin(set(c['prodcode'])).sum())
    long = pd.concat([c.assign(p=c['p1']), c.assign(p=c['p2'])])
    long = long[long['p'] > 0]
    res = {}
    for name, keyset, prodcol in (('label', ['prod', 'u', 'z', 'reg'], 'prod'), ('code', ['prodcode', 'u', 'z', 'reg'], 'prodcode')):
        for how in ('mean', 'median'):
            g = long.groupby(keyset)[('p')].agg(how).rename('cp')
            left = pr.rename(columns={'u7': 'u', 'z7': 'z'})
            if prodcol == 'prodcode':
                left = left.assign(prodcode=left['item'])
            jj = left.join(g, on=keyset)
            ok = jj['cp'].notna()
            res[f'{name}_{how}'] = {'with_cp': int(ok.sum()),
                                    'eq_1e-6': int((ok & ((jj['P'] / jj['cp'] - 1).abs() < 1e-6)).sum()),
                                    'within_1pct': int((ok & ((jj['P'] / jj['cp'] - 1).abs() < 0.01)).sum()),
                                    'log_ratio_q': q(np.log(jj.loc[ok, 'P'] / jj.loc[ok, 'cp']))}
    o['P_vs_community'] = res
    # unit mismatch, both waves, enabled rows with a last purchase
    q6 = num(s['s07bq06'])
    en = pos & q6.isin([1, 2, 3])
    o['2018_code_mismatch_enabled'] = round(100 * float(((u7 != u3) & en).sum()) / float(en.sum()), 2)
    o['2018_unit_or_size_mismatch_enabled'] = round(100 * float((((u7 != u3) | (z7 != z3)) & en).sum()) / float(en.sum()), 2)
    s2 = raw('Niger', '2021-22/Data/s07b_me_ner2021.dta')
    p2 = (num(s2['s07bq07a']) > 0) & num(s2['s07bq06']).isin([1, 2, 3])
    m2c = num(s2['s07bq07b']) != num(s2['s07bq03b'])
    m2s = num(s2['s07bq07c']) != num(s2['s07bq03c'])
    o['2021_code_mismatch_enabled'] = round(100 * float((m2c & p2).sum()) / float(p2.sum()), 2)
    o['2021_unit_or_size_mismatch_enabled'] = round(100 * float(((m2c | m2s) & p2).sum()) / float(p2.sum()), 2)
    # rational structure: denominators of the off-grid q7 (q7 = a / b, small integers)
    from fractions import Fraction
    dens = []
    for x in q7[off].head(4000):
        fr = Fraction(float(x)).limit_denominator(2000)
        if abs(float(fr) - float(x)) < 1e-9:
            dens.append(fr.denominator)
        else:
            dens.append(-1)
    o['off_q7_rational_denominator_top'] = vc(pd.Series(dens), 20)
    o['off_q7_not_rational_le_2000'] = int((pd.Series(dens) == -1).sum())
    return o


for k, fn in (('S', senegal), ('N', niger)):
    try:
        R[k] = fn()
    except Exception as e:  # noqa: BLE001
        R[k] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-2500:]}
    out.write_text(json.dumps(R, indent=1, default=str))
    print('SECTION', k, flush=True)
print('EXPLORE3 DONE', flush=True)
