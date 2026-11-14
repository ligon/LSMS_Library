"""Read-only exploration for the served-corrections task.  AGGREGATES ONLY.

usage: python explore.py <clone> <out.json> [M S N U]

M  Mali 2018-19 rice block (no vague, q3 = 1.4 x hhsize)
S  Senegal 2021-22 salt rows (q3 = 0.02 x hhsize)
N  Niger 2018-19 s07bq07a: how the off-grid values were constructed
U  Uganda 2009-10 ea: candidate labels from the same household in other waves

Every source read goes through lsms_library.local_tools.get_dataframe into the
private root; nothing is written but the JSON of counts.
"""
import json
import sys
import time
import traceback
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, out = sys.argv[1], Path(sys.argv[2])
which = sys.argv[3:] or ['M', 'S', 'N', 'U']
head = assert_env(clone)
warnings.simplefilter('ignore')

from lsms_library.local_tools import get_dataframe, format_id  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

R = {'head': head, 'sections': which}


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


def share(a, b):
    return round(100.0 * a / b, 3) if b else None


def hhkey(df):
    return (num(df['grappe']).astype('Int64').astype(str) + '-'
            + num(df['menage']).astype('Int64').astype(str))


def section(name, fn):
    t0 = time.time()
    try:
        R[name] = fn()
    except Exception as e:  # noqa: BLE001
        R[name] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-3000:]}
    R.setdefault('seconds', {})[name] = round(time.time() - t0, 1)
    print('SECTION', name, 'done', R['seconds'][name], flush=True)
    dump()


# --------------------------------------------------------------------------- Mali
def mali():
    o = {}
    s = raw('Mali', '2018-19/Data/s07b_me_mli2018.dta')
    sl = raw('Mali', '2018-19/Data/s07b_me_mli2018.dta', True)
    W = raw('Mali', '2018-19/Data/ehcvm_welfare_mli2018.dta')
    o['rows'] = int(len(s))
    o['columns'] = [c for c in s.columns]
    hh = hhkey(s)
    q3, q4, q5 = num(s['s07bq03a']), num(s['s07bq04']).fillna(0), num(s['s07bq05']).fillna(0)
    nov = num(s['vague']).isna()
    o['no_vague_rows'] = int(nov.sum())
    o['no_vague_item_code'] = vc(num(s.loc[nov, 's07bq01']))
    o['no_vague_item_label'] = vc(sl.loc[nov, 's07bq01'].astype(object).astype(str).str.strip())
    o['no_vague_unit_code'] = vc(num(s.loc[nov, 's07bq03b']))
    o['no_vague_unit_label'] = vc(sl.loc[nov, 's07bq03b'].astype(object).astype(str).str.strip())
    o['no_vague_size_code'] = vc(num(s.loc[nov, 's07bq03c']))
    o['no_vague_s07bq06'] = vc(sl.loc[nov, 's07bq06'].astype(object).astype(str))
    o['no_vague_q3_pos'] = int((nov & (q3 > 0)).sum())
    o['no_vague_q4_pos'] = int((nov & (q4 > 0)).sum())
    o['no_vague_q5_pos'] = int((nov & (q5 > 0)).sum())
    o['no_vague_q7a_notna'] = int((nov & num(s['s07bq07a']).notna()).sum())
    o['no_vague_q8_notna'] = int((nov & num(s['s07bq08']).notna()).sum())
    o['no_vague_households'] = int(hh[nov].nunique())
    o['dtype_q3'] = str(s['s07bq03a'].dtype)
    # household size, welfare and menage files
    wk = hhkey(W)
    o['welfare_unique'] = bool(not wk.duplicated().any())
    hs = pd.Series(num(W['hhsize']).to_numpy(), index=wk.to_numpy())
    try:
        Mn = raw('Mali', '2018-19/Data/ehcvm_menage_mli2018.dta')
        o['menage_has_hhsize'] = 'hhsize' in Mn.columns
        o['menage_columns_head'] = list(Mn.columns)[:40]
    except Exception as e:  # noqa: BLE001
        o['menage_error'] = repr(e)
    r1 = raw('Mali', '2018-19/Data/s01_me_mli2018.dta')
    rk = hhkey(r1)
    roster_n = rk.value_counts()
    o['roster_columns_head'] = list(r1.columns)[:30]
    hsz = hh.map(hs)
    rn = hh.map(roster_n)
    tgt = 1.4 * hsz
    exact = (q3 - tgt).abs() < 1e-9
    o['block_in_welfare'] = int((nov & hsz.notna()).sum())
    o['block_exact_1_4_hhsize_tol1e-9'] = int((nov & exact).sum())
    o['block_exact_1_4_hhsize_equal'] = int((nov & (q3 == tgt)).sum())
    o['block_exact_1_4_hhsize_tol1e-6'] = int((nov & ((q3 - tgt).abs() < 1e-6)).sum())
    resid = nov & ~exact
    o['block_not_exact_rows'] = int(resid.sum())
    o['block_not_exact_no_welfare'] = int((resid & hsz.isna()).sum())
    o['block_not_exact_q3_over_1_4_is_integer'] = int((resid & ((q3 / 1.4 - np.round(q3 / 1.4)).abs() < 1e-6)).sum())
    o['block_not_exact_q3_over_1_4_eq_roster_count'] = int((resid & ((q3 / 1.4 - rn).abs() < 1e-6)).sum())
    o['block_not_exact_q3_over_1_4_minus_hhsize'] = vc((np.round(q3[resid] / 1.4) - hsz[resid]).astype('Float64'))
    o['block_roster_count_eq_hhsize'] = int((nov & (rn == hsz)).sum())
    o['block_1_4_roster_count'] = int((nov & ((q3 - 1.4 * rn).abs() < 1e-9)).sum())
    # how specific is the selector: the same ratio on rows WITH a vague
    item = num(s['s07bq01'])
    unit = num(s['s07bq03b'])
    blk_item = set(num(s.loc[nov, 's07bq01']).dropna().unique())
    blk_unit = set(num(s.loc[nov, 's07bq03b']).dropna().unique())
    rice = item.isin(blk_item) & unit.isin(blk_unit)
    o['with_vague_rice_kg_rows_pos'] = int((~nov & rice & (q3 > 0)).sum())
    o['with_vague_rice_kg_exact_1_4_hhsize'] = int((~nov & rice & exact).sum())
    o['with_vague_any_item_exact_1_4_hhsize'] = int((~nov & exact & (q3 > 0)).sum())
    # households with both a block row and an ordinary rice-kg row
    both = set(hh[nov]) & set(hh[~nov & rice & ((q3 > 0) | (q4 > 0) | (q5 > 0))])
    o['households_with_both'] = len(both)
    bmask = hh.isin(both) & ~nov & rice
    o['both_ordinary_rows'] = int(bmask.sum())
    o['both_ordinary_s07bq06'] = vc(sl.loc[bmask, 's07bq06'].astype(object).astype(str))
    o['both_ordinary_purchased_pos'] = int((bmask & ((q3 - q4 - q5).clip(lower=0) > 0)).sum())
    o['both_ordinary_q4_pos'] = int((bmask & (q4 > 0)).sum())
    o['both_ordinary_q5_pos'] = int((bmask & (q5 > 0)).sum())
    o['both_ordinary_size_code'] = vc(num(s.loc[bmask, 's07bq03c']))
    o['both_block_size_code'] = vc(num(s.loc[hh.isin(both) & nov, 's07bq03c']))
    # kilograms
    pq = (q3.fillna(0) - q4 - q5).clip(lower=0)
    o['block_purchased_kg_sum'] = round(float(pq[nov].sum()), 3)
    o['block_exact_purchased_kg_sum'] = round(float(pq[nov & exact].sum()), 3)
    o['ordinary_rice_kg_purchased_kg_sum'] = round(float(pq[~nov & rice].sum()), 3)
    o['ordinary_rice_kg_purchased_rows'] = int((~nov & rice & (pq > 0)).sum())
    # source-file vague coverage of the block households in s00
    s00 = raw('Mali', '2018-19/Data/s00_me_mli2018.dta')
    s0v = pd.Series(num(s00['vague']).to_numpy(), index=hhkey(s00).to_numpy())
    o['block_households_vague_in_s00'] = vc(hh[nov].map(s0v))
    return o


# --------------------------------------------------------------------------- Senegal
def senegal():
    o = {}
    res = {}
    for wave, f, wf in (('2021-22', 's07b_me_sen2021.dta', 'ehcvm_welfare_sen2021.dta'),
                        ('2018-19', 's07b_me_sen2018.dta', 'ehcvm_welfare_sen2018.dta')):
        r = {}
        s = raw('Senegal', f'{wave}/Data/{f}')
        sl = raw('Senegal', f'{wave}/Data/{f}', True)
        W = raw('Senegal', f'{wave}/Data/{wf}')
        hh = hhkey(s)
        wk = hhkey(W)
        hs = pd.Series(num(W['hhsize']).to_numpy(), index=wk.to_numpy())
        reg = pd.Series(W['region'].astype(object).to_numpy(), index=wk.to_numpy()) if 'region' in W else None
        mil = pd.Series(W['milieu'].astype(object).to_numpy(), index=wk.to_numpy()) if 'milieu' in W else None
        lab = sl['s07bq01'].astype(object).astype(str)
        item = num(s['s07bq01'])
        codes = sorted(set(item[lab.str.lower().str.contains(r'\bsel\b', regex=True)].dropna().unique()))
        r['salt_codes'] = codes
        r['salt_labels'] = vc(lab[item.isin(codes)])
        unit = num(s['s07bq03b'])
        size = num(s['s07bq03c'])
        r['salt_units'] = vc(sl.loc[item.isin(codes), 's07bq03b'].astype(object).astype(str))
        q3, q4, q5 = num(s['s07bq03a']), num(s['s07bq04']).fillna(0), num(s['s07bq05']).fillna(0)
        hsz = hh.map(hs)
        kgcode = unit[sl['s07bq03b'].astype(object).astype(str).str.lower().str.contains(r'\bkg\b', regex=True)].dropna().unique()
        r['kg_unit_codes'] = sorted(set(kgcode))
        salt_kg = item.isin(codes) & unit.isin([100]) & (q3 > 0)
        r['salt_kg_pos_rows'] = int(salt_kg.sum())
        r['salt_kg_size_codes'] = vc(size[salt_kg])
        match = salt_kg & ((q3 - 0.02 * hsz).abs() < 1e-9)
        r['match_rows'] = int(match.sum())
        r['match_rows_tol1e-6'] = int((salt_kg & ((q3 - 0.02 * hsz).abs() < 1e-6)).sum())
        r['match_on_grid'] = int((match & on_grid(q3)).sum())
        r['match_off_grid'] = int((match & ~on_grid(q3)).sum())
        r['match_hhsize'] = vc(hsz[match], 25)
        r['match_q3'] = vc(q3[match].round(4), 25)
        r['match_s07bq06'] = vc(sl.loc[match, 's07bq06'].astype(object).astype(str))
        r['match_q4_pos'] = int((match & (q4 > 0)).sum())
        r['match_q5_pos'] = int((match & (q5 > 0)).sum())
        r['match_q3_eq_q5'] = int((match & ((q3 - q5).abs() < 1e-9)).sum())
        r['match_q7a_notna'] = int((match & num(s['s07bq07a']).notna()).sum())
        r['match_q8_notna'] = int((match & num(s['s07bq08']).notna()).sum())
        r['match_vague'] = vc(num(s.loc[match, 'vague']))
        r['match_households'] = int(hh[match].nunique())
        r['match_no_welfare'] = int((salt_kg & hsz.isna()).sum())
        pq = (q3.fillna(0) - q4 - q5).clip(lower=0)
        r['match_purchased_pos'] = int((match & (pq > 0)).sum())
        r['match_purchased_kg'] = round(float(pq[match].sum()), 4)
        r['salt_kg_purchased_kg'] = round(float(pq[salt_kg].sum()), 4)
        # last purchase on matches: same unit and a value, i.e. Expenditure served
        r['match_regions'] = vc(hh[match].map(reg)) if reg is not None else None
        # permutation null: shuffle hhsize among the salt-kg rows
        rng = np.random.default_rng(20261112)
        hv = hsz[salt_kg].to_numpy()
        qv = q3[salt_kg].to_numpy()
        null = []
        for _ in range(500):
            p = rng.permutation(hv)
            null.append(int((np.abs(qv - 0.02 * p) < 1e-9).sum()))
        r['null_matches_mean'] = round(float(np.mean(null)), 2)
        r['null_matches_p99'] = float(np.quantile(null, 0.99))
        r['null_matches_max'] = int(np.max(null))
        # coincidence estimate on the matches themselves: q3 values that a real
        # answer could produce (multiples of 0.05 kg, say)
        r['match_q3_multiple_of_0_05'] = int((match & (np.abs(20 * q3 - np.round(20 * q3)) < 1e-6)).sum())
        r['match_q3_multiple_of_0_1'] = int((match & (np.abs(10 * q3 - np.round(10 * q3)) < 1e-6)).sum())
        # per-capita salt among non-matching rows: national / region x milieu medians
        pc = (q3 / hsz)[salt_kg & ~match]
        r['nonmatch_per_capita_median'] = round(float(pc.median()), 5) if len(pc) else None
        r['all_per_capita_median'] = round(float((q3 / hsz)[salt_kg].median()), 5)
        if reg is not None and mil is not None:
            cell = hh.map(reg).astype(str) + '|' + hh.map(mil).astype(str)
            med = (q3 / hsz)[salt_kg].groupby(cell[salt_kg]).median()
            r['region_milieu_cells'] = int(len(med))
            r['region_milieu_cells_median_0_02'] = int((np.abs(med - 0.02) < 1e-9).sum())
        # the same constant on other items?  matches per item (all units)
        other = (q3 > 0) & ~item.isin(codes) & ((q3 - 0.02 * hsz).abs() < 1e-9) & ~on_grid(q3)
        r['other_items_off_grid_0_02_hhsize'] = int(other.sum())
        # how many served rows a match source row yields: purchased / produced / inkind
        r['match_served_rows'] = {'purchased': int((match & (pq > 0)).sum()),
                                  'produced': int((match & (q4 > 0)).sum()),
                                  'inkind': int((match & (q5 > 0)).sum())}
        res[wave] = r
    o['waves'] = res
    return o


# --------------------------------------------------------------------------- Niger
def niger():
    from lsms_library import ehcvm
    o = {}
    s = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta')
    sl = raw('Niger', '2018-19/Data/s07b_me_ner2018.dta', True)
    W = raw('Niger', '2018-19/Data/ehcvm_welfare_ner2018.dta')
    o['rows'] = int(len(s))
    hh = hhkey(s)
    wk = hhkey(W)
    reg = hh.map(pd.Series(W['region'].astype(object).astype(str).to_numpy(), index=wk.to_numpy()))
    mil = hh.map(pd.Series(W['milieu'].astype(object).astype(str).to_numpy(), index=wk.to_numpy()))
    q3, q4, q5 = num(s['s07bq03a']), num(s['s07bq04']).fillna(0), num(s['s07bq05']).fillna(0)
    q7, q8 = num(s['s07bq07a']), num(s['s07bq08'])
    q6 = num(s['s07bq06'])
    o['s07bq06_codes_labels'] = {str(k): str(v) for k, v in
                                 pd.DataFrame({'c': q6, 'l': sl['s07bq06'].astype(object).astype(str)})
                                 .drop_duplicates().set_index('c')['l'].items()}
    item = num(s['s07bq01'])
    u3, z3, u7, z7 = num(s['s07bq03b']), num(s['s07bq03c']), num(s['s07bq07b']), num(s['s07bq07c'])
    pos7 = q7 > 0
    off = pos7 & ~on_grid(q7)
    on = pos7 & on_grid(q7)
    o['q7a_pos'] = int(pos7.sum())
    o['q7a_off_grid'] = int(off.sum())
    o['q7a_off_grid_share'] = share(int(off.sum()), int(pos7.sum()))

    def ndec(x):
        out = []
        for v in x:
            t = repr(float(v))
            if 'e' in t:
                out.append(99)
            else:
                out.append(len(t.split('.')[1].rstrip('0')) if '.' in t else 0)
        return np.array(out)
    d7 = pd.Series(0, index=s.index)
    d7[off] = ndec(q7[off].to_numpy())
    o['off_grid_decimals'] = vc(d7[off], 30)
    long_dec = off & (d7 >= 6)
    o['off_grid_6plus_decimals'] = int(long_dec.sum())
    # T2: by s07bq06 (when last bought), enabled = 1, 2, 3
    o['off_by_s07bq06'] = vc(q6[off])
    o['on_by_s07bq06'] = vc(q6[on])
    o['pos7_by_s07bq06'] = vc(q6[pos7])
    o['q3pos_by_s07bq06'] = vc(q6[q3 > 0])
    enabled = q6.isin([1, 2, 3])
    o['q7a_filled_where_disabled'] = int((pos7 & ~enabled).sum())
    o['q8_filled_where_disabled'] = int((q8.notna() & ~enabled).sum())
    o['off_and_disabled'] = int((off & ~enabled).sum())
    o['off_and_enabled'] = int((off & enabled).sum())
    # T3: s07bq08 integer-ness
    q8int = (q8 - np.round(q8)).abs() < 1e-9
    o['q8_present'] = {'off': int((off & q8.notna()).sum()), 'on': int((on & q8.notna()).sum())}
    o['q8_integer'] = {'off': int((off & q8int).sum()), 'on': int((on & q8int).sum())}
    o['q8_mult25'] = {'off': int((off & ((q8 / 25 - np.round(q8 / 25)).abs() < 1e-9)).sum()),
                      'on': int((on & ((q8 / 25 - np.round(q8 / 25)).abs() < 1e-9)).sum())}
    o['q8_nonint_rows_total'] = int((q8.notna() & ~q8int).sum())
    o['q8_nonint_and_q7_on_grid'] = int((on & q8.notna() & ~q8int).sum())
    # T4: unit match
    um = (u7 == u3) & (z7 == z3)
    o['unit_match'] = {'off': share(int((off & um).sum()), int(off.sum())),
                       'on': share(int((on & um).sum()), int(on.sum()))}
    o['unit_code_match'] = {'off': share(int((off & (u7 == u3)).sum()), int(off.sum())),
                            'on': share(int((on & (u7 == u3)).sum()), int(on.sum()))}
    # T7: relation to q3
    o['q7_eq_q3'] = {'off': int((off & ((q7 - q3).abs() < 1e-9)).sum()),
                     'on': int((on & ((q7 - q3).abs() < 1e-9)).sum())}
    o['q7_eq_q3_minus_q4_q5'] = {'off': int((off & ((q7 - (q3 - q4 - q5)).abs() < 1e-9)).sum())}
    # T5/T6: unit value constancy in cells
    uv = q8 / q7.where(q7 > 0)
    f = pd.DataFrame({'item': item, 'u7': u7, 'z7': z7, 'reg': reg, 'mil': mil,
                      'grappe': num(s['grappe']), 'vague': num(s['vague']), 'uv': uv,
                      'off': off, 'on': on, 'q8int': q8int, 'q8': q8, 'q7': q7})
    f['uvk'] = f['uv'].map(lambda x: float(f'{x:.6g}') if pd.notna(x) else np.nan)
    cells = {'item_unit_size': ['item', 'u7', 'z7'],
             'item_unit_size_region': ['item', 'u7', 'z7', 'reg'],
             'item_unit_size_region_milieu': ['item', 'u7', 'z7', 'reg', 'mil'],
             'item_unit_size_region_milieu_vague': ['item', 'u7', 'z7', 'reg', 'mil', 'vague'],
             'item_unit_size_grappe': ['item', 'u7', 'z7', 'grappe']}
    T = {}
    offv = f[f['off'] & f['uv'].notna()]
    onv = f[f['on'] & f['uv'].notna()]
    for name, keys in cells.items():
        r = {}
        g = offv.groupby(keys, dropna=False)['uvk']
        nd = g.transform('nunique')
        mode = g.transform(lambda x: x.value_counts().index[0])
        r['off_rows'] = int(len(offv))
        r['off_rows_at_cell_mode'] = int((offv['uvk'] == mode).sum())
        r['off_rows_single_value_cell'] = int((nd == 1).sum())
        r['off_cells'] = int(g.ngroups)
        # the on-grid rows' statistics in the same cell
        st = onv.groupby(keys, dropna=False)['uv'].agg(['median', 'mean', 'count'])
        jn = offv.join(st, on=keys)
        for stat in ('median', 'mean'):
            ok = jn[stat].notna()
            r[f'off_with_on_{stat}'] = int(ok.sum())
            r[f'off_eq_on_{stat}_1e-6'] = int((ok & ((jn['uv'] / jn[stat] - 1).abs() < 1e-6)).sum())
            r[f'off_within_1pct_on_{stat}'] = int((ok & ((jn['uv'] / jn[stat] - 1).abs() < 0.01)).sum())
        # all rows (on + off) medians, computed from the file as distributed
        st2 = f[f['uv'].notna() & (f['on'] | f['off'])].groupby(keys, dropna=False)['uv'].agg(['median', 'mean'])
        jn2 = offv.join(st2, on=keys)
        for stat in ('median', 'mean'):
            r[f'off_eq_all_{stat}_1e-6'] = int(((jn2['uv'] / jn2[stat] - 1).abs() < 1e-6).sum())
        # does an on-grid row share the off-grid modal unit value?
        onk = onv.groupby(keys, dropna=False)['uvk'].apply(set)
        jj = offv.join(onk.rename('onset'), on=keys)
        r['off_uv_also_on_some_on_grid_row'] = int(sum(1 for a, b in zip(jj['uvk'], jj['onset'])
                                                      if isinstance(b, set) and a in b))
        T[name] = r
    o['unit_value_cells'] = T
    o['off_top_unit_values'] = vc(offv['uvk'], 12)
    o['off_top_unit_values_by_item'] = {str(k): int(v) for k, v in
                                        offv.groupby(['item', 'u7', 'z7', 'uvk']).size().sort_values(ascending=False).head(12).items()}
    # an off-grid unit value: do on-grid rows in the cell show that exact value?
    # T8: q8 / uv on off rows -- is q7 = q8 / P with P the cell mode
    # q8 non-integer rows: is q8 = q7 x P ?
    nonint = offv[~offv['q8int'] & offv['q8'].notna()]
    o['off_q8_nonint_rows'] = int(len(nonint))
    o['off_q8_nonint_q7_decimals'] = vc(d7[nonint.index], 15)
    # T9: the computed rows by item group, household concentration
    o['off_share_by_vague'] = {str(v): share(int((off & (f['vague'] == v)).sum()), int((pos7 & (f['vague'] == v)).sum())) for v in (1.0, 2.0)}
    hh_off = off.groupby(hh).mean()[pos7.groupby(hh).sum() > 0]
    o['household_off_share_quantiles'] = [round(float(x), 3) for x in hh_off.quantile([0.1, 0.25, 0.5, 0.75, 0.9, 1.0])]
    o['households_with_any_off'] = int((hh_off > 0).sum())
    o['households_all_off'] = int((hh_off == 1).sum())
    o['households'] = int(len(hh_off))
    it_off = off.groupby(item).mean()[pos7.groupby(item).sum() >= 200]
    o['item_off_share_quantiles'] = [round(float(x), 3) for x in it_off.quantile([0, 0.1, 0.5, 0.9, 1.0])]
    # T10: NSU kilograms of the last purchase -- is q7 a conversion of a round kg?
    try:
        rawn, weights, _, _ = ehcvm._nsu_read('Niger', '2018-19')
        assert len(rawn) == len(s)
        w7 = pd.Series([weights.get((int(a), int(b), int(c)), np.nan)
                        if pd.notna(a) and pd.notna(b) and pd.notna(c) else np.nan
                        for a, b, c in zip(num(rawn['s07bq01']), u7, z7)], index=s.index)
        kg7 = q7 * w7
        rk = (np.abs(10 * kg7 - np.round(10 * kg7)) < 1e-6)
        o['kg7_round_0_1'] = {'off': share(int((off & rk).sum()), int((off & kg7.notna()).sum())),
                              'on': share(int((on & rk).sum()), int((on & kg7.notna()).sum()))}
        rk25 = on_grid(kg7.fillna(0.3))
        o['kg7_quarter_grid'] = {'off': share(int((off & rk25 & kg7.notna()).sum()), int((off & kg7.notna()).sum())),
                                 'on': share(int((on & rk25 & kg7.notna()).sum()), int((on & kg7.notna()).sum()))}
        w3 = pd.Series([weights.get((int(a), int(b), int(c)), np.nan)
                        if pd.notna(a) and pd.notna(b) and pd.notna(c) else np.nan
                        for a, b, c in zip(num(rawn['s07bq01']), u3, z3)], index=s.index)
        # if q7 was converted from the consumption unit: q7 * w7 / w3 on grid?
        conv = q7 * w7 / w3
        o['q7_times_w7_over_w3_on_grid'] = {'off': share(int((off & on_grid(conv.fillna(0.3)) & conv.notna()).sum()), int((off & conv.notna()).sum())),
                                            'on': share(int((on & on_grid(conv.fillna(0.3)) & conv.notna()).sum()), int((on & conv.notna()).sum()))}
    except Exception as e:  # noqa: BLE001
        o['nsu_error'] = f'{type(e).__name__}: {e}'
    # T11: community prices s05 (2018-19): is the off-grid unit value a community price?
    try:
        cp = raw('Niger', '2018-19/Data/s05_co_ner2018.dta')
        cpl = raw('Niger', '2018-19/Data/s05_co_ner2018.dta', True)
        o['s05_columns'] = list(cp.columns)[:20]
        prices = pd.concat([num(cp['s05q03']), num(cp['s05q04'])]).dropna()
        o['s05_price_rows'] = int(len(prices))
        pset = set(np.round(prices.to_numpy(), 6))
        o['off_uv_is_some_community_price'] = int(sum(1 for x in offv['uv'] if round(float(x), 6) in pset))
        o['on_uv_is_some_community_price'] = int(sum(1 for x in onv['uv'] if round(float(x), 6) in pset))
        # mean of the two readings, per grappe / product / unit-size
        cp2 = pd.DataFrame({'grappe': num(cp['grappe']), 'prod': cpl['s05q01'].astype(object).astype(str),
                            'us': num(cp['s05q02']),
                            'p': (num(cp['s05q03']) + num(cp['s05q04'])) / 2})
        mset = set(np.round(cp2['p'].dropna().to_numpy(), 6))
        o['off_uv_is_some_community_mean_of_two'] = int(sum(1 for x in offv['uv'] if round(float(x), 6) in mset))
        o['on_uv_is_some_community_mean_of_two'] = int(sum(1 for x in onv['uv'] if round(float(x), 6) in mset))
    except Exception as e:  # noqa: BLE001
        o['s05_error'] = f'{type(e).__name__}: {e}'
    # T12: rows whose purchase is served: Expenditure finite under the shipped rule
    pq = (q3.fillna(0) - q4 - q5).clip(lower=0)
    exp = ehcvm.derive_ehcvm_purchase_value(pq, s['s07bq03b'].astype(object), s['s07bq03c'].astype(object),
                                            q7, s['s07bq07b'].astype(object), s['s07bq07c'].astype(object), q8)
    exp = pd.Series(exp, index=s.index)
    keep = (pq > 0) | (exp > 0)
    o['served_purchased_source_rows'] = int(keep.sum())
    o['served_purchased_source_rows_off'] = int((keep & off).sum())
    o['served_valued_rows'] = int((keep & exp.notna()).sum())
    o['served_valued_rows_off'] = int((keep & exp.notna() & off).sum())
    tot = float(exp[keep].sum())
    o['served_expenditure_total'] = round(tot, 1)
    o['served_expenditure_off_share'] = share(float(exp[keep & off].sum()), tot)
    o['served_purchased_off_disabled'] = int((keep & off & ~enabled).sum())
    # labelled s07bq06 for the off-grid rows
    o['off_s07bq06_labels'] = vc(sl.loc[off, 's07bq06'].astype(object).astype(str))
    # 2021-22 for contrast: same counts
    try:
        s2 = raw('Niger', '2021-22/Data/s07b_me_ner2021.dta')
        q72 = num(s2['s07bq07a'])
        p2 = q72 > 0
        o['2021_q7a_pos'] = int(p2.sum())
        o['2021_q7a_off_grid'] = int((p2 & ~on_grid(q72)).sum())
        o['2021_q7a_filled_where_disabled'] = int((p2 & ~num(s2['s07bq06']).isin([1, 2, 3])).sum())
        q82 = num(s2['s07bq08'])
        o['2021_q8_nonint'] = int((q82.notna() & ((q82 - np.round(q82)).abs() > 1e-9)).sum())
    except Exception as e:  # noqa: BLE001
        o['2021_error'] = repr(e)
    return o


# --------------------------------------------------------------------------- Uganda
def uganda():
    import lsms_library as ll
    from lsms_library.derivations import resolve_callable
    comm_ea = resolve_callable('lsms_library.countries.Uganda._.uganda:comm_ea')
    o = {}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        sm = ll.Country('Uganda').sample()
    o['sample_seconds'] = round(time.time() - t0, 1)
    o['sample_warnings'] = dict(Counter(x.category.__name__ for x in w))
    f = sm.reset_index()
    o['sample_columns'] = list(sm.columns)
    o['sample_index'] = list(sm.index.names)
    o['rows_by_t'] = vc(f['t'])
    o['ea_na_by_t'] = {str(t): int(g['ea'].isna().sum()) for t, g in f.groupby('t')}
    nine = f[f['t'] == '2009-10']
    na = nine[nine['ea'].isna()]
    o['na_2009'] = int(len(na))
    o['na_2009_v_kind'] = vc(na['v'].astype(str).str[:1].map({'@': 'bin', 'u': 'unlocated'}).fillna('other'))
    wsum = nine['weight'].sum()
    o['na_2009_weight_share'] = share(float(na['weight'].sum()), float(wsum))
    o['na_2009_weight_na'] = int(na['weight'].isna().sum())
    others = f[(f['t'] != '2009-10') & f['ea'].notna()][['i', 't', 'ea']]
    cand = na[['i', 'weight', 'v']].merge(others, on='i', how='left', suffixes=('', '_o'))
    has = cand.groupby('i')['ea'].apply(lambda x: x.dropna().nunique())
    o['na_with_label'] = int((has >= 1).sum())
    o['na_with_two_plus_distinct_labels'] = int((has >= 2).sum())
    o['label_waves'] = vc(cand.dropna(subset=['ea'])['t_o'] if 't_o' in cand else cand.dropna(subset=['ea'])['t'], 10)
    waves_per = cand.dropna(subset=['ea']).groupby('i')['t'].apply(lambda x: tuple(sorted(set(x))))
    o['label_wave_patterns'] = vc(waves_per, 15)
    o['na_with_label_by_v_kind'] = vc(na.loc[na['i'].isin(has[has >= 1].index), 'v'].astype(str).str[:1].map({'@': 'bin', 'u': 'unlocated'}).fillna('other'))
    filled = na['i'].isin(has[has >= 1].index)
    o['weight_share_still_na_after_fill'] = share(float(na.loc[~filled, 'weight'].sum()), float(wsum))
    o['still_na_weight_na'] = int(na.loc[~filled, 'weight'].isna().sum())
    # nearest-wave choice vs agreement
    order = ['2005-06', '2009-10', '2010-11', '2011-12', '2013-14', '2015-16', '2018-19', '2019-20']
    lab = cand.dropna(subset=['ea']).copy()
    lab['dist'] = lab['t'].map(lambda t: abs(order.index(t) - 1))
    nearest = lab.sort_values(['i', 'dist', 't']).drop_duplicates('i').set_index('i')['ea']
    o['nearest_wave_used'] = vc(lab.sort_values(['i', 'dist', 't']).drop_duplicates('i')['t'])
    first = lab.sort_values(['i', 't']).drop_duplicates('i').set_index('i')['ea']
    o['nearest_eq_earliest'] = int((nearest == first.reindex(nearest.index)).sum())
    # Uganda id convention: is 2009-10's i the raw HHID?
    g9 = raw('Uganda', '2009-10/Data/GSEC1.dta')
    o['gsec1_2009_columns_head'] = list(g9.columns)[:40]
    g9i = g9['HHID'].map(format_id)
    o['served_2009_i_in_gsec1'] = int(nine['i'].isin(set(g9i)).sum())
    o['na_2009_i_in_gsec1'] = int(na['i'].isin(set(g9i)).sum())
    st = [c for c in g9.columns if 'status' in c.lower()]
    o['gsec1_status_columns'] = st
    if st:
        sv = pd.Series(g9[st[0]].astype(object).astype(str).to_numpy(), index=g9i.to_numpy())
        o['na_2009_status'] = vc(na['i'].map(sv))
        o['filled_status'] = vc(na.loc[filled, 'i'].map(sv))
        o['not_filled_status'] = vc(na.loc[~filled, 'i'].map(sv))
    # raw route: early waves' GSEC1 comm by the same raw HHID
    rawlab = {}
    for wv, rel in (('2005-06', '2005-06/Data/GSEC1.dta'), ('2010-11', '2010-11/Data/GSEC1.dta'),
                    ('2011-12', '2011-12/Data/GSEC1.dta')):
        g = raw('Uganda', rel)
        e = pd.Series(g['comm'].map(comm_ea).to_numpy(), index=g['HHID'].map(format_id).to_numpy())
        o[f'raw_{wv}_hhid_unique'] = bool(not e.index.duplicated().any())
        rawlab[wv] = e[~e.index.duplicated()]
    rl = pd.DataFrame({wv: na['i'].map(e) for wv, e in rawlab.items()})
    rl.index = na['i'].to_numpy()
    o['raw_early_label_counts'] = {wv: int(rl[wv].notna().sum()) for wv in rl}
    nd_raw = rl.apply(lambda r: r.dropna().nunique(), axis=1)
    o['raw_early_any'] = int((nd_raw >= 1).sum())
    o['raw_early_disagree'] = int((nd_raw >= 2).sum())
    served_lab = cand.dropna(subset=['ea']).groupby('i')['ea'].first()
    rfirst = rl.apply(lambda r: r.dropna().iloc[0] if r.notna().any() else None, axis=1)
    both = served_lab.index.intersection(rfirst.dropna().index)
    o['raw_vs_served_both'] = int(len(both))
    o['raw_vs_served_agree'] = int((served_lab.loc[both] == rfirst.loc[both]).sum())
    o['served_only_not_raw_early'] = int(len(served_lab.index.difference(rfirst.dropna().index)))
    # 2013-14: HHID_old route for those the early waves miss
    g13 = raw('Uganda', '2013-14/Data/GSEC1.dta')
    old = g13['HHID_old'].map(format_id)
    e13 = pd.Series(g13['ea'].map(format_id).to_numpy(), index=old.to_numpy())
    e13 = e13[e13.index.notna()]
    o['raw_2013_by_old'] = int(na['i'].isin(set(e13.index)).sum())
    miss = served_lab.index.difference(rfirst.dropna().index)
    o['served_only_found_2013_old'] = int(pd.Index(miss).isin(set(e13.index)).sum())
    return o


for key, fn in (('M', mali), ('S', senegal), ('N', niger), ('U', uganda)):
    if key in which:
        section(key, fn)
print('EXPLORE DONE', flush=True)
