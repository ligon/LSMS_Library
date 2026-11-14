"""GH #1559 Part 2 (measurement only): price-implied kg per crop unit vs survey kg.

Usage: part2.py BUILD_OUT REPORT_DIR.  BUILD_OUT holds the candidate pickles of
build.py.  Writes AGGREGATES only (cell counts, ratio quantiles, cell lists keyed
by wave/crop/unit/region -- never household rows).

Signals, per (wave, region, crop, unit):
  survey  kpu = Quantity_kg / Quantity on harvest rows (survey-reported, a total / q)
  price   ipu = median(Value_sold/Quantity_sold | Unit_sold == u)
                / median(Value_sold/Quantity_sold | Unit_sold == 'Kg')
  sold    spu = Quantity_sold_kg / Quantity_sold on sale rows (new column)
Gates (the food ladder's): >= MIN reports on each side; each side's spread
p90/p10 (n >= 10) or max/min (n < 10) <= SPREAD.  Ladder: (t, region, crop[, state])
-> (t, crop[, state]).  Nothing here is wired.
"""
import json
import re
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library as ll

build, rep = Path(sys.argv[1]), Path(sys.argv[2])
rep.mkdir(parents=True, exist_ok=True)
MIN, SPREAD = 5, 10.0
STATED = re.compile(r'(kg|kilo|gramme|tonne|litre)', re.I)


def f64(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def spread(x):
    x = np.asarray(x, dtype='float64')
    x = x[np.isfinite(x) & (x > 0)]
    if len(x) == 0:
        return np.inf
    if len(x) >= 10:
        lo, hi = np.quantile(x, [0.1, 0.9])
    else:
        lo, hi = x.min(), x.max()
    return hi / lo if lo > 0 else np.inf


def q(x, ps=(0.1, 0.25, 0.5, 0.75, 0.9)):
    x = pd.Series(x, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        return None
    return {f'p{int(p * 100)}': round(float(x.quantile(p)), 4) for p in ps} | {'n': int(x.size)}


def ratio_summary(r):
    r = pd.Series(r, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    r = r[r > 0]
    if r.empty:
        return {'n': 0}
    lr = np.abs(np.log(r))
    return {'n': int(r.size), 'median': round(float(r.median()), 4),
            'p10': round(float(r.quantile(.1)), 4), 'p90': round(float(r.quantile(.9)), 4),
            'median_abs_log': round(float(lr.median()), 4),
            'within_2x': round(float((lr <= np.log(2)).mean()), 4),
            'within_1.25x': round(float((lr <= np.log(1.25)).mean()), 4),
            'beyond_10x': int((lr > np.log(10)).sum()),
            'beyond_10x_low': int((r < 0.1).sum()), 'beyond_10x_high': int((r > 10).sum())}


def nominal_mass(label):
    """kg a sold-unit label STATES: Kg 1, Gramme 0.001, Tonne 1000, 'Sac de 50 kg' 50."""
    if label is None or pd.isna(label):
        return np.nan
    s = str(label).strip().lower()
    if s in ('kg', 'kilogramme', 'kilogrammes'):
        return 1.0
    if s.startswith('gramme'):
        return 0.001
    if s.startswith('tonne'):
        return 1000.0
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*kg', s)
    return float(m.group(1).replace(',', '.')) if m else np.nan


def gated(values):
    v = np.asarray(values, dtype='float64')
    v = v[np.isfinite(v) & (v > 0)]
    return len(v) >= MIN and spread(v) <= SPREAD, len(v), float(np.median(v)) if len(v) else np.nan


report = {}
for country in ('Niger', 'Mali'):
    df = pd.read_pickle(build / f'{country}_crop_production.pkl').reset_index()
    cf = pd.read_pickle(build / f'{country}_cluster_features.pkl').reset_index()
    if 'u' not in df.columns:
        raise SystemExit('no u')
    if 'v' in df.columns and 'Region' in cf.columns:
        reg = cf[['t', 'v', 'Region']].drop_duplicates(['t', 'v'])
        df = df.merge(reg, on=['t', 'v'], how='left')
    else:
        df['Region'] = pd.NA
    df['Region'] = df['Region'].astype('string').fillna('<no region>')
    for col in ('Quantity', 'Quantity_kg', 'Quantity_sold', 'Quantity_sold_kg', 'Value_sold'):
        df[col] = f64(df[col]) if col in df.columns else np.nan
    df['u'] = df['u'].astype('string')
    df['Unit_sold'] = df['Unit_sold'].astype('string')
    df['crop'] = df['crop'].astype('string')
    for col in ('condition', 'Condition_sold'):
        df[col] = df[col].astype('string') if col in df.columns else pd.NA
    dry = (df['t'] == '2011-12') & (df['plot_id'] == 'Farm-level') if country == 'Niger' else \
        pd.Series(False, index=df.index)
    R = {'rows': int(len(df)), 'regions_by_wave': {w: int(g['Region'].nunique())
                                                     for w, g in df.groupby('t')}}

    # ---- donors: sales -----------------------------------------------------
    S = df[(df['Quantity_sold'] > 0) & (df['Value_sold'] > 0) & df['Unit_sold'].notna()].copy()
    S['dry'] = dry[S.index].to_numpy()
    S['price'] = S['Value_sold'] / S['Quantity_sold']
    S['kg_sale'] = S['Unit_sold'].eq('Kg').fillna(False)
    # The food ladder's baseline reads every label that STATES a mass at nominal
    # (a sack of 50 kg is 50 kg); variant B does the same here.
    S['mass'] = S['Unit_sold'].map(nominal_mass)
    S['price_kg_nominal'] = S['price'] / S['mass']
    S['spu'] = (S['Quantity_sold_kg'] / S['Quantity_sold']).where(S['Quantity_sold_kg'] > 0)
    R['sales'] = {w: {'rows': int(len(g)), 'in_Kg': int(g['kg_sale'].sum()),
                      'with_sold_kg': int(g['spu'].notna().sum()),
                      'dry_3E': int(g['dry'].sum())} for w, g in S.groupby('t')}
    S_rainy = S[~S['dry']]   # 3E's window is per cycle and its value window is unstated

    # ---- recipients: harvest rows with survey kg -----------------------------
    H = df[(df['Quantity'] > 0) & (df['Quantity_kg'] > 0) & df['u'].notna()
           & ~df['u'].isin(['Unknown'])].copy()
    H['kpu'] = H['Quantity_kg'] / H['Quantity']
    H['stated'] = H['u'].str.contains(STATED).fillna(False)
    H['dry'] = dry[H.index].to_numpy()
    R['survey_kg_rows'] = {w: {'rows': int(len(g)), 'unstated_unit_rows': int((~g['stated']).sum()),
                               'unstated_kg': round(float(g.loc[~g['stated'], 'Quantity_kg'].sum()), 1),
                               'all_kg': round(float(g['Quantity_kg'].sum()), 1),
                               'units_unstated': {k: int(v) for k, v in
                                                  g.loc[~g['stated'], 'u'].value_counts().head(8).items()}}
                           for w, g in H.groupby('t')}

    # ---- price-implied factors on a ladder -----------------------------------
    def implied(keys, donors, state_from=None, variant='kg_only'):
        """{(keys..., u): (ipu, n_unit, n_kg)} for gated cells."""
        d = donors.copy()
        if state_from:
            d = d[d[state_from].notna()]
            d['_state'] = d[state_from]
            keys = keys + ['_state']
        if variant == 'kg_only':
            in_base, base_price = d['kg_sale'].to_numpy(dtype=bool), 'price'
        else:
            in_base, base_price = d['mass'].notna().to_numpy(), 'price_kg_nominal'
        base = {}
        for k, g in d[in_base].groupby(keys, dropna=False):
            ok, n, med = gated(g[base_price])
            base[k if isinstance(k, tuple) else (k,)] = (ok, n, med)
        out, cells_all, cells_n, cells_spread_fail = {}, 0, 0, 0
        for k, g in d[~in_base].groupby(keys + ['Unit_sold'], dropna=False):
            cells_all += 1
            kk = k[:-1]
            ok, n, med = gated(g['price'])
            b = base.get(kk)
            if b is not None and n >= MIN and b[1] >= MIN:
                cells_n += 1
                if not (ok and b[0]):
                    cells_spread_fail += 1
            if ok and b is not None and b[0]:
                out[k] = (med / b[2], n, b[1])
        stats = {'unit_cells_with_any_sale': cells_all,
                 'unit_cells_with_min_n_both_sides': cells_n,
                 'of_which_refused_by_spread': cells_spread_fail,
                 'kg_baseline_cells': len(base),
                 'kg_baseline_cells_passing': int(sum(1 for b in base.values() if b[0])),
                 'implied_cells': len(out),
                 'implied_cells_unstated_unit': int(sum(1 for k in out if not STATED.search(str(k[-1]))))}
        return out, stats

    ladders = {
        'no_state': [['t', 'Region', 'crop'], ['t', 'crop']],
    }
    results = {}
    for name, rungs in ladders.items():
        for state, variant in ((None, 'kg_only'), ('Condition_sold', 'kg_only'),
                               (None, 'stated_mass_nominal'),
                               ('Condition_sold', 'stated_mass_nominal')):
            label = (name if state is None else 'sold_state_donor') + (
                '' if variant == 'kg_only' else '+nominal_sacks_baseline')
            tables = [implied(list(r), S_rainy, state, variant) for r in rungs]
            # assign to recipients, first rung that resolves
            Hr = H[~H['dry']].copy()
            if state:
                Hr['_state'] = Hr['condition']
            Hr['ipu'] = np.nan
            Hr['rung'] = pd.NA
            for (tab, st), r in zip(tables, rungs):
                keys = list(r) + (['_state'] if state else []) + ['u']
                look = pd.Series({k: v[0] for k, v in tab.items()}, dtype='float64')
                if look.empty:
                    continue
                look.index = pd.MultiIndex.from_tuples(look.index, names=keys)
                idx = pd.MultiIndex.from_frame(Hr[keys])
                val = look.reindex(idx).to_numpy()
                fill = np.isnan(Hr['ipu'].to_numpy()) & ~np.isnan(val)
                Hr.loc[fill, 'ipu'] = val[fill]
                Hr.loc[fill, 'rung'] = '+'.join(r)
            Hr['ratio'] = Hr['kpu'] / Hr['ipu']
            res = {'rung_stats': {'+'.join(r): st for (_, st), r in zip(tables, rungs)},
                   'coverage': {}, 'agreement': {}, 'agreement_by_unit': {}, 'disagree_10x_cells': []}
            for w, g in Hr.groupby('t'):
                un = g[~g['stated']]
                res['coverage'][w] = {
                    'survey_kg_rows': int(len(g)), 'covered': int(g['ipu'].notna().sum()),
                    'unstated_rows': int(len(un)), 'unstated_covered': int(un['ipu'].notna().sum()),
                    'unstated_kg': round(float(un['Quantity_kg'].sum()), 1),
                    'unstated_kg_covered': round(float(un.loc[un['ipu'].notna(), 'Quantity_kg'].sum()), 1),
                    'by_rung': {str(k): int(v) for k, v in g['rung'].value_counts().items()}}
                res['agreement'][w] = {'all': ratio_summary(g['ratio']),
                                       'unstated': ratio_summary(un['ratio']),
                                       'stated': ratio_summary(g.loc[g['stated'], 'ratio'])}
                for u_, gu in g[g['ipu'].notna()].groupby('u'):
                    if len(gu) >= 20:
                        res['agreement_by_unit'][f'{w} {u_}'] = ratio_summary(gu['ratio'])
                bad = g[(g['ratio'] > 10) | (g['ratio'] < 0.1)]
                for k, gb in bad.groupby(['crop', 'u', 'rung'], dropna=False):
                    res['disagree_10x_cells'].append({
                        'wave': w, 'crop': k[0], 'u': k[1], 'rung': str(k[2]),
                        'rows': int(len(gb)), 'median_ratio': round(float(gb['ratio'].median()), 3),
                        'median_survey_kpu': round(float(gb['kpu'].median()), 3),
                        'implied_kpu': round(float(gb['ipu'].median()), 3)})
            res['disagree_10x_cells'] = sorted(res['disagree_10x_cells'], key=lambda d: -d['rows'])[:40]
            results[label] = res
    R['price_implied'] = results

    # ---- the sold-kg signal ---------------------------------------------------
    sp = S[S['spu'].notna()].copy()
    sold = {'by_wave_unit': {}, 'vs_survey_same_row': {}, 'vs_price_implied': {}, 'heaping': {}}
    for (w, u_), g in sp.groupby(['t', 'Unit_sold']):
        if len(g) >= 10:
            sold['by_wave_unit'][f'{w} {u_}'] = q(g['spu'])
    # same row: a sale on its harvest line in the harvest's unit
    same = sp[(sp['u'] == sp['Unit_sold']).fillna(False).astype(bool)
              & (sp['Quantity'] > 0) & (sp['Quantity_kg'] > 0)].copy()
    same['kpu'] = same['Quantity_kg'] / same['Quantity']
    same['r'] = same['spu'] / same['kpu']
    for w, g in same.groupby('t'):
        sold['vs_survey_same_row'][w] = ratio_summary(g['r']) | {
            'exactly_equal': int(np.isclose(g['spu'], g['kpu']).sum()),
            'unstated_unit': ratio_summary(g.loc[~g['u'].str.contains(STATED).fillna(False), 'r'])}
    # cell level: sold kg per unit vs price-implied (national, no state)
    tab, _ = implied(['t', 'crop'], S_rainy)
    look = {k: v[0] for k, v in tab.items()}
    sp_r = sp[~sp['dry']].copy()
    sp_r['ipu'] = [look.get((a, b, c), np.nan) for a, b, c in zip(sp_r['t'], sp_r['crop'], sp_r['Unit_sold'])]
    sp_r['r'] = sp_r['spu'] / sp_r['ipu']
    for w, g in sp_r.groupby('t'):
        un = g[~g['Unit_sold'].str.contains(STATED).fillna(False)]
        sold['vs_price_implied'][w] = {'all': ratio_summary(g['r']), 'unstated': ratio_summary(un['r'])}
    # heaping: share at the unit's modal value, sold vs harvest
    for (w, u_), g in sp.groupby(['t', 'Unit_sold']):
        if len(g) < 30:
            continue
        s = g['spu'].round(3)
        h = H.loc[(H['t'] == w) & (H['u'] == u_), 'kpu'].round(3)
        sold['heaping'][f'{w} {u_}'] = {
            'sold_n': int(len(s)), 'sold_mode': float(s.mode().iloc[0]), 'sold_at_mode': round(float((s == s.mode().iloc[0]).mean()), 3),
            'harvest_n': int(len(h)),
            'harvest_mode': float(h.mode().iloc[0]) if len(h) else None,
            'harvest_at_mode': round(float((h == h.mode().iloc[0]).mean()), 3) if len(h) else None}
    R['sold_kg_signal'] = sold

    # ---- caveats ---------------------------------------------------------------
    cav = {}
    # (a) state: survey kpu by harvest condition; price per kg by sold state
    st = {}
    for (w, c_, u_), g in H[H['condition'].isin(['shelled', 'unshelled'])].groupby(['t', 'crop', 'u']):
        m = g.groupby('condition')['kpu'].agg(['median', 'size'])
        if {'shelled', 'unshelled'} <= set(m.index) and m['size'].min() >= 10:
            st[f'{w} {c_} {u_}'] = {'shelled': round(float(m.loc['shelled', 'median']), 3),
                                    'unshelled': round(float(m.loc['unshelled', 'median']), 3),
                                    'n': [int(m.loc['shelled', 'size']), int(m.loc['unshelled', 'size'])]}
    cav['survey_kpu_by_harvest_state'] = st
    pk = {}
    kgs = S[S['kg_sale'] & S['Condition_sold'].isin(['shelled', 'unshelled'])]
    for (w, c_), g in kgs.groupby(['t', 'crop']):
        m = g.groupby('Condition_sold')['price'].agg(['median', 'size'])
        if {'shelled', 'unshelled'} <= set(m.index) and m['size'].min() >= 5:
            pk[f'{w} {c_}'] = {'shelled': round(float(m.loc['shelled', 'median']), 1),
                               'unshelled': round(float(m.loc['unshelled', 'median']), 1),
                               'n': [int(m.loc['shelled', 'size']), int(m.loc['unshelled', 'size'])]}
    cav['kg_price_by_sold_state'] = pk
    ps = {}
    for (w, c_, u_), g in S[~S['kg_sale'] & S['Condition_sold'].isin(['shelled', 'unshelled'])].groupby(
            ['t', 'crop', 'Unit_sold']):
        m = g.groupby('Condition_sold')['price'].agg(['median', 'size'])
        if {'shelled', 'unshelled'} <= set(m.index) and m['size'].min() >= 10:
            ps[f'{w} {c_} {u_}'] = {'shelled': round(float(m.loc['shelled', 'median']), 1),
                                    'unshelled': round(float(m.loc['unshelled', 'median']), 1),
                                    'n': [int(m.loc['shelled', 'size']), int(m.loc['unshelled', 'size'])]}
    cav['unit_price_by_sold_state'] = ps
    # sold state vs harvest state on the same row
    both = S[S['Condition_sold'].notna() & S['condition'].isin(['shelled', 'unshelled', 'shell_not_applicable'])]
    cav['sold_state_differs_from_harvest_state'] = {
        w: {'rows': int(len(g)), 'differ': int((g['Condition_sold'] != g['condition']).sum())}
        for w, g in both.groupby('t')}
    # (b) selection: survey kpu of sellers vs non-sellers, same (t, crop, u)
    seller_pairs = set(zip(S['t'], S['i'], S['crop']))
    H['seller'] = [(a, b, c_) in seller_pairs for a, b, c_ in zip(H['t'], H['i'], H['crop'])]
    sel = []
    for (w, c_, u_), g in H[~H['dry']].groupby(['t', 'crop', 'u']):
        a, b = g.loc[g['seller'], 'kpu'], g.loc[~g['seller'], 'kpu']
        if len(a) >= 10 and len(b) >= 10:
            sel.append({'wave': w, 'crop': c_, 'u': u_, 'n_sellers': int(len(a)), 'n_non': int(len(b)),
                        'ratio_of_medians': round(float(a.median() / b.median()), 3)})
    cav['selection_seller_vs_nonseller_kpu'] = {
        'cells': len(sel), 'ratio_of_medians': q([d['ratio_of_medians'] for d in sel]),
        'largest': sorted(sel, key=lambda d: -abs(np.log(d['ratio_of_medians'])))[:10]}
    # (b') share of harvest households that sell the crop at all
    cav['seller_share_of_survey_kg_rows'] = {w: round(float(g['seller'].mean()), 3) for w, g in H.groupby('t')}
    # (c) seasonality: price per kg by interview month (Kg sales), per crop
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            idate = ll.Country(country).interview_date().reset_index()
        dcol = [c_ for c_ in idate.columns if c_.lower().startswith('int_t') or 'date' in c_.lower()
                or c_.lower().startswith('interview start')]
        dcol = dcol[0]
        idate['month'] = pd.to_datetime(idate[dcol], errors='coerce').dt.month
        mm = idate.dropna(subset=['month']).groupby(['t', 'i'])['month'].min().reset_index()
        K = S[S['kg_sale']].merge(mm, on=['t', 'i'], how='left')
        sea = {}
        for (w, c_), g in K.dropna(subset=['month']).groupby(['t', 'crop']):
            m = g.groupby('month')['price'].agg(['median', 'size'])
            m = m[m['size'] >= 5]
            if len(m) >= 2:
                sea[f'{w} {c_}'] = {'months': [int(x) for x in m.index],
                                    'median_by_month': [round(float(x), 1) for x in m['median']],
                                    'n_by_month': [int(x) for x in m['size']],
                                    'max_over_min': round(float(m['median'].max() / m['median'].min()), 3)}
        cav['kg_price_by_interview_month'] = sea
        cav['interview_month_join'] = {'kg_sales': int(len(K)), 'with_month': int(K['month'].notna().sum()),
                                       'date_column': dcol}
    except Exception as e:  # noqa: BLE001
        cav['kg_price_by_interview_month'] = f'not measured: {type(e).__name__}: {str(e)[:200]}'
    R['caveats'] = cav
    report[country] = R
    print(country, 'done', flush=True)

(rep / 'part2.json').write_text(json.dumps(report, indent=1, default=str, ensure_ascii=False))
print('wrote', rep / 'part2.json')
