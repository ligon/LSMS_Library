"""GH #1559 measurements on the cold CANDIDATE frames: the screen, and the rung memo.

Usage: analysis.py FRAMES_DIR OUT_JSON.  Run in the candidate clone (PYTHONPATH, cwd).
Writes AGGREGATES only: counts, quantiles, and cells keyed by wave / crop / unit --
never a household row.  Every price-implied number comes from the library's own
estimator (quantity_audit.survey_kg_price_factors) on a re-shaped frame, so the
memo's numbers and the screen's are one computation.
"""
import json
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library import quantity_audit as Q
from lsms_library.transformations import harvest_kg_factors

frames, out_json = Path(sys.argv[1]), Path(sys.argv[2])
warnings.simplefilter('ignore')


def f64(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def q(x, ps=(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)):
    x = pd.Series(x, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        return {'n': 0}
    return {'n': int(x.size)} | {f'p{round(p * 100)}': round(float(x.quantile(p)), 4) for p in ps}


def agree(r):
    r = pd.Series(r, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    r = r[r > 0]
    if r.empty:
        return {'n': 0}
    lr = np.abs(np.log(r))
    return {'n': int(r.size), 'median': round(float(r.median()), 4),
            'p10': round(float(r.quantile(.1)), 4), 'p90': round(float(r.quantile(.9)), 4),
            'median_abs_log': round(float(lr.median()), 4),
            'within_1.25x': round(float((lr <= np.log(1.25)).mean()), 4),
            'within_2x': round(float((lr <= np.log(2)).mean()), 4),
            'below_0.1': int((r < 0.1).sum()), 'above_10': int((r > 10).sum())}


def flat_of(df):
    flat = df.reset_index()
    for c in ('u', 'Unit_sold', 'crop', 'condition', 'Condition_sold', 't', 'plot_id', 'i'):
        if c in flat.columns:
            flat[c] = flat[c].astype(object).where(flat[c].notna(), None)
    return flat


def factors(flat):
    got = Q.survey_kg_price_factors(flat)
    return got.reset_index(drop=True) if got is not None else None


def flags(fac):
    return fac['kg_price_flag'].notna().to_numpy() if fac is not None else np.zeros(0, bool)


def keys_of(flat, mask):
    cols = [c for c in ('t', 'i', 'plot_id', 'crop', 'u', 'condition', 'production_obs')
            if c in flat.columns]
    return set(map(tuple, flat.loc[mask, cols].astype(str).to_numpy()))


def firing(flat, fac):
    """Per wave: rows eligible (mass-less unit, survey kg), judged, low, high; kg."""
    out = {}
    elig = fac['kg_survey'].notna()
    judged = fac['kg_price_implied'].notna() & elig
    for w in sorted(flat['t'].unique()):
        m = (flat['t'] == w).to_numpy()
        kg = f64(flat['Quantity_kg'])
        out[w] = {'rows': int(m.sum()),
                  'survey_kg_rows': int((m & kg.gt(0).to_numpy()).sum()),
                  'eligible_massless': int((m & elig).sum()),
                  'eligible_kg': round(float(kg[m & elig].sum()), 1),
                  'judged': int((m & judged).sum()),
                  'judged_kg': round(float(kg[m & judged].sum()), 1),
                  'low': int((m & (fac['kg_price_flag'] == 'low')).sum()),
                  'high': int((m & (fac['kg_price_flag'] == 'high')).sum()),
                  'flagged_kg': round(float(kg[m & fac['kg_price_flag'].notna()].sum()), 1),
                  'by_rung': {str(k): int(v) for k, v in
                              fac.loc[m & judged, 'kg_price_rung'].value_counts().items()},
                  'agreement': agree(fac.loc[m & judged, 'survey_over_implied'])}
    return out


def flagged_cells(flat, fac, top=30):
    m = fac['kg_price_flag'].notna()
    if not m.any():
        return []
    g = pd.DataFrame({'t': flat['t'], 'crop': flat['crop'], 'u': flat['u'],
                      'rung': fac['kg_price_rung'], 'flag': fac['kg_price_flag'],
                      'kg_survey': fac['kg_survey'], 'implied': fac['kg_price_implied'],
                      'ratio': fac['survey_over_implied'],
                      'n_unit': fac['kg_price_n_unit'], 'n_kg': fac['kg_price_n_kg']})[m]
    rows = []
    for k, x in g.groupby(['t', 'crop', 'u', 'rung', 'flag'], dropna=False):
        rows.append({'wave': k[0], 'crop': k[1], 'u': k[2], 'rung': k[3], 'flag': k[4],
                     'rows': int(len(x)), 'median_ratio': round(float(x['ratio'].median()), 3),
                     'median_survey_kpu': round(float(x['kg_survey'].median()), 3),
                     'implied_kpu': round(float(x['implied'].median()), 3),
                     'n_unit_sales': int(x['n_unit'].median()), 'n_kg_sales': int(x['n_kg'].median())})
    return sorted(rows, key=lambda d: -d['rows'])[:top]


def with_region(flat, cf):
    if 'v' not in flat.columns or 'Region' not in cf.columns:
        return None
    reg = cf.reset_index()[['t', 'v', 'Region']].drop_duplicates(['t', 'v'])
    merged = flat.merge(reg, on=['t', 'v'], how='left')
    assert len(merged) == len(flat)
    return merged['Region'].astype(object).where(merged['Region'].notna(), '<no region>').to_numpy()


def region_ladder(flat, region):
    """Region -> national: the module run with Region as an extra key on every rung,
    then nationally where the regional run found nothing (the #1559 Part 2 ladder,
    with the state rung on top of each)."""
    reg = factors(flat.assign(season=region))
    nat = factors(flat)
    take = reg['kg_price_implied'].notna()
    out = nat.copy()
    out.loc[take] = reg.loc[take]
    out.loc[take, 'kg_price_rung'] = 'region:' + reg.loc[take, 'kg_price_rung'].astype(str)
    return out


def rung_sim(flat, hk_source):
    """Rows the rung would SERVE: Quantity > 0, a usable mass-less unit, now on 'none'."""
    qty = f64(flat['Quantity'])
    sim = flat.assign(Quantity_kg=np.where(qty > 0, qty, np.nan))
    fac = factors(sim)
    none = (hk_source == 'none') & (qty > 0).to_numpy()
    eligible = none & fac['kg_survey'].notna().to_numpy()   # mass-less usable unit
    served = eligible & fac['kg_price_implied'].notna().to_numpy()
    out = {}
    for w in sorted(flat['t'].unique()):
        m = (flat['t'] == w).to_numpy()
        kg = qty * fac['kg_price_implied']
        out[w] = {'none_positive': int((m & none).sum()),
                  'none_massless_unit': int((m & eligible).sum()),
                  'served': int((m & served).sum()),
                  'kg_added': round(float(kg[m & served].sum()), 1),
                  'by_unit': {str(k): int(v) for k, v in
                              flat.loc[m & served, 'u'].value_counts().head(10).items()},
                  'unserved_units': {str(k): int(v) for k, v in
                                     flat.loc[m & eligible & ~served, 'u'].value_counts().head(10).items()},
                  'by_rung': {str(k): int(v) for k, v in
                              fac.loc[m & served, 'kg_price_rung'].value_counts().items()},
                  'implied_by_unit': {str(u): {'rows': int(len(g)),
                                               'median_kg_per_unit': round(float(g.median()), 3),
                                               'max_kg_per_unit': round(float(g.max()), 3),
                                               'rows_over_250': int((g > 250).sum())}
                                      for u, g in fac.loc[m & served, 'kg_price_implied'].groupby(
                                          flat.loc[m & served, 'u'])}}
    return out, fac, served


def holdout(flat, labels):
    """Answer key: a sold unit that STATES its mass, held out of the baseline and
    inferred from prices.  Returns implied / stated per sale row and per cell."""
    res = {}
    stated = Q._stated_masses(flat['Unit_sold'].to_numpy(dtype=object))
    for label in labels:
        m = (flat['Unit_sold'] == label).to_numpy()
        if m.sum() < 5:
            continue
        mass = float(np.nanmedian(stated[m]))
        h = flat.copy()
        h.loc[m, 'Unit_sold'] = 'HOLDOUT'
        h['Quantity_kg'] = np.nan
        h.loc[m, 'u'] = 'HOLDOUT'
        h.loc[m, 'Quantity'] = 1.0
        h.loc[m, 'Quantity_kg'] = 1.0
        if 'condition' in h.columns and 'Condition_sold' in h.columns:
            h.loc[m, 'condition'] = h.loc[m, 'Condition_sold'].where(
                h.loc[m, 'Condition_sold'].notna(), 'unknown_condition')
        fac = factors(h)
        imp = fac.loc[m, 'kg_price_implied']
        ratio = imp / mass
        cells = pd.DataFrame({'t': h.loc[m, 't'], 'crop': h.loc[m, 'crop'],
                              'rung': fac.loc[m, 'kg_price_rung'], 'r': ratio})
        cells = cells.dropna(subset=['r']).groupby(['t', 'crop', 'rung'])['r'].agg(['median', 'size'])
        res[label] = {'stated_kg': mass, 'sales': int(m.sum()), 'rows_inferred': int(imp.notna().sum()),
                      'row_agreement': agree(ratio),
                      'cell_agreement': agree(cells['median']) if len(cells) else {'n': 0},
                      'cells': [{'wave': k[0], 'crop': k[1], 'rung': k[2],
                                 'implied_over_stated': round(float(v['median']), 3),
                                 'sales': int(v['size'])}
                                for k, v in cells.sort_values('size', ascending=False).head(15).iterrows()]}
    return res


report = {}
for country in ('Niger', 'Mali'):
    df = pd.read_pickle(frames / f'{country}_crop_production.pkl')
    cf = pd.read_pickle(frames / f'{country}_cluster_features.pkl')
    site = json.loads((frames / f'{country}_site_reports.json').read_text())
    flat = flat_of(df)
    R = {'rows': int(len(df))}

    # ---- 1. the screen as served (real path) and as recomputed -------------
    R['real_path'] = sorted([(r['wave'], r['direction'], r['n_implausible'], r['n_judged'])
                             for r in site])
    fac = factors(flat)
    if fac is None:
        report[country] = R | {'note': 'nothing to judge'}
        continue
    R['recomputed_equals_real_path'] = (
        int(flags(fac).sum()) == sum(r['n_implausible'] for r in site))
    R['firing'] = firing(flat, fac)
    R['tails'] = {w: q(fac.loc[(flat['t'] == w).to_numpy(), 'survey_over_implied'])
                  for w in sorted(flat['t'].unique())}
    R['flagged_cells'] = flagged_cells(flat, fac)
    R['judged_by_unit'] = {f'{w} {u}': agree(g['r']) for (w, u), g in pd.DataFrame(
        {'t': flat['t'], 'u': flat['u'], 'r': fac['survey_over_implied']}).dropna().groupby(['t', 'u'])
        if len(g) >= 20}

    # ---- 2. the alternatives the ruling does not settle ---------------------
    base_flag = flags(fac)
    base_keys = keys_of(flat, base_flag)
    alts = {}
    nostate = factors(flat.assign(Condition_sold=None))
    alts['no_state'] = nostate
    region = with_region(flat, cf)
    if region is not None:
        alts['region_then_national'] = region_ladder(flat, region)
    if country == 'Niger':
        dry = ((flat['t'] == '2011-12') & (flat['plot_id'] == 'Farm-level')).to_numpy()
        x = flat.copy()
        x.loc[dry, ['Quantity_sold', 'Value_sold', 'Quantity_kg']] = np.nan
        alts['niger_3e_excluded'] = factors(x)
        R['niger_3e'] = {'rows': int(dry.sum()), 'flagged_rows': int((base_flag & dry).sum()),
                         'judged_rows': int((dry & fac['kg_price_implied'].notna().to_numpy()
                                             & fac['kg_survey'].notna().to_numpy()).sum())}
        if region is not None:
            alts['part2_like_region_no_state_3e_excluded'] = region_ladder(
                x.assign(Condition_sold=None), region)
    if country == 'Mali' and 'Derivation' in flat.columns:
        tree = flat['Derivation'].astype(str).str.contains('tree-sale-window').to_numpy()
        x = flat.copy()
        x.loc[tree, ['Quantity_sold', 'Value_sold']] = np.nan
        alts['mali_tree_sales_excluded'] = factors(x)
        R['mali_tree_sale_rows'] = int(tree.sum())
    R['alternatives'] = {}
    for name, a in alts.items():
        af = flags(a)
        ak = keys_of(flat, af)
        R['alternatives'][name] = {
            'flagged': int(af.sum()), 'only_in_alternative': len(ak - base_keys),
            'only_in_screen': len(base_keys - ak),
            'judged': int((a['kg_price_implied'].notna() & a['kg_survey'].notna()).sum()),
            'by_wave': {w: {'low': int(((flat['t'] == w).to_numpy() & (a['kg_price_flag'] == 'low')).sum()),
                            'high': int(((flat['t'] == w).to_numpy() & (a['kg_price_flag'] == 'high')).sum()),
                            'judged': int(((flat['t'] == w).to_numpy() & a['kg_price_implied'].notna()
                                           & a['kg_survey'].notna()).sum()),
                            'agreement': agree(a.loc[(flat['t'] == w).to_numpy(), 'survey_over_implied'])}
                        for w in sorted(flat['t'].unique())}}

    # ---- 3. overlap with the label screen ------------------------------------
    hk = harvest_kg_factors(df)
    off = hk['kg_survey_off_label'].to_numpy(dtype=bool)
    R['overlap_with_off_label'] = {'off_label': int(off.sum()), 'price_flagged': int(base_flag.sum()),
                                   'both': int((off & base_flag).sum())}
    R['kg_factor_sources'] = hk.attrs.get('kg_factor_sources')

    # ---- 4. cost -------------------------------------------------------------
    def best(fn, k=5):
        ts = []
        for _ in range(k):
            t0 = time.perf_counter()
            fn()
            ts.append(time.perf_counter() - t0)
        return round(min(ts) * 1000, 1)
    R['cost_ms'] = {'audit_survey_kg_prices': best(lambda: Q.audit_survey_kg_prices(df, country=country)),
                    'check_quantities_with': best(lambda: Q.check_quantities(df, country=country,
                                                                             table='crop_production'))}
    orig = Q.audit_survey_kg_prices
    Q.audit_survey_kg_prices = lambda *a, **k: []
    try:
        R['cost_ms']['check_quantities_without'] = best(
            lambda: Q.check_quantities(df, country=country, table='crop_production'))
    finally:
        Q.audit_survey_kg_prices = orig
    spent = []

    def spy(*a, **k):
        t0 = time.perf_counter()
        try:
            return orig(*a, **k)
        finally:
            spent.append(time.perf_counter() - t0)
    Q.audit_survey_kg_prices = spy
    try:
        totals = []
        for _ in range(5):
            spent.clear()
            t0 = time.perf_counter()
            ll.Country(country).crop_production()
            totals.append((time.perf_counter() - t0, sum(spent), len(spent)))
    finally:
        Q.audit_survey_kg_prices = orig
    best_total = min(totals)
    R['cost_ms']['inside_warm_read'] = {'read_ms': round(best_total[0] * 1000, 1),
                                        'screen_ms': round(best_total[1] * 1000, 1),
                                        'screen_calls': best_total[2]}
    Q._clear_quantity_reports()

    # ---- 5. the rung (design memo, nothing wired) -----------------------------
    src = hk['KgFactorSource'].to_numpy(dtype=object)
    R['rung'] = {}
    R['rung']['national'], simfac, served = rung_sim(flat, src)
    # context: the kilograms the ladder serves today, per wave (survey_kg rows read
    # their own total; every other row Quantity x kg_per_unit)
    qty_now = f64(flat['Quantity']).to_numpy()
    kpu = hk['kg_per_unit'].to_numpy(dtype='float64')
    kg_now = np.where(src == 'survey_kg', f64(flat['Quantity_kg']).to_numpy()
                      if 'Quantity_kg' in flat.columns else np.nan, qty_now * kpu)
    R['rung']['kg_served_now'] = {w: round(float(np.nansum(kg_now[(flat['t'] == w).to_numpy()])), 1)
                                  for w in sorted(flat['t'].unique())}
    R['rung']['source_by_wave'] = {w: {str(k): int(v) for k, v in
                                       pd.Series(src[(flat['t'] == w).to_numpy()]).value_counts().items()}
                                   for w in sorted(flat['t'].unique())}
    if region is not None:
        qty = f64(flat['Quantity'])
        sim = flat.assign(Quantity_kg=np.where(qty > 0, qty, np.nan))
        rl = region_ladder(sim, region)
        none = (src == 'none') & (qty > 0).to_numpy()
        srv = none & rl['kg_survey'].notna().to_numpy() & rl['kg_price_implied'].notna().to_numpy()
        R['rung']['region_then_national_served'] = {
            w: int(((flat['t'] == w).to_numpy() & srv).sum()) for w in sorted(flat['t'].unique())}
    # selection: served rows whose household sold that crop at all
    sold_pairs = set(zip(flat.loc[f64(flat['Quantity_sold']).gt(0), 't'],
                         flat.loc[f64(flat['Quantity_sold']).gt(0), 'i'],
                         flat.loc[f64(flat['Quantity_sold']).gt(0), 'crop']))
    seller = np.array([(a, b, c) in sold_pairs for a, b, c in zip(flat['t'], flat['i'], flat['crop'])])
    R['rung']['served_rows_from_sellers'] = {
        w: round(float(seller[(flat['t'] == w).to_numpy() & served].mean()), 3)
        if ((flat['t'] == w).to_numpy() & served).any() else None for w in sorted(flat['t'].unique())}
    R['rung']['survey_kg_rows_from_sellers'] = {
        w: round(float(seller[(flat['t'] == w).to_numpy() & f64(flat['Quantity_kg']).gt(0).to_numpy()].mean()), 3)
        if ((flat['t'] == w).to_numpy() & f64(flat['Quantity_kg']).gt(0).to_numpy()).any() else None
        for w in sorted(flat['t'].unique())}
    # answer key 1: stated-mass sold units held out of the baseline
    stated = pd.Series(Q._stated_masses(flat['Unit_sold'].to_numpy(dtype=object)))
    labels = sorted({u for u, m in zip(flat['Unit_sold'], stated) if u is not None and np.isfinite(m)
                     and m not in (1.0, 0.001)})
    R['rung']['answer_key_holdout'] = holdout(flat, labels)
    # answer key 2: the survey's own kilograms (the screen's agreement, all judged rows)
    R['rung']['answer_key_survey_kg'] = {w: v['agreement'] for w, v in R['firing'].items()}
    # answer key 3 (Mali EAC-I): the reported kg per SOLD unit, KgFactor_sold
    if 'KgFactor_sold' in flat.columns:
        kfs = f64(flat['KgFactor_sold'])
        m = kfs.gt(0).to_numpy() & f64(flat['Quantity_sold']).gt(0).to_numpy()
        s = flat.copy()
        s['Quantity_kg'] = np.nan
        s.loc[m, 'u'] = s.loc[m, 'Unit_sold']
        s.loc[m, 'Quantity'] = 1.0
        s.loc[m, 'Quantity_kg'] = 1.0
        sf = factors(s)
        r = kfs / sf['kg_price_implied']
        ok = m & sf['kg_price_implied'].notna().to_numpy() & sf['kg_survey'].notna().to_numpy()
        R['rung']['answer_key_kgfactor_sold'] = {
            w: {'sales_with_factor': int(((flat['t'] == w).to_numpy() & m).sum()),
                'massless_with_factor': int(((flat['t'] == w).to_numpy() & m
                                             & sf['kg_survey'].notna().to_numpy()).sum()),
                'inferred': int(((flat['t'] == w).to_numpy() & ok).sum()),
                'reported_over_implied': agree(r[(flat['t'] == w).to_numpy() & ok]),
                'by_unit': {str(u): agree(g) for u, g in
                            r[(flat['t'] == w).to_numpy() & ok].groupby(
                                flat.loc[(flat['t'] == w).to_numpy() & ok, 'Unit_sold']) if len(g) >= 10}}
            for w in sorted(flat['t'].unique())}
    # caveat: state -- price per kg of Kg sales by sold state, per wave and crop
    st = {}
    S = flat[f64(flat['Quantity_sold']).gt(0) & f64(flat['Value_sold']).gt(0)]
    S = S[S['Unit_sold'].astype(str).str.lower().eq('kg')]
    if 'Condition_sold' in S.columns:
        p = f64(S['Value_sold']) / f64(S['Quantity_sold'])
        for (w, c), g in pd.DataFrame({'t': S['t'], 'c': S['crop'], 's': S['Condition_sold'],
                                       'p': p}).dropna().groupby(['t', 'c']):
            m = g.groupby('s')['p'].agg(['median', 'size'])
            if {'shelled', 'unshelled'} <= set(m.index) and m['size'].min() >= 5:
                st[f'{w} {c}'] = {'shelled_over_unshelled': round(float(m.loc['shelled', 'median']
                                                                         / m.loc['unshelled', 'median']), 3),
                                  'n': [int(m.loc['shelled', 'size']), int(m.loc['unshelled', 'size'])]}
    R['rung']['state_kg_price_ratio'] = st
    # which waves record a sale state and a harvest state at all
    R['rung']['state_recorded'] = {
        w: {'sales_with_state': int(((flat['t'] == w) & flat.get('Condition_sold', pd.Series(None, index=flat.index)).notna()
                                     & f64(flat['Quantity_sold']).gt(0)).sum()),
            'rows_with_harvest_state': int(((flat['t'] == w) & flat['condition'].notna()
                                            & ~flat['condition'].astype(str).str.startswith('unknown')).sum())
            if 'condition' in flat.columns else 0}
        for w in sorted(flat['t'].unique())}
    report[country] = R
    print(country, 'done', flush=True)

out_json.parent.mkdir(parents=True, exist_ok=True)
out_json.write_text(json.dumps(report, indent=1, default=str, ensure_ascii=False))
print('wrote', out_json)
