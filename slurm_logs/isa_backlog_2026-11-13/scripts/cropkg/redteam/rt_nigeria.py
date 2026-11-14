"""Red team GH #1559: what the candidate's screen would name on the Nigeria candidate's
crop_production frame (built by the Nigeria code).  Candidate env.

Usage: rt_nigeria.py FRAME_PKL OUT_JSON
"""
import json
import os
import sys
import time
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rtlib  # noqa: E402
from lsms_library import quantity_audit as Q  # noqa: E402

df = pd.read_pickle(sys.argv[1])
out = Path(sys.argv[2])
R = {'rows': len(df), 'index': list(df.index.names), 'has_cols': {c: c in df.columns for c in
     ('Quantity', 'Quantity_kg', 'Quantity_sold', 'Value_sold', 'Unit_sold', 'Condition_sold', 'KgFactor')}}
f = rtlib.flat(df)
R['rows_by_t'] = dict(Counter(f['t']))
R['qkg_rows_by_t'] = {t: int(((f['t'] == t) & (f['kg'] > 0)).sum()) for t in sorted(set(f['t']))}
R['sales_by_t'] = {t: int(((f['t'] == t) & (f['qs'] > 0) & (f['vs'] > 0)).sum()) for t in sorted(set(f['t']))}
t0 = time.perf_counter()
cand = Q.survey_kg_price_factors(df)
R['factors_seconds'] = round(time.perf_counter() - t0, 3)
R['candidate_returned_none'] = cand is None
if cand is not None:
    t0 = time.perf_counter()
    reps = Q.audit_survey_kg_prices(df, country='Nigeria')
    R['audit_seconds'] = round(time.perf_counter() - t0, 3)
    R['reports'] = [{k: r[k] for k in ('wave', 'direction', 'rows', 'n_judged', 'n_implausible', 'max_ratio')}
                    for r in reps]
    fl = cand['kg_price_flag'].notna().to_numpy()
    judged = (cand['kg_price_implied'].notna() & cand['kg_survey'].notna()).to_numpy()
    rat = cand['survey_over_implied'].to_numpy(dtype='float64')
    per_t = {}
    for t in sorted(set(f['t'])):
        m = (f['t'] == t).to_numpy()
        jm = judged & m
        r = rat[jm]
        per_t[t] = {'qkg_rows': int(((f['kg'] > 0).to_numpy() & m).sum()),
                    'massless_qkg_rows': int((cand['kg_survey'].notna().to_numpy() & m).sum()),
                    'judged': int(jm.sum()), 'named': int((fl & m).sum()),
                    'median': float(np.median(r)) if len(r) else None,
                    'p5': float(np.quantile(r, .05)) if len(r) else None,
                    'p95': float(np.quantile(r, .95)) if len(r) else None,
                    'within2x': float(((r >= .5) & (r <= 2)).mean()) if len(r) else None}
    R['per_t'] = per_t
    # by cell: is the firing all-or-nothing within a (t, crop, u) cell (a table factor)?
    g = pd.DataFrame({'t': f['t'].to_numpy(), 'j': f['j'].to_numpy(), 'u': f['u_raw'].astype(str).to_numpy(),
                      'us': f['us_raw'].astype(str).to_numpy(), 'judged': judged, 'named': fl,
                      'kgpu': cand['kg_survey'].to_numpy(dtype='float64'),
                      'impl': cand['kg_price_implied'].to_numpy(dtype='float64'),
                      'rung': cand['kg_price_rung'].astype(str).to_numpy()})
    # Why nothing is judged, per t: survey-kg rows in a mass-less unit, and whether any
    # sale in the wave is in that unit (sold-unit vocabulary against the harvest one).
    massless = cand['kg_survey'].notna().to_numpy()
    sale = ((f['qs'] > 0) & (f['vs'] > 0)).to_numpy()
    why = {}
    for t in sorted(set(f['t'])):
        m = (f['t'] == t).to_numpy()
        if not (massless & m).any():
            continue
        hu = Counter(g.loc[massless & m, 'u'])
        su = Counter(g.loc[sale & m, 'us'])
        why[t] = {'massless_qkg_units': hu.most_common(12), 'sold_units': su.most_common(15),
                  'sales': int((sale & m).sum()),
                  'massless_units_with_a_sale_in_unit': sorted(u for u in hu if u in su),
                  'kg_sales_by_crop_top': Counter(g.loc[sale & m & g['us'].str.lower().isin(
                      ['kg', 'kilogram', 'kilogramme']).to_numpy(), 'j']).most_common(10)}
    R['why'] = why
    R['qkg_units_all'] = {t: Counter(g.loc[(f['kg'] > 0).to_numpy() & (f['t'] == t).to_numpy(), 'u']).most_common(15)
                          for t in sorted(set(f['t'])) if ((f['kg'] > 0) & (f['t'] == t)).any()}
    cells = []
    for (t, j, u), gg in g[g['judged']].groupby(['t', 'j', 'u']):
        cells.append({'t': t, 'j': j, 'u': u, 'judged': int(len(gg)), 'named': int(gg['named'].sum()),
                      'kg_per_unit_distinct': int(gg['kgpu'].round(6).nunique()),
                      'kg_per_unit_median': float(gg['kgpu'].median()), 'implied': float(gg['impl'].median()),
                      'ratio_median': float((gg['kgpu'] / gg['impl']).median())})
    R['judged_cells'] = len(cells)
    R['named_cells'] = sorted([c for c in cells if c['named']], key=lambda c: -c['named'])[:30]
    R['cells_partly_named'] = sum(1 for c in cells if 0 < c['named'] < c['judged'])
    R['units_judged'] = dict(Counter(g.loc[g['judged'], 'u']).most_common(20))
    mine = rtlib.estimate(f=f)
    ci = cand['kg_price_implied'].to_numpy(dtype='float64')
    mi = mine['implied'].to_numpy(dtype='float64')
    R['independent_estimator'] = {
        'implied_disagree': int((~(np.isclose(ci, mi, rtol=1e-9) | (np.isnan(ci) & np.isnan(mi)))).sum()),
        'flags_cand': int(fl.sum()), 'flags_mine': int(mine['flag'].notna().sum()),
        'summary_mine': rtlib.summarise(f, mine)}
    # frame-level strict, per t: full Site Q on the slice, and distributional-only.
    strict = {}
    os.environ['LSMS_QUANTITY_STRICT'] = '1'
    try:
        for t in sorted(set(f['t'])):
            sl = df[(f['t'] == t).to_numpy()]
            row = {}
            for tag, fn in (('full_site_q', lambda d: Q.check_quantities(d, country='Nigeria', table='crop_production')),
                            ('price_only', lambda d: [Q._record_quantity_report(r) for r in
                                                      Q.audit_survey_kg_prices(d, country='Nigeria')])):
                Q._clear_quantity_reports()
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        fn(sl)
                    row[tag] = None
                except Exception as e:  # noqa: BLE001
                    row[tag] = type(e).__name__
            strict[t] = row
    finally:
        os.environ.pop('LSMS_QUANTITY_STRICT', None)
    R['frame_strict'] = strict
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps({k: R[k] for k in R if k not in ('named_cells',)}, default=str)[:6000])
