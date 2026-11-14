"""GH #1559 follow-up checks for the rung memo (advisor review).  Aggregates only.

Usage: analysis2.py FRAMES_DIR OUT_JSON, in the candidate clone.
  (a) Niger 2018-19: the implied masses the rung would serve, per (crop, unit), with both
      sale counts -- is a big sack lighter than a medium one WITHIN a crop?
  (b) Mali 2017-18: crop mix, kg and concentration of the served `Sac` rows, and their
      overlap with Site Q's `Quantity` names.
  (c) both answer keys (held-out stated sacks, KgFactor_sold) by crop and without cotton.
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from lsms_library import quantity_audit as Q
from lsms_library.transformations import harvest_kg_factors

frames, out_json = Path(sys.argv[1]), Path(sys.argv[2])
warnings.simplefilter('ignore')


def f64(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def agree(r):
    r = pd.Series(r, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    r = r[r > 0]
    if r.empty:
        return {'n': 0}
    lr = np.abs(np.log(r))
    return {'n': int(r.size), 'median': round(float(r.median()), 4),
            'p10': round(float(r.quantile(.1)), 4), 'p90': round(float(r.quantile(.9)), 4),
            'within_1.25x': round(float((lr <= np.log(1.25)).mean()), 4),
            'within_2x': round(float((lr <= np.log(2)).mean()), 4)}


def flat_of(df):
    flat = df.reset_index()
    for c in ('u', 'Unit_sold', 'crop', 'condition', 'Condition_sold', 't', 'plot_id', 'i'):
        if c in flat.columns:
            flat[c] = flat[c].astype(object).where(flat[c].notna(), None)
    return flat


def factors(flat):
    return Q.survey_kg_price_factors(flat).reset_index(drop=True)


def sim_served(flat, src):
    qty = f64(flat['Quantity'])
    fac = factors(flat.assign(Quantity_kg=np.where(qty > 0, qty, np.nan)))
    served = ((src == 'none') & (qty > 0).to_numpy() & fac['kg_survey'].notna().to_numpy()
              & fac['kg_price_implied'].notna().to_numpy())
    return fac, served, qty


def holdout_by_crop(flat, label):
    stated = Q._stated_masses(flat['Unit_sold'].to_numpy(dtype=object))
    m = (flat['Unit_sold'] == label).to_numpy()
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
    r = fac['kg_price_implied'] / mass
    crop = flat['crop']
    return {'all': agree(r[m]),
            'without_cotton': agree(r[m & (crop != 'Coton').to_numpy()]),
            'cotton_rows': int((m & (crop == 'Coton').to_numpy() & r.notna().to_numpy()).sum()),
            'by_crop': {str(c): agree(g) for c, g in r[m].groupby(crop[m]) if g.notna().sum() >= 10}}


report = {}
for country in ('Niger', 'Mali'):
    df = pd.read_pickle(frames / f'{country}_crop_production.pkl')
    flat = flat_of(df)
    hk = harvest_kg_factors(df)
    src = hk['KgFactorSource'].to_numpy(dtype=object)
    fac, served, qty = sim_served(flat, src)
    R = {}
    # (a)/(b): per (wave, crop, unit) of the served rows
    cells = []
    g = pd.DataFrame({'t': flat['t'], 'crop': flat['crop'], 'u': flat['u'],
                      'implied': fac['kg_price_implied'], 'n_unit': fac['kg_price_n_unit'],
                      'n_kg': fac['kg_price_n_kg'], 'rung': fac['kg_price_rung'],
                      'kg': qty * fac['kg_price_implied']})[served]
    for k, x in g.groupby(['t', 'crop', 'u', 'rung'], dropna=False):
        cells.append({'wave': k[0], 'crop': k[1], 'u': k[2], 'rung': k[3], 'rows': int(len(x)),
                      'implied_kg_per_unit': round(float(x['implied'].median()), 3),
                      'n_unit_sales': int(x['n_unit'].median()), 'n_kg_sales': int(x['n_kg'].median()),
                      'kg_added': round(float(x['kg'].sum()), 1)})
    R['served_cells'] = sorted(cells, key=lambda d: (d['wave'], -d['rows']))
    if country == 'Niger':
        w = g[g['t'] == '2018-19']
        order = {}
        for c, x in w.groupby('crop'):
            med = x.groupby('u')['implied'].median()
            if {'Gros sac', 'Sac Moyen'} <= set(med.index):
                order[str(c)] = {'Gros sac': round(float(med['Gros sac']), 3),
                                 'Sac Moyen': round(float(med['Sac Moyen']), 3)}
        R['niger_2018_19_gros_vs_moyen_within_crop'] = order
    if country == 'Mali':
        w = g[(g['t'] == '2017-18')]
        tot = float(w['kg'].sum())
        s = w.sort_values('kg', ascending=False)['kg']
        R['mali_2017_18'] = {
            'served_rows': int(len(w)), 'kg_added': round(tot, 1),
            'top10_share': round(float(s.head(10).sum() / tot), 4),
            'top1pct_share': round(float(s.head(max(1, len(s) // 100)).sum() / tot), 4),
            'sac_by_crop': {str(c): {'rows': int(len(x)), 'kg': round(float(x['kg'].sum()), 1),
                                     'implied': round(float(x['implied'].median()), 1)}
                            for c, x in w[w['u'] == 'Sac'].groupby('crop')},
            'quantity_q': {f'p{p}': round(float(qty[served & (flat['t'] == '2017-18').to_numpy()].quantile(p / 100)), 2)
                           for p in (50, 90, 99)}}
        # overlap with Site Q's names on Quantity (the generic (t,u,j) ladder)
        rep = Q.audit_quantities(df, 'Quantity', country=country, table='crop_production',
                                 tails=('high',))
        named = {(o['i'], o['plot'], o['j'], o['value']) for r in rep if r['wave'] == '2017-18'
                 for o in r['offenders']}
        sm = served & (flat['t'] == '2017-18').to_numpy()
        rows = pd.DataFrame({'i': flat.loc[sm, 'i'].astype(str), 'plot': flat.loc[sm, 'plot_id'].astype(str),
                             'j': flat.loc[sm, 'crop'].astype(str), 'q': qty[sm],
                             'kg': (qty * fac['kg_price_implied'])[sm]})
        tag = pd.Series([(a, b, c, d) in {(str(x[0]), str(x[1]), str(x[2]), x[3]) for x in named}
                         for a, b, c, d in zip(rows['i'], rows['plot'], rows['j'], rows['q'])],
                        index=rows.index)
        R['mali_2017_18']['site_q_quantity_names'] = {
            'named_in_wave': len(named), 'served_by_rung': int(tag.sum()),
            'kg_of_those': round(float(rows.loc[tag, 'kg'].sum()), 1)}
        # (c) KgFactor_sold by crop and without cotton
        kfs = f64(flat['KgFactor_sold'])
        m = kfs.gt(0).to_numpy() & f64(flat['Quantity_sold']).gt(0).to_numpy()
        s2 = flat.copy()
        s2['Quantity_kg'] = np.nan
        s2.loc[m, 'u'] = s2.loc[m, 'Unit_sold']
        s2.loc[m, 'Quantity'] = 1.0
        s2.loc[m, 'Quantity_kg'] = 1.0
        sf = factors(s2)
        r = kfs / sf['kg_price_implied']
        ok = m & sf['kg_price_implied'].notna().to_numpy() & sf['kg_survey'].notna().to_numpy()
        crop = flat['crop']
        R['kgfactor_sold'] = {
            'all': agree(r[ok]), 'without_cotton': agree(r[ok & (crop != 'Coton').to_numpy()]),
            'cotton': int((ok & (crop == 'Coton').to_numpy()).sum()),
            'by_crop': {str(c): agree(x) for c, x in r[ok].groupby(crop[ok]) if len(x) >= 10}}
    R['holdout'] = {}
    stated = Q._stated_masses(flat['Unit_sold'].to_numpy(dtype=object))
    for label in sorted({u for u, mm in zip(flat['Unit_sold'], stated)
                         if u is not None and np.isfinite(mm) and mm not in (1.0, 0.001)}):
        if (flat['Unit_sold'] == label).sum() >= 5:
            R['holdout'][label] = holdout_by_crop(flat, label)
    report[country] = R
    print(country, 'done', flush=True)
out_json.write_text(json.dumps(report, indent=1, default=str, ensure_ascii=False))
print('wrote', out_json)
