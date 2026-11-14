"""Red team GH #1559: re-measure the rung memo's key numbers independently.

Usage: rt_memo.py FRAMES_DIR OUT_JSON   (candidate env, warm private root)
Aggregates only in OUT_JSON.
"""
import json
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rtlib  # noqa: E402

from lsms_library import paths, transformations as tr  # noqa: E402
from lsms_library import quantity_audit as Q  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402

frames, out_json = Path(sys.argv[1]), Path(sys.argv[2])
warnings.simplefilter('ignore')
R = {}


def q(x, p):
    return float(np.nanquantile(x, p)) if len(x) else None


for country in ('Niger', 'Mali'):
    df = pd.read_pickle(frames / f'{country}_crop_production.pkl')
    f = rtlib.flat(df)
    rec = {}
    kf = tr.harvest_kg_factors(df)
    src = kf['KgFactorSource'].astype(str).to_numpy()
    kpu = pd.to_numeric(kf['kg_per_unit'], errors='coerce').to_numpy(dtype='float64')
    served = np.where(src == 'survey_kg', f['kg'].to_numpy(), f['q'].to_numpy() * kpu)
    served = np.where(np.isfinite(served), served, 0.0)
    hk = tr.harvest_kg(df)
    col = hk.columns[0]
    hkt = hk.reset_index().groupby('t')[col].sum()
    est = rtlib.estimate(f=f, judge_all=True)            # implied for every mass-less row
    none_q = (src == 'none') & (f['q'] > 0).to_numpy()
    massless = none_q & est['judged_any'].to_numpy()
    servable = massless & est['implied'].notna().to_numpy()
    add = np.where(servable, f['q'].to_numpy() * est['implied'].to_numpy(), 0.0)
    rep = json.load(open(frames / f'{country}_reports.json'))
    q_off = {(str(r['wave']), o.get('i'), o.get('plot'), o.get('j'), o.get('u'))
             for r in rep if r.get('column') == 'Quantity' and r.get('site') != 'survey-kg-price'
             for o in r['offenders']}
    d = df.reset_index()
    jcol = 'j' if 'j' in d.columns else 'crop'
    ukey = d['u'].astype(str)
    rec['by_wave'] = {}
    for w in sorted(set(f['t'])):
        m = (f['t'] == w).to_numpy()
        a = add[m & servable]
        top = np.sort(a)[::-1]
        k = max(1, int(round(0.01 * len(top))))
        pos = np.flatnonzero(m & servable)
        top_pos = pos[np.argsort(add[pos])[::-1][:1]] if len(pos) else []
        top_is_q_off = any((w, str(d.loc[p, 'i']), str(d.loc[p, 'plot_id']), str(d.loc[p, jcol]), ukey[p]) in q_off
                           for p in top_pos)
        n_q_off_served = sum(((w, str(d.loc[p, 'i']), str(d.loc[p, 'plot_id']), str(d.loc[p, jcol]), ukey[p]) in q_off)
                             for p in pos)
        units = Counter(f.loc[m & servable, 'u_raw'].astype(str))
        rec['by_wave'][w] = {
            'rows': int(m.sum()), 'none_q_pos': int((m & none_q).sum()), 'none_massless': int((m & massless).sum()),
            'servable': int((m & servable).sum()), 'kg_added': float(a.sum()),
            'kg_served_now_rows': float(served[m].sum()), 'kg_served_now_harvest_kg': float(hkt.get(w, np.nan)),
            'top1pct_share': float(top[:k].sum() / a.sum()) if a.sum() else None,
            'top10_share': float(top[:10].sum() / a.sum()) if a.sum() else None,
            'max_row_kg': float(top[0]) if len(top) else None,
            'max_row_is_siteq_quantity_offender': bool(top_is_q_off),
            'siteq_quantity_offenders_served': int(n_q_off_served),
            'units_served': units.most_common(8),
            'rung_counts': dict(Counter(est.loc[m & servable, 'rung'].astype(str))),
        }
        # per (crop, unit) cells: support behind each served cell
        cells = []
        sel = m & servable
        g = pd.DataFrame({'j': f.loc[sel, 'j'], 'u': f.loc[sel, 'u'], 'add': add[sel], 'q': f.loc[sel, 'q'],
                          'impl': est.loc[sel, 'implied'], 'nu': est.loc[sel, 'n_unit'], 'nk': est.loc[sel, 'n_kg']})
        for (j, u), gg in sorted(g.groupby(['j', 'u']), key=lambda kv: -kv[1]['add'].sum())[:8]:
            cells.append({'j': j, 'u': u, 'rows': len(gg), 'kg_added': float(gg['add'].sum()),
                          'implied_median': float(gg['impl'].median()),
                          'n_unit_sales': int(gg['nu'].iloc[0]), 'n_kg_sales': int(gg['nk'].iloc[0])})
        rec['by_wave'][w]['top_cells'] = cells

    # Baseline support: sales by mass per (t, crop), and DISTINCT households behind them.
    sale = ((f['qs'] > 0) & (f['vs'] > 0) & f['us_raw'].map(rtlib.my_usable).astype(bool)).astype(bool)
    bym = sale & f['us_raw'].map(rtlib.my_mass).astype('float64').notna()
    bb = f[bym].groupby(['t', 'j']).agg(n=('i', 'size'), hh=('i', 'nunique'),
                                         kg_units=('us_raw', lambda s: dict(Counter(s.astype(str)))))
    rec['baseline_support'] = {f'{t}|{j}': {'n': int(r.n), 'households': int(r.hh), 'units': r.kg_units}
                               for (t, j), r in bb.iterrows() if r.n <= 12}

    # Comparator: kilograms per household-crop, served and served + rung, by wave.
    comp = {}
    tot = served + add
    hhc = pd.DataFrame({'t': f['t'], 'i': f['i'], 'j': f['j'], 'served': served, 'with_rung': tot})
    for (t, j), gg in hhc.groupby(['t', 'j']):
        if j not in ('mil', 'maïs', 'arachide', 'sorgho', 'niébé', 'coton', 'mil/sorgho', 'riz paddy'):
            continue
        per = gg.groupby('i')[['served', 'with_rung']].sum()
        comp[f'{t}|{j}'] = {'households': len(per),
                           'served_median_pos': float(per.loc[per.served > 0, 'served'].median()) if (per.served > 0).any() else None,
                           'served_share_pos': float((per.served > 0).mean()),
                           'rung_median_pos': float(per.loc[per.with_rung > 0, 'with_rung'].median()) if (per.with_rung > 0).any() else None,
                           'rung_share_pos': float((per.with_rung > 0).mean())}
    rec['per_household_crop'] = comp

    # Answer key (a): each stated-mass SOLD label held out of the baseline in turn.
    sale_rows = f[sale].copy()
    held = {}
    for lab in sorted({x for x in sale_rows['us_raw'].astype(str) if np.isfinite(rtlib.my_mass(x))}):
        stated = rtlib.my_mass(lab)
        if stated == 1.0:
            continue
        for st in (False, True):
            def mass_ho(x, lab=lab):
                return np.nan if str(x) == lab else rtlib.my_mass(x)
            g = f.copy()
            # recipients are the held-out label's own SALES: give them q=qs, kg=nan, u=Unit_sold
            r = g['us_raw'].astype(str).eq(lab) & sale
            g2 = g.copy()
            g2.loc[r, 'u_raw'] = g2.loc[r, 'us_raw']
            g2.loc[r, 'u'] = g2.loc[r, 'us']
            g2.loc[r, 'cond'] = g2.loc[r, 'cond_s']
            g2.loc[r, 'q'] = g2.loc[r, 'qs']
            e = rtlib.estimate(f=g2, state=st, mass=mass_ho, judge_all=True, recip=r.to_numpy())
            ratio = (e.loc[r.to_numpy(), 'implied'] / stated).dropna()
            byc = Counter()
            crop_med = {}
            sub = g2[r.to_numpy()].assign(ratio=e.loc[r.to_numpy(), 'implied'] / stated)
            for j, gg in sub.dropna(subset=['ratio']).groupby('j'):
                if len(gg) >= 10:
                    crop_med[j] = [float(gg['ratio'].median()), len(gg)]
            held[f'{lab}|state={st}'] = {'sales': int(r.sum()), 'inferred': int(len(ratio)),
                                         'median': float(ratio.median()) if len(ratio) else None,
                                         'p10': q(ratio, .1), 'p90': q(ratio, .9),
                                         'within2x': float(((ratio >= .5) & (ratio <= 2)).mean()) if len(ratio) else None,
                                         'by_crop': crop_med}
    rec['held_out_sacks'] = held

    # Answer key (c): KgFactor_sold against the price-implied mass per SOLD unit.
    if f['kgf_sold'].notna().any():
        r = (sale & (f['kgf_sold'] > 0) & f['us_raw'].map(rtlib.my_mass).astype('float64').isna()).to_numpy(dtype=bool)
        g2 = f.copy()
        g2.loc[r, 'u_raw'] = g2.loc[r, 'us_raw']
        g2.loc[r, 'u'] = g2.loc[r, 'us']
        g2.loc[r, 'q'] = g2.loc[r, 'qs']
        e = rtlib.estimate(f=g2, state=False, judge_all=True, recip=r)
        sub = g2[r].assign(ratio=(g2.loc[r, 'kgf_sold'] / e.loc[r, 'implied']).to_numpy())
        sub = sub.dropna(subset=['ratio'])
        kc = {}
        for w, gw in sub.groupby('t'):
            kc[w] = {'n': len(gw), 'median': float(gw['ratio'].median()), 'p10': q(gw['ratio'], .1),
                     'p90': q(gw['ratio'], .9), 'within2x': float(((gw['ratio'] >= .5) & (gw['ratio'] <= 2)).mean())}
        kc['by_crop'] = {j: [float(g['ratio'].median()), len(g)] for j, g in sub.groupby('j') if len(g) >= 10}
        kc['by_unit'] = {u: [float(g['ratio'].median()), len(g)] for u, g in sub.groupby('us_raw') if len(g) >= 10}
        nc = sub[sub['j'] != 'coton']
        kc['without_cotton'] = {'n': len(nc), 'median': float(nc['ratio'].median()) if len(nc) else None}
        kc['sales_with_factor'] = int((sale & (f['kgf_sold'] > 0)).sum())
        rec['kgfactor_sold_key'] = kc
    R[country] = rec
    print(country, json.dumps(rec['by_wave'], default=str)[:3000], flush=True)

# Mali EAC-I: does the HARVEST file carry the instrument's own kilogram equivalent?
raw = {}
root = Path(paths.countries_root()) / 'Mali'
for wave, fn, pre in (('2017-18', '2017-18/Data/eaci17_s7fp2.dta', 's7fq13'),
                      ('2014-15', '2014-15/Data/EACIS3A_p2.dta', 's3aq08')):
    try:
        h = get_dataframe(str(root / fn))
        cols = [c for c in h.columns if c.lower().startswith(pre)]
        rec = {'columns': cols, 'rows': len(h)}
        a = pd.to_numeric(h.get(pre + 'a'), errors='coerce')
        kgc = [c for c in cols if c.lower() in (pre + 'd', pre + 'c')]
        for c in kgc:
            v = h[c]
            num = pd.to_numeric(v, errors='coerce')
            rec[c] = {'dtype': str(v.dtype), 'n_pos': int((num > 0).sum()),
                      'n_q_pos': int((a > 0).sum()) if a is not None else None,
                      'sample_values': [str(x) for x in pd.unique(v.dropna())[:8]]}
        ucol = pre + ('c' if wave == '2017-18' else 'b')
        dcol = pre + ('d' if wave == '2017-18' else 'c')
        if ucol in h.columns and dcol in h.columns and a is not None:
            dd = pd.to_numeric(h[dcol], errors='coerce')
            ok = (a > 0) & (dd > 0)
            byu = {}
            for u, gg in pd.DataFrame({'u': h[ucol].astype(str), 'a': a, 'd': dd})[ok].groupby('u'):
                byu[u] = {'n': len(gg), 'd_median': float(gg['d'].median()),
                          'd_over_a_median': float((gg['d'] / gg['a']).median()),
                          'd_eq_a_share': float((gg['d'] == gg['a']).mean()),
                          'd_eq_1_share': float((gg['d'] == 1).mean()),
                          'sum_a_times_d': float((gg['a'] * gg['d']).sum()), 'sum_d': float(gg['d'].sum())}
            rec['by_unit'] = byu
            rec['totals'] = {'rows_q_and_d_pos': int(ok.sum()),
                             'sum_a_times_d_if_factor': float((a * dd)[ok].sum()),
                             'sum_d_if_total': float(dd[ok].sum())}
        raw[wave] = rec
    except Exception as e:  # noqa: BLE001
        raw[wave] = {'error': f'{type(e).__name__}: {str(e)[:300]}'}
R['mali_raw_harvest_kg_equivalent'] = raw
print('raw', json.dumps(raw, default=str)[:3000], flush=True)

out_json.write_text(json.dumps(R, indent=1, default=str))
print('done', flush=True)
