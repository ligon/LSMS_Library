"""Red team GH #1559: the candidate's estimator against an independent one, variants,
Farm-level / case-folding / 3E, fault injection, cost, Feature.  Candidate env, warm.

Usage: rt_analysis.py FRAMES_DIR OUT_JSON
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

import lsms_library as ll  # noqa: E402
from lsms_library import quantity_audit as Q  # noqa: E402

frames, out_json = Path(sys.argv[1]), Path(sys.argv[2])
R = {}


def load(c):
    return pd.read_pickle(frames / f'{c}_crop_production.pkl')


def cmp_rows(cand, mine):
    """Row-by-row agreement; aggregates only."""
    ci = cand['kg_price_implied'].to_numpy(dtype='float64')
    mi = mine['implied'].to_numpy(dtype='float64')
    both_nan = np.isnan(ci) & np.isnan(mi)
    close = np.isclose(ci, mi, rtol=1e-9, atol=0) | both_nan
    cr = cand['kg_price_rung'].astype(object).where(cand['kg_price_rung'].notna(), None).to_numpy()
    mr = mine['rung'].to_numpy()
    rung_eq = np.array([a == b for a, b in zip(cr, mr)])
    cf = cand['kg_price_flag'].astype(object).where(cand['kg_price_flag'].notna(), None).to_numpy()
    mf = mine['flag'].to_numpy()
    flag_eq = np.array([a == b for a, b in zip(cf, mf)])
    nu = np.isclose(cand['kg_price_n_unit'].to_numpy(dtype='float64'), mine['n_unit'].to_numpy(dtype='float64'),
                    equal_nan=True)
    nk = np.isclose(cand['kg_price_n_kg'].to_numpy(dtype='float64'), mine['n_kg'].to_numpy(dtype='float64'),
                    equal_nan=True)
    ks = np.isclose(cand['kg_survey'].to_numpy(dtype='float64'), mine['kg_survey'].to_numpy(dtype='float64'),
                    equal_nan=True)
    return {'rows': len(ci), 'implied_disagree': int((~close).sum()), 'rung_disagree': int((~rung_eq).sum()),
            'flag_disagree': int((~flag_eq).sum()), 'n_unit_disagree': int((~nu).sum()),
            'n_kg_disagree': int((~nk).sum()), 'kg_survey_disagree': int((~ks).sum()),
            'cand_implied_rows': int(np.isfinite(ci).sum()), 'mine_implied_rows': int(np.isfinite(mi).sum()),
            'cand_flags': dict(Counter(x for x in cf if x is not None)),
            'mine_flags': dict(Counter(x for x in mf if x is not None))}


def label_maps(df):
    """Unit labels on which the candidate's mass / usability reading differs from mine."""
    labels = set()
    for name in ('u', 'Unit_sold'):
        vals = (df.index.get_level_values(name) if name in df.index.names
                else (df[name] if name in df.columns else []))
        labels |= {str(x) for x in pd.unique(pd.Series(vals).dropna())}
    labels = sorted(labels)
    cm = Q._stated_masses(np.array(labels, dtype=object))
    cu = Q._usable_units(np.array(labels, dtype=object))
    diff = []
    for lab, a, b in zip(labels, cm, cu):
        mm, mu = rtlib.my_mass(lab), rtlib.my_usable(lab)
        if not ((np.isnan(a) and np.isnan(mm)) or np.isclose(a, mm)) or bool(b) != bool(mu):
            diff.append({'label': lab, 'cand_mass': None if np.isnan(a) else float(a),
                         'mine_mass': None if np.isnan(mm) else float(mm),
                         'cand_usable': bool(b), 'mine_usable': bool(mu)})
    massed = [(lab, float(a)) for lab, a in zip(labels, cm) if np.isfinite(a)]
    unusable = [lab for lab, b in zip(labels, cu) if not b]
    return {'n_labels': len(labels), 'disagreements': diff, 'stated_mass_labels': massed,
            'unusable_labels': unusable}


for country in ('Niger', 'Mali'):
    df = load(country)
    rec = {}
    f = rtlib.flat(df)
    cand = Q.survey_kg_price_factors(df)
    rec['candidate_returned_none'] = cand is None
    rec['label_maps'] = label_maps(df)
    mine = rtlib.estimate(f=f)
    if cand is not None:
        rec['row_compare_default'] = cmp_rows(cand, mine)
        # use the candidate's own label readings, to isolate the arithmetic
        lab_m = dict(zip(*[np.array(sorted(set(f['u_raw'].dropna()) | set(f['us_raw'].dropna())), dtype=object)] * 2))
        labs = np.array(list(lab_m), dtype=object)
        cm = dict(zip(labs, Q._stated_masses(labs)))
        cu = dict(zip(labs, Q._usable_units(labs)))
        mine_c = rtlib.estimate(f=f, mass=lambda x: cm.get(x, np.nan) if isinstance(x, str) else np.nan,
                                usable=lambda x: bool(cu.get(x, False)) if isinstance(x, str) else False)
        rec['row_compare_cand_labels'] = cmp_rows(cand, mine_c)
    rec['summary_default'] = rtlib.summarise(f, mine)

    # Case-folding: what changes if labels are not folded.
    nf = rtlib.flat(df, fold=False)
    mine_nf = rtlib.estimate(f=nf)
    rec['casefold'] = {
        'judged_folded': int(mine['implied'].notna().sum()),
        'judged_unfolded': int(mine_nf['implied'].notna().sum()),
        'flags_folded': int(mine['flag'].notna().sum()), 'flags_unfolded': int(mine_nf['flag'].notna().sum()),
        'distinct_raw_labels': int(len(set(f['u_raw'].dropna()) | set(f['us_raw'].dropna()))),
        'distinct_folded_labels': int(len(set(f['u'].dropna()) | set(f['us'].dropna()))),
        'labels_merged_by_fold': sorted({str(a) for a in (set(f['u_raw'].dropna()) | set(f['us_raw'].dropna()))
                                         if sum(1 for b in (set(f['u_raw'].dropna()) | set(f['us_raw'].dropna()))
                                                if str(b).strip().casefold() == str(a).strip().casefold()) > 1}),
    }
    # Farm-level: judged rows and donors per wave.
    fl = (f['plot_id'] == 'Farm-level')
    sale = (f['qs'] > 0) & (f['vs'] > 0)
    rec['farm_level'] = {w: {'rows': int((fl & (f['t'] == w)).sum()),
                             'judged': int((fl & (f['t'] == w) & mine['implied'].notna()
                                            & mine['kg_survey'].notna()).sum()),
                             'with_quantity_kg': int((fl & (f['t'] == w) & (f['kg'] > 0)).sum()),
                             'with_quantity': int((fl & (f['t'] == w) & (f['q'] > 0)).sum()),
                             'donor_sales': int((fl & (f['t'] == w) & sale).sum()),
                             'named': int((fl & (f['t'] == w) & mine['flag'].notna()).sum())}
                         for w in sorted(set(f['t']))}

    # Variants.
    variants = {'state_blind': dict(state=False), 'baseline_only_gate': dict(gate_both=False)}
    if country == 'Niger':
        e3 = ((f['t'] == '2011-12') & fl).to_numpy()
        variants.update({'3E_out_donor': dict(donor=~e3), '3E_out_recip': dict(recip=~e3),
                         '3E_out_both': dict(donor=~e3, recip=~e3)})
    base_named = set(np.flatnonzero(mine['flag'].notna().to_numpy()))
    rec['variants'] = {}
    for name, kw in variants.items():
        v = rtlib.estimate(f=f, **kw)
        vn = set(np.flatnonzero(v['flag'].notna().to_numpy()))
        rec['variants'][name] = {'summary': rtlib.summarise(f, v), 'named': len(vn),
                                 'only_variant': len(vn - base_named), 'only_default': len(base_named - vn)}
        if name == '3E_out_both':
            # rainy rows judged with 3E out but not with it in: which gate refused them?
            lost = (v['implied'].notna() & mine['implied'].isna()).to_numpy() & ~e3
            gained = (v['implied'].isna() & mine['implied'].notna()).to_numpy() & ~e3
            rec['variants'][name]['rainy_lost_with_3E_in'] = int(lost.sum())
            rec['variants'][name]['rainy_gained_with_3E_in'] = int(gained.sum())
            cells = Counter(zip(f.loc[lost, 't'], f.loc[lost, 'j'], f.loc[lost, 'u']))
            # gate diagnosis on each lost cell, with 3E in
            sale_ok = (f['qs'] > 0) & (f['vs'] > 0) & f['us_raw'].map(rtlib.my_usable).astype(bool)
            ms = f['us_raw'].map(rtlib.my_mass)
            fx = f.assign(price=f['vs'] / f['qs'])
            fx['pkg'] = fx['price'] / ms
            diag = []
            for (t, j, u), nrows in cells.most_common():
                d = {'t': t, 'j': j, 'u': u, 'rows': nrows}
                for tag, don in (('3E_in', np.ones(len(f), bool)), ('3E_out', ~e3)):
                    s = sale_ok & don & (fx['t'] == t) & (fx['j'] == j)
                    mk = fx.loc[s & ms.notna(), 'pkg']
                    pu = fx.loc[s & ms.isna() & (fx['us'] == u), 'price']
                    def spr(x):
                        if len(x) == 0:
                            return None
                        return float(x.quantile(.9) / x.quantile(.1)) if len(x) >= 10 else float(x.max() / x.min())
                    d[tag] = {'n_kg': int(len(mk)), 'spread_kg': spr(mk), 'n_unit': int(len(pu)),
                              'spread_unit': spr(pu), 'n_3E_unit': int((s & ms.isna() & (fx['us'] == u) & e3).sum()),
                              'n_3E_kg': int((s & ms.notna() & e3).sum())}
                diag.append(d)
            rec['variants'][name]['lost_cells'] = diag
    R[country] = rec
    print(country, json.dumps({k: rec[k] for k in rec if k in ('row_compare_default', 'row_compare_cand_labels',
                                                                'summary_default', 'casefold', 'farm_level')},
                              default=str)[:4000], flush=True)

# --- cost, a spy inside a warm read -------------------------------------------
def timed_read(country, n=5):
    ts = []
    for _ in range(n):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            t0 = time.perf_counter()
            ll.Country(country).crop_production()
            ts.append(time.perf_counter() - t0)
    return min(ts)


cost = {}
real = Q.audit_survey_kg_prices
for country in ('Niger', 'Mali'):
    spent = []

    def spy(df, **kw):
        t0 = time.perf_counter()
        try:
            return real(df, **kw)
        finally:
            spent.append(time.perf_counter() - t0)
    Q.audit_survey_kg_prices = spy
    with_screen = timed_read(country)
    Q.audit_survey_kg_prices = lambda df, **kw: []
    without = timed_read(country)
    Q.audit_survey_kg_prices = real
    df = load(country)
    direct = []
    for _ in range(5):
        t0 = time.perf_counter()
        real(df, country=country)
        direct.append(time.perf_counter() - t0)
    cost[country] = {'warm_read_with_s': round(with_screen, 4), 'warm_read_noop_screen_s': round(without, 4),
                     'spy_min_s': round(min(spent), 4) if spent else None,
                     'direct_min_s': round(min(direct), 4), 'rows': len(df)}
R['cost'] = cost
print('cost', cost, flush=True)

# --- fault injection --------------------------------------------------------
faults = {}
for which in ('price_screen_raises', 'distributional_raises'):
    Q._clear_quantity_reports()
    saved = (Q.audit_survey_kg_prices, Q.audit_quantities)

    def boom(*a, **k):
        raise ValueError('injected')
    if which == 'price_screen_raises':
        Q.audit_survey_kg_prices = boom
    else:
        Q.audit_quantities = boom
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            got = ll.Country('Niger').crop_production()
    finally:
        Q.audit_survey_kg_prices, Q.audit_quantities = saved
    reps = Q.quantity_reports(country='Niger', table='crop_production')
    ref = load('Niger')
    try:
        pd.testing.assert_frame_equal(got, ref, check_exact=True)
        eq = True
    except AssertionError as e:
        eq = str(e)[:200]
    faults[which] = {'frame_equal': eq,
                     'could_not_run': [w.category.__name__ + ': ' + str(w.message)[:120]
                                       for w in caught if 'could not run' in str(w.message)],
                     'reports': sorted(Counter((r.get('site') or 'distributional', r.get('column'), str(r.get('wave')))
                                               for r in reps).items(), key=str)}
R['faults'] = faults
print('faults', json.dumps(faults, default=str), flush=True)

# --- Feature, default and strict ------------------------------------------------
feat = {}
for strict in (False, True):
    if strict:
        os.environ['LSMS_QUANTITY_STRICT'] = '1'
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                fr = ll.Feature('crop_production')(['Niger', 'Mali'])
                got = {'rows': len(fr), 'countries': sorted(set(fr.index.get_level_values('country')))}
            except Exception as e:  # noqa: BLE001
                got = {'raised': f'{type(e).__name__}: {str(e)[:200]}'}
        got['warning_categories'] = dict(Counter(w.category.__name__ for w in caught))
        got['dropped_msgs'] = [str(w.message)[:200] for w in caught
                               if 'drop' in str(w.message).lower() and 'Niger' in str(w.message)][:3]
        feat['strict' if strict else 'default'] = got
    finally:
        os.environ.pop('LSMS_QUANTITY_STRICT', None)
R['feature'] = feat
print('feature', json.dumps(feat, default=str)[:2000], flush=True)

out_json.write_text(json.dumps(R, indent=1, default=str))
print('done', flush=True)
