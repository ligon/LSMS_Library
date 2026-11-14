"""Each stream's headline numbers, computed on one checkout's pickled frames (task i).

usage: headlines.py FRAMES_ROOT SHA12 OUT.json
"""
import json
import pickle
import sys
from pathlib import Path

import pandas as pd

root, sha, out_path = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])


def load(country, label):
    p = root / sha / country / f'{label}.pkl'
    if not p.exists():
        return None
    with open(p, 'rb') as fh:
        return pickle.load(fh)


def summ(country):
    p = root / sha / country / 'summary.json'
    return json.loads(p.read_text()) if p.exists() else {}


def tcol(df):
    return pd.Index(df.index.get_level_values('t')).astype(str)


def keyed(df, key):
    if 'Derivation' not in df:
        return pd.Series(False, index=df.index)
    return df['Derivation'].astype('string').fillna('').str.split('+').map(lambda ks: key in ks)


def by_t(mask, df):
    return {str(k): int(v) for k, v in pd.Series(mask.to_numpy(), index=tcol(df)).groupby(level=0).sum().items()}


H = {}
safe = {}


def put(k, f):
    try:
        H[k] = f()
    except Exception as e:  # noqa: BLE001
        H[k] = f'ERR {type(e).__name__}: {e}'


# Nigeria
fq = load('Nigeria', 'food_quantities__units-kgs')
if fq is not None:
    def _kg():
        u = fq.index.get_level_values('u').astype(str)
        kg = fq['Quantity'][u == 'kg']
        return {'total_kg': round(float(kg.sum()), 1),
                'by_t': {k: round(float(v), 1) for k, v in kg.groupby(tcol(fq)[u == 'kg']).sum().items()},
                'rows': len(fq)}
    put('Nigeria food_quantities kgs', _kg)
for lab in ('shocks', 'assets', 'individual_education', 'crop_production', 'food_acquired', 'food_prices',
            'community_prices'):
    d = load('Nigeria', lab)
    if d is not None:
        put(f'Nigeria {lab} rows by t', lambda d=d: {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()})
put('Nigeria crop_production ledgers', lambda: summ('Nigeria')['tables']['crop_production']['ledgers'])

# Niger / Senegal / CIV / Togo / Burkina / GNB / Benin plot_inputs purchases
for c in ('Niger', 'Senegal', 'CotedIvoire', 'Togo', 'Burkina_Faso', 'Guinea-Bissau', 'Benin'):
    d = load(c, 'plot_inputs')
    if d is not None:
        def _pi(d=d):
            o = {'rows_by_t': {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()}}
            if 'Purchased' in d:
                o['Purchased_true_by_t'] = by_t(d['Purchased'].astype('boolean').fillna(False), d)
            if 'Received_as_gift' in d:
                o['Gift_true_by_t'] = by_t(d['Received_as_gift'].astype('boolean').fillna(False), d)
            if 'u' in d.index.names:
                o['u_Unknown_by_t'] = by_t(pd.Series(d.index.get_level_values('u').astype(str) == 'Unknown'), d)
                o['u_Manquant'] = int((d.index.get_level_values('u').astype(str) == 'Manquant').sum())
            return o
        put(f'{c} plot_inputs', _pi)

# crop_production rows + survey-kg-price reports
for c in ('Niger', 'Mali', 'Burkina_Faso', 'Malawi', 'Senegal', 'Guinea-Bissau', 'Benin'):
    d = load(c, 'crop_production')
    if d is not None:
        put(f'{c} crop_production rows by t', lambda d=d: {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()})
        put(f'{c} crop_production ledgers', lambda c=c: summ(c)['tables']['crop_production']['ledgers'])
        put(f'{c} crop_production warnings', lambda c=c: summ(c)['tables']['crop_production']['warnings'])
m = load('Malawi', 'crop_production')
if m is not None:
    put('Malawi crop_production cowpea key by t', lambda: by_t(keyed(m, 'Malawi::crop_production::cowpea-from-crop-text'), m))
m = load('Malawi', 'plot_inputs')
if m is not None:
    put('Malawi plot_inputs cowpea key by t', lambda: by_t(keyed(m, 'Malawi::plot_inputs::cowpea-from-crop-text'), m))
    put('Malawi plot_inputs six H columns present', lambda: sorted(
        c for c in ('ag_h17_os', 'ag_h26b_os', 'ag_h27_os', 'ag_h36b_os', 'ag_h26_1b', 'ag_h36_1b') if c in m))

# shocks everywhere
for c in ('Nigeria', 'Niger', 'Mali', 'Senegal', 'Togo', 'CotedIvoire', 'Guinea-Bissau', 'Burkina_Faso',
          'Uganda', 'Benin', 'Malawi'):
    d = load(c, 'shocks')
    if d is None:
        continue
    def _sh(c=c, d=d):
        o = {'rows_by_t': {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()}}
        for col in ('HowCoped0', 'HowCoped1', 'HowCoped2', 'HowCoped3', 'HowCopedSet'):
            if col in d:
                o[f'{col}_nonnull_by_t'] = by_t(d[col].notna(), d)
        o[f'code25_keyed_by_t'] = by_t(keyed(d, f'{c}::shocks::code25-rank-inferred'), d)
        if 'HowCoped0' in d:
            o['HowCoped0_did_nothing_by_t'] = by_t(d['HowCoped0'].astype('string').fillna('') == 'Did nothing', d)
        return o
    put(f'{c} shocks', _sh)
    put(f'{c} shocks warnings', lambda c=c: summ(c)['tables']['shocks']['warnings'])

# GNB food_acquired 2021-22
g = load('Guinea-Bissau', 'food_acquired')
if g is not None:
    def _g():
        s = g.index.get_level_values('s').astype(str)
        t = tcol(g)
        o = {'rows_by_t_s': {f'{a}|{b}': int(n) for (a, b), n in pd.Series(1, index=[t, s]).groupby(level=[0, 1]).sum().items()}}
        wk = keyed(g, 'Guinea-Bissau::food_acquired::s07bq03a-rounded-to-zero-withheld')
        o['withheld_by_s'] = {k: int(v) for k, v in pd.Series(wk.to_numpy(), index=s).groupby(level=0).sum().items() if v}
        o['withheld_quantity_nonnull'] = int(g.loc[wk.to_numpy(), 'Quantity'].notna().sum())
        rk = keyed(g, 'Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even')
        o['rounded_key_rows'] = int(rk.sum())
        return o
    put('Guinea-Bissau food_acquired', _g)
for lab in ('food_quantities__units-kgs', 'food_prices', 'household_characteristics', 'household_roster', 'sample',
            'assets', 'cluster_features'):
    d = load('Guinea-Bissau', lab)
    if d is not None:
        put(f'Guinea-Bissau {lab} rows by t', lambda d=d: {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()})
gs = load('Guinea-Bissau', 'sample')
if gs is not None:
    def _gs():
        key = 'Guinea-Bissau::sample::region-milieu-stratum'
        o = {}
        for w in ('2018-19', '2021-22'):
            s = gs[tcol(gs) == w]
            k = keyed(s, key)
            o[w] = {'rows': len(s), 'strata_present': int(s['strata'].notna().sum()),
                    'key_on_present': int(k[s['strata'].notna().to_numpy()].sum()),
                    'key_on_absent': int(k[s['strata'].isna().to_numpy()].sum()),
                    'labels': int(s['strata'].nunique())}
        return o
    put('Guinea-Bissau sample strata key', _gs)
gr = load('Guinea-Bissau', 'household_roster')
if gr is not None and 'Departed' in gr:
    put('Guinea-Bissau roster Departed by t', lambda: by_t(gr['Departed'].astype('boolean').fillna(False), gr))

# Burkina
for lab in ('interview_date', 'food_acquired'):
    d = load('Burkina_Faso', lab)
    if d is not None:
        put(f'Burkina_Faso {lab} rows by t', lambda d=d: {str(k): int(v) for k, v in tcol(d).value_counts().sort_index().items()})

# Benin
bs = load('Benin', 'sample')
if bs is not None:
    def _bs():
        s = bs[tcol(bs) == '2021-22']
        i = s.index.get_level_values('i').astype(str)
        return {'rows': len(s), 'E2_ids': int(pd.Series(i).str.startswith('E2_').sum()),
                'panel_weight_nonnull': int(s['panel_weight'].notna().sum()),
                'strata_present': int(s['strata'].notna().sum()),
                'key_on_present': int(keyed(s, 'Benin::sample::region-milieu-stratum')[s['strata'].notna().to_numpy()].sum())}
    put('Benin sample 2021-22', _bs)

# Uganda assets
d = load('Uganda', 'assets')
if d is not None:
    put('Uganda assets rows', lambda: len(d))

out_path.write_text(json.dumps(H, indent=1, default=str))
for k, v in H.items():
    print(k, '=>', json.dumps(v, default=str)[:600])
