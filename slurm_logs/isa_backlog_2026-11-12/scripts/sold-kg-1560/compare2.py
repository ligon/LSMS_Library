"""Follow-up rulings (a)-(d): base vs new candidate, and prior candidate vs new.

Usage: compare2.py BASE_OUT PRIOR_OUT NEW_OUT REPORT_DIR.  Aggregates only.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

base_dir, prior_dir, new_dir, rep = map(Path, sys.argv[1:5])
rep.mkdir(parents=True, exist_ok=True)
NEW, DER = 'Quantity_sold_kg', 'Derivation'
KEYS = {'Niger': ['Niger::crop_production::sold-kg-estimate',
                  'Niger::crop_production::dry-season-sold-kg-no-sale'],
        'Mali': ['Mali::crop_production::sold-kg-estimate']}
out = {}


def keyset(s):
    return s.astype('string').fillna('').map(lambda v: frozenset(k for k in v.split('+') if k))


for country in ('Niger', 'Mali'):
    b = pd.read_pickle(base_dir / f'{country}_crop_production.pkl')
    p = pd.read_pickle(prior_dir / f'{country}_crop_production.pkl')
    c = pd.read_pickle(new_dir / f'{country}_crop_production.pkl')
    r = {'rows': [len(b), len(p), len(c)], 'index_equal_base': bool(b.index.equals(c.index)),
         'index_equal_prior': bool(p.index.equals(c.index)),
         'columns_equal_prior': list(p.columns) == list(c.columns)}
    other = [x for x in b.columns if x != DER]
    try:
        pd.testing.assert_frame_equal(b[other], c[other])
        r['equal_to_base_apart_from_new_and_Derivation'] = True
    except AssertionError as e:
        r['equal_to_base_apart_from_new_and_Derivation'] = False
        r['diff'] = str(e)[:1500]
    # Derivation: which rows differ from base, and by which keys
    kb, kc = keyset(b[DER]), keyset(c[DER])
    added = [kc.iloc[n] - kb.iloc[n] for n in range(len(c))]
    removed = [kb.iloc[n] - kc.iloc[n] for n in range(len(c))]
    changed = np.array([bool(a or d) for a, d in zip(added, removed)])
    t = np.asarray(c.index.get_level_values('t').astype(str))
    plot = np.asarray(c.index.get_level_values('plot_id').astype(str))
    r['derivation_rows_changed'] = int(changed.sum())
    r['derivation_keys_removed'] = sorted({k for d in removed for k in d})
    r['derivation_added_by_key_wave'] = {}
    for key in KEYS[country]:
        has = np.array([key in a for a in added])
        r['derivation_added_by_key_wave'][key] = {
            w: {'rows': int((has & (t == w)).sum()),
                'farm_level': int((has & (t == w) & (plot == 'Farm-level')).sum())}
            for w in sorted(set(t[has]))}
    r['derivation_changed_rows_all_explained'] = bool(
        all(a <= set(KEYS[country]) and not d for a, d in zip(added, removed)))
    # Quantity_sold_kg: prior candidate vs new
    qp = pd.to_numeric(p[NEW], errors='coerce').astype('float64').to_numpy()
    qc = pd.to_numeric(c[NEW], errors='coerce').astype('float64').to_numpy()
    diff = ~((qp == qc) | (np.isnan(qp) & np.isnan(qc)))
    r['sold_kg_changed_vs_prior'] = {
        'rows': int(diff.sum()),
        'now_na_was_value': int((diff & np.isnan(qc) & ~np.isnan(qp)).sum()),
        'kg_removed': float(np.nansum(qp[diff])),
        'by_wave_plot': {f'{w} {pl}': int(((t == w) & (plot == pl) & diff).sum())
                         for w, pl in sorted(set(zip(t[diff], plot[diff])))}}
    if country == 'Niger':
        nosale = np.array(['Niger::crop_production::dry-season-sold-kg-no-sale' in k for k in kc])
        r['no_sale_keyed_rows_equal_changed_rows'] = bool((nosale == diff).all())
    per_wave = {}
    for w in sorted(set(t)):
        k = qc[t == w]
        per_wave[w] = {'positive': int((k > 0).sum()), 'kg': round(float(np.nansum(k)), 3)}
    r['per_wave'] = per_wave
    r['attrs_derivations_keys'] = sorted(c.attrs.get('derivations', {}).get(country, {}).keys())
    out[country] = r
    print(country, json.dumps({k: r[k] for k in ('rows', 'equal_to_base_apart_from_new_and_Derivation',
                                                 'derivation_rows_changed',
                                                 'derivation_changed_rows_all_explained')}), flush=True)

(rep / 'compare2.json').write_text(json.dumps(out, indent=1, default=str))
print('wrote', rep / 'compare2.json')
