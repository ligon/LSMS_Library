"""Base vs candidate crop_production: identical apart from Quantity_sold_kg.

Usage: compare.py BASE_OUT CAND_OUT REPORT_DIR.  Writes aggregates only.
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from lsms_library.transformations import _sold_kg_factors

base_dir, cand_dir, rep = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
rep.mkdir(parents=True, exist_ok=True)
NEW = 'Quantity_sold_kg'
out = {}


def f64(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


for country in ('Niger', 'Mali'):
    b = pd.read_pickle(base_dir / f'{country}_crop_production.pkl')
    c = pd.read_pickle(cand_dir / f'{country}_crop_production.pkl')
    r = {'rows_base': len(b), 'rows_cand': len(c),
         'index_names_equal': list(b.index.names) == list(c.index.names),
         'index_equal_in_order': bool(b.index.equals(c.index)),
         'new_column_present': NEW in c.columns, 'new_column_in_base': NEW in b.columns,
         'columns_other_than_new_equal_in_order':
             [x for x in c.columns if x != NEW] == list(b.columns)}
    try:
        pd.testing.assert_frame_equal(b, c.drop(columns=[NEW])[list(b.columns)], check_like=False)
        r['frame_equal_apart_from_new'] = True
    except AssertionError as e:
        r['frame_equal_apart_from_new'] = False
        r['frame_diff'] = str(e)[:2000]
    # attrs: report keys whose value differs
    ab, ac = b.attrs, c.attrs
    r['attrs_keys_base'] = sorted(ab)
    r['attrs_keys_cand'] = sorted(ac)
    r['attrs_differ'] = sorted(k for k in set(ab) | set(ac) if repr(ab.get(k)) != repr(ac.get(k)))
    r['dtype_new'] = str(c[NEW].dtype)

    kg = f64(c[NEW])
    q = f64(c['Quantity_sold'])
    v = f64(c['Value_sold'])
    t = np.asarray(c.index.get_level_values('t').astype(str))
    plot = np.asarray(c.index.get_level_values('plot_id').astype(str))
    sale = (q.fillna(0) > 0) | (v.fillna(0) > 0)
    per_wave = {}
    for w in sorted(set(t)):
        m = (t == w)
        k = kg[m]
        per_wave[w] = {'rows': int(m.sum()), 'nonNA': int(k.notna().sum()),
                       'positive': int(k.gt(0).sum()), 'zero': int(k.eq(0).sum()),
                       'kg_sum': round(float(k.sum()), 3),
                       'kg_max': float(k.max()) if k.notna().any() else None,
                       'sale_rows': int(sale[m].sum()),
                       'sale_rows_with_kg': int((sale & kg.notna())[m].sum()),
                       'kg_on_non_sale_rows': int((~sale & kg.gt(0))[m].sum()),
                       'kg_on_non_sale_rows_sum': round(float(kg[m & (~sale & kg.gt(0)).to_numpy()].sum()), 3),
                       'farm_level_rows_with_kg': int((kg.notna().to_numpy() & (plot == 'Farm-level') & m).sum())}
    r['per_wave'] = per_wave

    # Usable price per kg on sale rows: current (sold-unit factor) vs literal sold kg.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        factor, counts = _sold_kg_factors(c)
    cur_kg = q * factor
    cur_ok = np.isfinite(cur_kg) & (cur_kg > 0) & (v > 0)
    lit_ok = np.isfinite(kg) & (kg > 0) & (v > 0)
    either = cur_ok | lit_ok
    support = {}
    for w in sorted(set(t)):
        for where, mask in (('Farm-level', plot == 'Farm-level'), ('on a line', plot != 'Farm-level')):
            m = (t == w) & mask & sale.to_numpy()
            if not m.any():
                continue
            support[f'{w} {where}'] = {
                'sale_rows': int(m.sum()),
                'usable_now': int(cur_ok[m].sum()),
                'usable_with_sold_kg': int(lit_ok[m].sum()),
                'usable_either': int(either[m].sum()),
                'gained': int((lit_ok & ~cur_ok)[m].sum()),
                'lost_if_only_sold_kg': int((cur_ok & ~lit_ok)[m].sum())}
    r['price_support'] = support
    r['sold_kg_factor_counts'] = counts
    # Kg-unit control on served rows
    unit = c['Unit_sold'].astype('string')
    kgu = unit.eq('Kg').fillna(False).to_numpy() & kg.notna().to_numpy() & (q > 0).to_numpy()
    r['kg_unit_control'] = {'rows': int(kgu.sum()), 'kg_eq_qty': int((kg[kgu] == q[kgu]).sum())}
    out[country] = r
    print(country, json.dumps({k: r[k] for k in ('rows_base', 'rows_cand', 'frame_equal_apart_from_new',
                                                 'columns_other_than_new_equal_in_order', 'attrs_differ')}))

(rep / 'compare.json').write_text(json.dumps(out, indent=1, default=str))
print('wrote', rep / 'compare.json')
