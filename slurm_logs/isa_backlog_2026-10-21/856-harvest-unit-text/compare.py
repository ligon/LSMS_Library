"""GH #856 step 2 -- compare two build_and_measure.py outputs row by row.

    python compare.py <before_dir> <after_dir> <out.json>

Rows are compared on EVERY served index level and column.  Rows identical
in both builds are "unchanged".  The rest are paired on every field except
``u`` to show exactly which rows changed only their unit, and to what; any
row that does not pair 1:1 that way is listed (a grain collision, or a
second-order effect of the sale ladder).  The kg-factor frames ride along
row for row (harvest_kg_factors returns one row per crop_production row, in
the same order).
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

before, after, out_json = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])

KEYS = ['t', 'i', 'v', 'plot_id', 'crop', 'u', 'condition']


def load(d):
    cp = pd.read_parquet(d / 'cp.parquet')
    for name in ('kg_default', 'kg_shipped'):
        f = pd.read_parquet(d / f'{name}.parquet')
        assert len(f) == len(cp)
        cp[f'{name}_kg_per_unit'] = f['kg_per_unit'].to_numpy(dtype='float64')
        cp[f'{name}_source'] = f['KgFactorSource'].astype(str).to_numpy()
    return cp, json.loads((d / 'summary.json').read_text())


b, sb = load(before)
a, sa = load(after)
served = [c for c in b.columns if not (c.startswith('kg_default') or c.startswith('kg_shipped'))]
assert served == [c for c in a.columns if not (c.startswith('kg_default') or c.startswith('kg_shipped'))]


def norm(df, cols):
    """String view for exact matching (NA-safe, float repr exact)."""
    out = pd.DataFrame(index=df.index)
    for c in cols:
        s = df[c]
        if pd.api.types.is_float_dtype(s):
            out[c] = s.astype('float64').map(lambda x: 'NA' if pd.isna(x) else repr(float(x)))
        else:
            out[c] = s.astype('string').fillna('NA')
    return out


nb, na_ = norm(b, served), norm(a, served)
mb = nb.merge(na_, how='outer', on=served, indicator=True)
only_b = mb[mb['_merge'] == 'left_only']
only_a = mb[mb['_merge'] == 'right_only']
both_n = int((mb['_merge'] == 'both').sum())

# Pair the changed rows on everything but u.
nonu = [c for c in served if c != 'u']
pb = only_b.drop(columns='_merge').rename(columns={'u': 'u_before'})
pa = only_a.drop(columns='_merge').rename(columns={'u': 'u_after'})
paired = pb.merge(pa, how='outer', on=nonu, indicator=True)
pairs = paired[paired['_merge'] == 'both']
unpaired_before = paired[paired['_merge'] == 'left_only']
unpaired_after = paired[paired['_merge'] == 'right_only']
dup_keys = int(pairs.duplicated(nonu).sum())

u_changes = (pairs.groupby(['t', 'u_before', 'u_after']).size()
             .rename('rows').reset_index())

# kg factors on rows unchanged in every served field: did any move?
kcols = ['kg_default_kg_per_unit', 'kg_default_source',
         'kg_shipped_kg_per_unit', 'kg_shipped_source']
bk = pd.concat([nb, b[kcols]], axis=1)
ak = pd.concat([na_, a[kcols]], axis=1)
same = bk.merge(ak, on=served, suffixes=('_b', '_a'))
kg_moved = {}
for name in ('kg_default', 'kg_shipped'):
    x, y = same[f'{name}_kg_per_unit_b'], same[f'{name}_kg_per_unit_a']
    moved = ~((x == y) | (x.isna() & y.isna()))
    src = same.loc[moved].groupby([f'{name}_source_b', f'{name}_source_a', 'u']).size()
    kg_moved[name] = {
        'unchanged_rows_whose_factor_moved': int(moved.sum()),
        'by_source_and_u': {'|'.join(map(str, k)): int(v) for k, v in src.items()},
    }

# Second pairing: on the HARVEST fields only (everything but u and the sale
# columns), so a row whose sale attachment moved -- with or without a u
# change -- is classified rather than left unpaired.
SALE = ['Quantity_sold', 'Value_sold', 'Derivation']
harvest_fields = [c for c in served if c not in SALE + ['u']]
hb = only_b.drop(columns='_merge').rename(columns={c: f'{c}_before' for c in SALE + ['u']})
ha = only_a.drop(columns='_merge').rename(columns={c: f'{c}_after' for c in SALE + ['u']})
hp = hb.merge(ha, how='outer', on=harvest_fields, indicator=True)
hpairs = hp[hp['_merge'] == 'both'].copy()
hpairs['u_changed'] = hpairs['u_before'] != hpairs['u_after']
hpairs['sale_changed'] = ((hpairs['Quantity_sold_before'] != hpairs['Quantity_sold_after'])
                          | (hpairs['Value_sold_before'] != hpairs['Value_sold_after']))
hpairs['sale_before'] = hpairs['Value_sold_before'] != 'NA'
hpairs['sale_after'] = hpairs['Value_sold_after'] != 'NA'
hpairs['derivation_changed'] = hpairs['Derivation_before'] != hpairs['Derivation_after']
cls = (hpairs.groupby(['t', 'u_before', 'u_after', 'u_changed', 'sale_before', 'sale_after',
                       'sale_changed', 'derivation_changed']).size()
       .rename('rows').reset_index())


def _mwk(col):
    v = pd.to_numeric(hpairs[col].replace('NA', np.nan), errors='coerce')
    return v


hpairs['vb'] = _mwk('Value_sold_before')
hpairs['va'] = _mwk('Value_sold_after')
attach = {
    'rows_sale_attached_before_only': int((hpairs.sale_before & ~hpairs.sale_after).sum()),
    'rows_sale_attached_after_only': int((~hpairs.sale_before & hpairs.sale_after).sum()),
    'rows_sale_changed_both_attached': int((hpairs.sale_before & hpairs.sale_after
                                            & hpairs.sale_changed).sum()),
    'mwk_detached': float(hpairs.loc[hpairs.sale_before & ~hpairs.sale_after, 'vb'].sum()),
    'mwk_attached': float(hpairs.loc[~hpairs.sale_before & hpairs.sale_after, 'va'].sum()),
    'mwk_attached_total_before': float(pd.to_numeric(b['Value_sold'], errors='coerce').sum()),
    'mwk_attached_total_after': float(pd.to_numeric(a['Value_sold'], errors='coerce').sum()),
    'rows_with_sale_before': int(pd.to_numeric(b['Value_sold'], errors='coerce').notna().sum()),
    'rows_with_sale_after': int(pd.to_numeric(a['Value_sold'], errors='coerce').notna().sum()),
}

# kg factors on EVERY u-changed row (harvest-field pairing): source transitions.
uch = hpairs[hpairs['u_changed']][harvest_fields + ['u_before', 'u_after']]
trans = {}
for name in ('kg_default', 'kg_shipped'):
    kb = bk[harvest_fields + ['u', f'{name}_source', f'{name}_kg_per_unit']].rename(
        columns={'u': 'u_before', f'{name}_source': 'src_before',
                 f'{name}_kg_per_unit': 'kg_before'})
    ka = ak[harvest_fields + ['u', f'{name}_source', f'{name}_kg_per_unit']].rename(
        columns={'u': 'u_after', f'{name}_source': 'src_after',
                 f'{name}_kg_per_unit': 'kg_after'})
    j = uch.merge(kb, on=harvest_fields + ['u_before']).merge(ka, on=harvest_fields + ['u_after'])
    assert len(j) == len(uch), (len(j), len(uch))
    g = j.groupby(['u_before', 'u_after', 'src_before', 'src_after'], dropna=False)
    trans[name] = [
        {'u_before': k[0], 'u_after': k[1], 'src_before': k[2], 'src_after': k[3],
         'rows': int(len(v)),
         'kg_after_median': (None if v['kg_after'].isna().all()
                             else float(v['kg_after'].median())),
         'kg_after_min': (None if v['kg_after'].isna().all()
                          else float(v['kg_after'].min())),
         'kg_after_max': (None if v['kg_after'].isna().all()
                          else float(v['kg_after'].max()))}
        for k, v in g]

moved = hpairs[hpairs.sale_before != hpairs.sale_after]
moved_rows = moved[['t', 'i', 'plot_id', 'crop', 'condition', 'perennial', 'Quantity',
                    'u_before', 'u_after', 'Quantity_sold_before', 'Value_sold_before',
                    'Quantity_sold_after', 'Value_sold_after', 'Derivation_after']]
u_changed_by_module = (hpairs[hpairs.u_changed]
                       .groupby(['t', 'perennial', 'u_after']).size()
                       .rename('rows').reset_index())

result = {
    'u_changed_rows_by_wave_module': u_changed_by_module.to_dict(orient='records'),
    'sale_attachment_moved_rows': moved_rows.head(300).to_dict(orient='records'),
    'harvest_field_pairing': {
        'pairs': int(len(hpairs)),
        'ambiguous': int(hpairs.duplicated(harvest_fields).sum()),
        'unpaired_before': int((hp['_merge'] == 'left_only').sum()),
        'unpaired_after': int((hp['_merge'] == 'right_only').sum()),
        'classes': cls.to_dict(orient='records'),
        'sale_attachment': attach,
    },
    'before': str(before), 'after': str(after),
    'rows_before': int(len(b)), 'rows_after': int(len(a)),
    'rows_identical_in_every_served_field': both_n,
    'rows_only_before': int(len(only_b)), 'rows_only_after': int(len(only_a)),
    'pairs_differing_only_in_u': int(len(pairs)),
    'pairs_ambiguous_on_non_u_fields': dup_keys,
    'u_changes': u_changes.to_dict(orient='records'),
    'n_unpaired_before': int(len(unpaired_before)),
    'n_unpaired_after': int(len(unpaired_after)),
    'kg_factor_moved_on_unchanged_rows': kg_moved,
    'kg_factor_transitions_on_u_changed_rows': trans,
    'kg_source_counts': {'before': {k: sb[k]['source_counts'] for k in ('kg_default', 'kg_shipped')},
                         'after': {k: sa[k]['source_counts'] for k in ('kg_default', 'kg_shipped')}},
    'harvest_kg_by_wave': {k: {'before': sb[k]['harvest_kg_by_wave'],
                               'after': sa[k]['harvest_kg_by_wave']}
                           for k in ('kg_default', 'kg_shipped')},
    'malawi_build_warnings': {'before': sb.get('malawi_warnings'),
                              'after': sa.get('malawi_warnings')},
}
out_json.write_text(json.dumps(result, indent=1, default=str) + '\n')
print(json.dumps({k: result[k] for k in (
    'rows_before', 'rows_after', 'rows_identical_in_every_served_field',
    'rows_only_before', 'rows_only_after', 'pairs_differing_only_in_u',
    'pairs_ambiguous_on_non_u_fields', 'n_unpaired_before', 'n_unpaired_after')}, indent=1))
print(u_changes.to_string())
print(json.dumps({k: v for k, v in result['harvest_field_pairing'].items() if k != 'classes'}, indent=1))
print(cls.to_string())
print(json.dumps(result['kg_factor_moved_on_unchanged_rows'], indent=1))
for name, rows in trans.items():
    print(name)
    for r in rows:
        print('  ', r)
print(json.dumps(result['kg_source_counts'], indent=1))
print(json.dumps(result['harvest_kg_by_wave'], indent=1))
