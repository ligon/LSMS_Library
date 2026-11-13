"""GH #637 round 3: compare the public-API frames of three cold builds
(development 9a0ed5f3c, PR head before the rulings 5239daecd, new head).

Usage: python compare.py <private_dir> <aggregate_json>
Reads <private_dir>/{dev,old,new}_*.pkl written by snapshot.py; writes
aggregates only.
"""
import json
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

priv, out = Path(sys.argv[1]), Path(sys.argv[2])
F = {lab: {t: pd.read_pickle(priv / f'{lab}_{t}.pkl') for t in
           ['crop_production', 'plot_inputs', 'harvest_kg', 'yield_kg', 'seed_kg', 'nitrogen_kg',
            'gross_crop_revenue', 'gross_crop_revenue_by_j']} for lab in ['dev', 'old', 'new']}
res = {}
COLS = ['Quantity', 'u', 'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped', 'perennial']


def same(a, b):
    return (a == b).fillna(False) | (a.isna() & b.isna())


for base in ['dev', 'old']:
    B, N = F[base]['crop_production'], F['new']['crop_production']
    key = [n for n in B.index.names]
    assert key == ['i', 't', 'v', 'plot_id', 'j'], key
    R = {'rows': {'base': {str(k): int(v) for k, v in B.groupby(level='t').size().items()},
                  'new': {str(k): int(v) for k, v in N.groupby(level='t').size().items()}}}
    n_per_key = N.groupby(level=key).size()
    assert set(n_per_key.index) == set(B.index), 'old keys not covered one-for-one'
    R['rows_added'] = {str(t): int(v) for t, v in (n_per_key - 1).groupby(level='t').sum().items()}
    # singleton keys: the one new row must equal the base row in every column
    single = n_per_key[n_per_key == 1].index
    Ns = N.droplevel('production_obs').loc[single].sort_index()
    Bs = B.loc[single].sort_index()
    diff = {c: int((~same(Ns[c].astype(Bs[c].dtype) if c != 'u' else Ns[c].astype('string'),
                          Bs[c] if c != 'u' else Bs[c].astype('string'))).sum()) for c in COLS}
    R['singleton_keys'] = int(len(single))
    R['singleton_keys_cells_differing'] = diff
    # split keys: quantity conserved; sales and selected fields reported
    multi = n_per_key[n_per_key > 1].index
    Nm = N.droplevel('production_obs')
    Nm = Nm[Nm.index.isin(multi)]
    agg = Nm.groupby(level=key).agg(Quantity=('Quantity', lambda s: s.sum(min_count=1)),
                                    Quantity_sold=('Quantity_sold', lambda s: s.sum(min_count=1)),
                                    Value_sold=('Value_sold', lambda s: s.sum(min_count=1)),
                                    n_sales=('Value_sold', lambda s: int(s.notna().sum())))
    Bm = B.loc[multi]
    agg = agg.loc[Bm.index]
    R['split_keys'] = {str(t): int(v) for t, v in pd.Series(1, index=multi).groupby(level='t').sum().items()}
    R['split_keys_quantity_conserved'] = bool(same(agg['Quantity'].astype('Float64'), Bm['Quantity'].astype('Float64')).all())
    for c in ['Quantity_sold', 'Value_sold']:
        d = (agg[c].astype('Float64').fillna(0) - Bm[c].astype('Float64').fillna(0))
        R[f'split_keys_{c}_gained'] = {str(t): float(v) for t, v in d.groupby(level='t').sum().items()}
        R[f'split_keys_{c}_keys_gaining'] = {str(t): int(v) for t, v in (d > 0).groupby(level='t').sum().items()}
        R[f'split_keys_{c}_keys_losing'] = int((d < 0).sum())
    R['split_keys_with_two_or_more_sales'] = {str(t): int(v) for t, v in (agg['n_sales'] >= 2).groupby(level='t').sum().items()}
    for c in ['harvest_month', 'intercropped']:
        rows_differ = Nm[c].astype('string').groupby(level=key).nunique(dropna=True) > 1
        R[f'split_keys_{c}_rows_now_disagree'] = {str(t): int(v) for t, v in rows_differ.groupby(level='t').sum().items()}
    R['totals'] = {c: {'base': {str(t): float(v) for t, v in B[c].astype('Float64').groupby(level='t').sum().items()},
                       'new': {str(t): float(v) for t, v in N[c].astype('Float64').groupby(level='t').sum().items()}}
                   for c in ['Quantity', 'Quantity_sold', 'Value_sold']}
    R['new_index_unique'] = bool(N.index.is_unique)
    R['production_obs_modules'] = {str(k): int(v) for k, v in
                                   N.index.get_level_values('production_obs').str.split(':').str[1].value_counts().sort_index().items()}
    # downstream
    D = {}
    for name in ['harvest_kg', 'yield_kg', 'seed_kg', 'nitrogen_kg']:
        try:
            assert_frame_equal(F[base][name].sort_index(), F['new'][name].sort_index())
            D[name] = 'frame-equal'
        except AssertionError:
            b, n = F[base][name], F['new'][name]
            D[name] = {'DIFFERS': {'rows': [len(b), len(n)],
                                   'sum_by_t': {str(t): [float(b.iloc[:, 0].xs(t, level='t').sum()),
                                                         float(n.iloc[:, 0].xs(t, level='t').sum())]
                                                for t in sorted(set(b.index.get_level_values('t')))}}}
    for name in ['gross_crop_revenue', 'gross_crop_revenue_by_j']:
        b, n = F[base][name]['Gross_crop_revenue'], F['new'][name]['Gross_crop_revenue']
        j = pd.concat({'b': b, 'n': n}, axis=1)
        D[name] = {'rows': [len(b), len(n)],
                   'total': {str(t): [float(v['b'].sum()), float(v['n'].sum())] for t, v in j.groupby(level='t')},
                   'households_changed': int((~same(j['b'], j['n'])).sum()),
                   'households_new': int((j['b'].isna() & j['n'].notna()).sum()),
                   'households_lower': int((j['n'] < j['b']).sum())}
    R['downstream'] = D
    res[f'crop_production_{base}_vs_new'] = R
    # plot_inputs
    B, N = F[base]['plot_inputs'], F['new']['plot_inputs']
    P = {'rows': [len(B), len(N)]}
    if base == 'old':
        P['index_equal'] = bool(B.index.equals(N.index))
        P['columns_added'] = [c for c in N.columns if c not in B.columns]
        try:
            assert_frame_equal(B, N.drop(columns=P['columns_added']))
            P['other_columns'] = 'frame-equal'
        except AssertionError:
            P['other_columns'] = 'DIFFERS'
    res[f'plot_inputs_{base}_vs_new'] = P
N = F['new']['plot_inputs']
U = N['Unit_purchased'].astype('string')
inp = N.index.get_level_values('input')
res['unit_purchased'] = {
    'dtype': str(N['Unit_purchased'].dtype),
    'coverage_by_input': {f'{a}|{b}': int(v) for (a, b), v in
                          pd.DataFrame({'input': inp, 'U': U.fillna('<NA>').values}).value_counts().sort_index().items()},
    'quantity_purchased_without_unit': {str(k): int(v) for k, v in
                                        (N['Quantity_purchased'].gt(0).fillna(False) & U.isna()).groupby(inp).sum().items() if v},
    'served_rows_unit_purchased_ne_u': {str(t): int(v) for t, v in
                                        (U.notna() & (U != pd.Series(N.index.get_level_values('u'), index=N.index).astype('string')).fillna(False))
                                        .groupby(level='t').sum().items()},
}
out.write_text(json.dumps(res, indent=1, default=str) + '\n')
print(json.dumps(res, indent=1, default=str))
