"""Before/after corpus comparison for weight normalisation."""
import sys
import numpy as np, pandas as pd

pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 300)

b = pd.read_csv(sys.argv[1])
a = pd.read_csv(sys.argv[2])

key = ['country', 'wave']
print('=== ROW PARITY ===')
print('before rows', len(b), 'after rows', len(a))
bn = b.dropna(subset=['wave']).set_index(key).sort_index()
an = a.dropna(subset=['wave']).set_index(key).sort_index()
print('index identical:', bn.index.equals(an.index))
print('before notes:', sorted(set(b['note'].dropna()) - {''}) or 'none')
print('after  notes:', sorted(set(a['note'].dropna()) - {''}) or 'none')
print('n identical:', (bn['n'] == an['n']).all())

for col in ('weight', 'panel_weight'):
    print(f'\n=== {col} ===')
    nb = bn[bn[f'{col}_n'] > 0]
    na = an[an[f'{col}_n'] > 0]
    print(f'weighted cells: before {len(nb)}  after {len(na)}  '
          f'same set: {nb.index.equals(na.index)}')
    if not len(nb):
        continue
    m = nb[f'{col}_mean']
    norm = m[np.isclose(m, 1.0, rtol=1e-3)]
    exp = m[~np.isclose(m, 1.0, rtol=1e-3)]
    print(f'BEFORE  normalised-type cells: {len(norm)}  '
          f'[min {norm.min():.6f}  median {norm.median():.6f}  max {norm.max():.6f}]'
          if len(norm) else 'BEFORE  normalised-type cells: 0')
    print(f'BEFORE  expansion-type  cells: {len(exp)}  '
          f'[min {exp.min():.2f}  median {exp.median():.2f}  max {exp.max():.2f}]'
          if len(exp) else 'BEFORE  expansion-type  cells: 0')
    ma = na[f'{col}_mean']
    ok = np.isclose(ma, 1.0, rtol=1e-9, atol=0)
    print(f'AFTER   mean==1 (rtol 1e-9): {int(ok.sum())} of {len(ma)}')
    if not ok.all():
        print('  !! CELLS NOT AT MEAN 1:')
        print(na.loc[~ok, [f'{col}_n', f'{col}_mean', f'{col}_min',
                           f'{col}_max', f'{col}_zero', f'{col}_neg']])
    # sum should equal the non-null count
    sums_ok = np.isclose(na[f'{col}_sum'], na[f'{col}_n'], rtol=1e-9)
    print(f'AFTER   sum == non-null n: {int(sums_ok.sum())} of {len(na)}')
    negs = nb[nb[f'{col}_neg'] > 0]
    print(f'negative weights anywhere (before): {len(negs)} cells'
          + (f'\n{negs[[f"{col}_neg", f"{col}_min"]]}' if len(negs) else ''))
    zeros = nb[nb[f'{col}_zero'] > 0]
    print(f'zero weights (before): {len(zeros)} cells, '
          f'{int(zeros[f"{col}_zero"].sum())} rows')

print('\n=== COTEDIVOIRE (the key mixed-scale country) ===')
cols = ['weight_n', 'weight_mean', 'weight_min', 'weight_max', 'weight_sum']
print('BEFORE'); print(bn.loc['CotedIvoire', cols].to_string())
print('AFTER');  print(an.loc['CotedIvoire', cols].to_string())

print('\n=== BEFORE, all weighted cells by mean (the two-scale evidence) ===')
allb = bn[bn['weight_n'] > 0][['weight_n', 'weight_mean', 'weight_sum']]
allb = allb.assign(type=np.where(np.isclose(allb['weight_mean'], 1.0, rtol=1e-3),
                                 'normalised', 'expansion'))
print(allb.groupby('type').agg(cells=('weight_mean', 'size'),
                               min_mean=('weight_mean', 'min'),
                               median_mean=('weight_mean', 'median'),
                               max_mean=('weight_mean', 'max')).to_string())
print('countries with weighted cells:', allb.index.get_level_values(0).nunique())
mixed = allb.groupby(level=0)['type'].nunique()
print('countries mixing BOTH types before:', list(mixed[mixed > 1].index))
