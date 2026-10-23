import pandas as pd, numpy as np, warnings; warnings.simplefilter('ignore')
OUT = '/local/job40390255/data-private/824-ml/out'
b = pd.read_parquet(f'{OUT}/base_served.parquet'); n = pd.read_parquet(f'{OUT}/branch_served.parquet')
tq = n.perennial.fillna(False) & (n.index.get_level_values('t') == '2014-15')
bq, nq, m = b.Quantity[tq], n.Quantity[tq], n.months_harvested[tq]
same = ((bq == nq) | (bq.isna() & nq.isna())).fillna(False)
s = pd.DataFrame({'bq': bq[same], 'nq': nq[same], 'm': m[same]})
print('coinciding 2014-15 tree rows', len(s), ': both 0', int((s.bq == 0).sum()), ' both NA', int(s.bq.isna().sum()), ' equal positive', int((s.bq > 0).sum()),
      '; of which months==Quantity==1', int(((s.bq == 1)).sum()))
print('rows with Unit_sold but Q_sold==0&V_sold==0 or Q_sold NA:')
x = n[(n.Unit_sold.notna()) & (((n.Quantity_sold == 0) & (n.Value_sold == 0)) | n.Quantity_sold.isna())]
print(x[['Quantity_sold', 'Unit_sold', 'Value_sold', 'perennial']].reset_index(drop=True).assign(t=x.index.get_level_values('t')).to_string())
# Site Q tree row
big = n[tq & (n.Quantity >= 9999)]
print('2014-15 tree rows with Quantity >= 9999:', len(big), big[['Quantity', 'u', 'months_harvested', 'Quantity_sold', 'Unit_sold', 'Value_sold']].to_string(index=False))
print('2014-15 tree Quantity quantiles', nq.quantile([.5, .9, .99, 1]).round(2).to_dict())
# downstream
for name in ['harvest_kg', 'gross_crop_revenue', 'gross_crop_revenue_j', 'yield_kg']:
    try:
        x0 = pd.read_parquet(f'{OUT}/base_{name}.parquet'); x1 = pd.read_parquet(f'{OUT}/branch_{name}.parquet')
    except Exception as e:
        print(name, 'missing', e); continue
    same_idx = x0.index.equals(x1.index)
    try:
        eq = x0.equals(x1)
    except Exception:
        eq = None
    print(name, 'shape', x0.shape, x1.shape, 'index equal', same_idx, 'frame equal', eq, 'cols', list(x1.columns))
    if not eq and same_idx:
        for c in x1.columns:
            if c in x0 and pd.api.types.is_numeric_dtype(x1[c]):
                d = ~(((x0[c] == x1[c]) | (x0[c].isna() & x1[c].isna())).fillna(False))
                print('   ', c, 'rows differ', int(d.sum()), 'sum base', float(x0[c].sum()), 'branch', float(x1[c].sum()))
