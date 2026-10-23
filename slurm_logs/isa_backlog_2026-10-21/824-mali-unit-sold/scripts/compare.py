"""Base vs branch served Mali crop_production: aggregates only."""
import sys, json
import pandas as pd, numpy as np
OUT = '/local/job40390255/data-private/824-ml/out'
b = pd.read_parquet(f'{OUT}/base_served.parquet')
n = pd.read_parquet(f'{OUT}/branch_served.parquet')
res = {}
print('rows base/branch', len(b), len(n), ' index equal', b.index.equals(n.index))
print('columns added', [c for c in n.columns if c not in b.columns], ' removed', [c for c in b.columns if c not in n.columns])
for c in b.columns:
    eq = (b[c] == n[c]) | (b[c].isna() & n[c].isna())
    eq = eq.fillna(False)
    diff = ~eq
    if diff.any():
        d = n[diff]
        print(f'  {c}: {int(diff.sum())} rows differ; by t x perennial',
              d.groupby([d.index.get_level_values("t"), d.perennial]).size().to_dict())
    else:
        print(f'  {c}: identical')
# u diffs detail
du = ~(((b.u == n.u) | (b.u.isna() & n.u.isna())).fillna(False))
print('  u diffs: base', b.u[du].astype(object).value_counts(dropna=False).to_dict(), '-> branch', n.u[du].value_counts(dropna=False).to_dict())
# tree Quantity change
tq = n.perennial.fillna(False) & (n.index.get_level_values('t') == '2014-15')
bq = b.Quantity[tq]
print('2014-15 tree rows', int(tq.sum()), ' base Quantity all integer<=12:', bool(((bq.dropna() % 1 == 0) & (bq.dropna() <= 12)).all()), ' base nonnull', int(bq.notna().sum()))
print('  base Quantity == branch months_harvested on', int((bq == n.months_harvested[tq]).sum()), 'of', int(bq.notna().sum()), 'nonnull')
nq = n.Quantity[tq]
print('  branch Quantity: nonnull', int(nq.notna().sum()), 'pos', int((nq > 0).sum()), 'zero', int((nq == 0).sum()), 'median pos', float(nq[nq > 0].median()))
print('  rows whose Quantity changed', int((~(((bq == nq) | (bq.isna() & nq.isna())).fillna(False))).sum()))
print('  base sum', float(bq.sum()), 'branch sum', float(nq.sum()))
# months_harvested
for t in ['2014-15', '2017-18']:
    m = n[(n.index.get_level_values('t') == t)]
    print(t, 'months_harvested on tree rows', m[m.perennial.fillna(False)].months_harvested.value_counts(dropna=False).sort_index().to_dict(), '; non-null on field rows', int(m[~m.perennial.fillna(False)].months_harvested.notna().sum()))
print('Derivation', n.Derivation.value_counts(dropna=False).to_dict())
print('Derivation on non-perennial rows', int(n.Derivation[~n.perennial.fillna(False)].notna().sum()))
# Unit_sold coverage / agreement
n2 = n.copy()
n2['t_'] = n2.index.get_level_values('t'); n2['kind'] = np.where(n2.perennial.fillna(False), 'tree', 'field')
priced = n2[(n2.Quantity_sold > 0) & (n2.Value_sold > 0)]
rows = []
for (t, k), g in priced.groupby(['t_', 'kind']):
    knows = g.Unit_sold.notna() & g.u.notna()
    agree = knows & (g.Unit_sold == g.u)
    rows.append(dict(t=t, kind=k, priced=len(g), unit_sold_nonnull=int(g.Unit_sold.notna().sum()),
                     knows_both=int(knows.sum()), agree=int(agree.sum()), differ=int((knows & ~agree).sum()),
                     sold_in_kg=int((g.Unit_sold == 'Kg').sum()), harvest_u_kg=int((g.u == 'Kg').sum())))
tab = pd.DataFrame(rows); print(tab.to_string(index=False))
print('disagreeing (u -> Unit_sold) top cells by wave')
for t, g in priced.groupby('t_'):
    k = g[g.Unit_sold.notna() & g.u.notna() & (g.Unit_sold != g.u)]
    print(' ', t, k.groupby(['u', 'Unit_sold']).size().sort_values(ascending=False).head(6).to_dict())
# Unit_sold where no sale
ns = n2[(n2.Quantity_sold == 0) & (n2.Value_sold == 0)]
print('rows with Q_sold==0 & V_sold==0:', len(ns), ' Unit_sold non-null there', int(ns.Unit_sold.notna().sum()))
print('Unit_sold non-null where Quantity_sold NA', int((n2.Unit_sold.notna() & n2.Quantity_sold.isna()).sum()))
print('Unit_sold values', n2.Unit_sold.value_counts(dropna=False).to_dict())
tab.to_csv(f'{OUT}/unit_sold_table.csv', index=False)
