import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import lsms_library as ll, pandas as pd, difflib
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile

c = ll.Country('GhanaLSS')
fe = c.food_expenditures()
col = fe.columns[0]
t = df_from_orgfile(Path('lsms_library/countries/GhanaLSS/_/categorical_mapping.org'), name='harmonize_food')
t.columns=[str(x).strip() for x in t.columns]
pl = set(t['Preferred Label'].astype(str).str.strip())

jv = fe.index.get_level_values('j').astype(str)
unmapped = ~pd.Series(jv).isin(pl).values
tot = fe[col].sum()
print(f'rows total {len(fe):,}   unmapped rows {unmapped.sum():,} ({unmapped.sum()/len(fe)*100:.1f}%)')
print(f'expenditure total {tot:,.0f}   unmapped {fe[col][unmapped].sum():,.0f} ({fe[col][unmapped].sum()/tot*100:.1f}%)')

print('\n=== unmapped share by wave ===')
d = pd.DataFrame({'t':fe.index.get_level_values('t').astype(str),'j':jv,'x':fe[col].values,'un':unmapped})
w = d.groupby('t').apply(lambda g: pd.Series({'rows':len(g),'unmapped_rows_pct':100*g['un'].mean(),
                                              'unmapped_exp_pct':100*g.loc[g['un'],'x'].sum()/g['x'].sum() if g['x'].sum() else 0}),
                         include_groups=False)
print(w.round(1).to_string())

print('\n=== top unmapped j by expenditure ===')
top = d[d['un']].groupby('j')['x'].agg(['sum','count']).sort_values('sum',ascending=False)
top['pct_of_total']=100*top['sum']/tot
print(top.head(25).round(2).to_string())

# recoverability by case-insensitive / close match to country PL
print('\n=== recoverability of the unmapped labels ===')
low={p.lower():p for p in pl}
rec_ci, rec_near, hard = [],[],[]
for u in sorted(set(d.loc[d['un'],'j'])):
    if u.lower() in low: rec_ci.append((u,low[u.lower()]))
    else:
        n=difflib.get_close_matches(u,sorted(pl),n=1,cutoff=0.85)
        (rec_near if n else hard).append((u,n[0] if n else None))
print(f'case-insensitive match to a country PL: {len(rec_ci)}')
for a,b in rec_ci: print(f'    {a!r} -> {b!r}')
print(f'close match (>=0.85): {len(rec_near)}')
for a,b in rec_near: print(f'    {a!r} ~> {b!r}')
print(f'NO country-table counterpart: {len(hard)}')
hx = d[d['un']&d['j'].isin([a for a,_ in hard])].groupby('j')['x'].sum().sort_values(ascending=False)
print(f'   their expenditure share: {100*hx.sum()/tot:.1f}%')
for k,v in hx.items(): print(f'    {k!r}: {100*v/tot:.2f}%')
