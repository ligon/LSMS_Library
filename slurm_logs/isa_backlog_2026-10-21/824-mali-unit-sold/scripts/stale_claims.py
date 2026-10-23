import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd
from lsms_library import transformations as T
cp = pd.read_parquet(f'/local/job40390255/data-private/824-ml/out/{label}_served.parquet')
print('rows', len(cp), 'by t', cp.groupby(level='t').size().to_dict())
pr = cp[(cp.Quantity_sold > 0) & (cp.Value_sold > 0)]
print('priced (Qs>0 & Vs>0)', len(pr), 'by t', pr.groupby(level='t').size().to_dict())
print('priced by t x perennial', pr.groupby([pr.index.get_level_values('t'), pr.perennial]).size().to_dict())
print('priced with u==Kg by t', pr[pr.u == 'Kg'].groupby(level='t').size().to_dict())
if 'Unit_sold' in pr:
    print('priced with Unit_sold==Kg by t', pr[pr.Unit_sold == 'Kg'].groupby(level='t').size().to_dict())
f = T.harvest_kg_factors(cp)
print('harvest_kg_factors type', type(f).__name__, getattr(f, 'columns', None) if hasattr(f, 'columns') else '')
if isinstance(f, pd.DataFrame):
    src = [c for c in f.columns if 'Source' in c or 'source' in c]
    print(' source cols', src)
    kgc = [c for c in f.columns if 'kg' in c.lower()]
    print(' kg cols', kgc)
    j = cp[['u']].join(f, how='left') if f.index.equals(cp.index) else None
    if j is not None:
        for s in src:
            print(' by u x', s, j.groupby(['u', s], dropna=False).size().to_dict())
        fac = kgc[0]
        print(' rows with a factor', int(j[fac].notna().sum()), 'of', len(j), '; u with factor', sorted(j.loc[j[fac].notna(), 'u'].dropna().unique()))
