"""Cold plot_inputs only (+ per wave, nitrogen_kg, seed_kg, Feature parity).  usage: <outdir>"""
import json, pickle, sys, warnings
from collections import Counter
from pathlib import Path
import pandas as pd
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
import lsms_library as ll
from lsms_library import transformations as T
c = ll.Country('Benin')
summ = {}
for k, kw in (('plot_inputs', {}), ("plot_inputs__waves=['2018-19']", {'waves': ['2018-19']}),
              ("plot_inputs__waves=['2021-22']", {'waves': ['2021-22']})):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        df = c.plot_inputs(**kw)
    pickle.dump(df, open(out / f'{k}.pkl', 'wb'))
    summ[k] = {'rows': len(df), 'warnings': dict(Counter(x.category.__name__ for x in w))}
pi = pickle.load(open(out / 'plot_inputs.pkl', 'rb'))
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    for fn in ('nitrogen_kg', 'seed_kg'):
        r = getattr(T, fn)(pi)
        pickle.dump(r, open(out / f'T_{fn}.pkl', 'wb'))
        summ[fn] = {c_: float(pd.to_numeric(r[c_], errors='coerce').sum()) for c_ in r.columns
                    if pd.api.types.is_numeric_dtype(r[c_])}
    f = ll.Feature('plot_inputs')(['Benin'])
fdf = f.xs('Benin', level='country')
summ['feature'] = {'rows': len(f), 'unique': bool(f.index.is_unique),
                   'same_i_t': set(zip(fdf.index.get_level_values('i'), fdf.index.get_level_values('t').astype(str)))
                   == set(zip(pi.index.get_level_values('i'), pi.index.get_level_values('t').astype(str))),
                   'cols_equal': {col: sorted(map(str, fdf[col])) == sorted(map(str, pi[col]))
                                  for col in pi.columns if col in fdf.columns}}
for wv in ('2018-19', '2021-22'):
    p = pi[pi.index.get_level_values('t').astype(str) == wv]
    summ[f'units_{wv}'] = {col: p[col].astype(str).value_counts(dropna=False).to_dict()
                           for col in ('Unit_purchased', 'Unit_received_as_gift')}
    summ[f'u_{wv}'] = pd.Series(p.index.get_level_values('u').astype(str)).value_counts().to_dict()
json.dump(summ, open(out / 'summary.json', 'w'), indent=1, default=str)
print(json.dumps(summ, default=str)[:4000])
print('DONE')
