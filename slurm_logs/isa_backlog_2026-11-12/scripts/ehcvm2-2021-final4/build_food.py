"""Cold-build the food tables and sample of one country into a private root.

usage: python build_food.py COUNTRY OUTDIR
Writes OUTDIR/<name>.parquet (index reset) and OUTDIR/meta.json.  The kg-factor detail
(`food_kg_factors` on the served food_acquired, defaults) is written as kgf.parquet.
"""
import json, os, sys, time, traceback, warnings, collections
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
from lsms_library import paths
root = os.path.realpath(str(paths.data_root()))
assert root.startswith('/local/job'), root
import pandas as pd
import lsms_library as ll
from lsms_library.transformations import food_kg_factors

country, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
c = ll.Country(country)
meta = {'country': country, 'checkout': CHECKOUT, 'data_root': root, 'waves': list(c.waves), 'tables': {}}
CALLS = [('sample', 'sample', {}), ('food_acquired', 'food_acquired', {}),
         ('food_expenditures', 'food_expenditures', {}),
         ('food_quantities', 'food_quantities', {}),
         ('food_quantities_units', 'food_quantities', {'units': 'units'}),
         ('food_prices', 'food_prices', {})]


def jsonable(v):
    try:
        return json.loads(json.dumps(v, default=str))
    except Exception:
        return repr(v)[:2000]


def save(name, df):
    flat = df.reset_index()
    for col in flat.columns:
        if flat[col].dtype == object:
            flat[col] = flat[col].astype('string')
    flat.to_parquet(os.path.join(outdir, f'{name}.parquet'))


fa = None
for name, method, kw in CALLS:
    rec = {}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(c, method)(**kw)
            rec.update(rows=len(df), index=list(df.index.names), columns=list(df.columns),
                       dup_index=int(df.index.duplicated().sum()),
                       attrs={k: jsonable(v) for k, v in (df.attrs or {}).items()
                              if k in ('kg_factor_sources', 'kg_factor_baseline_gate', 'kg_factor_size_pool',
                                       'kg_factor_wave_spread', 'derivations', 'labels_unmatched')})
            save(name, df)
            if name == 'food_acquired':
                fa = df
        except Exception as exc:
            rec['error'] = f'{type(exc).__name__}: {exc}'[:4000]
            rec['traceback'] = traceback.format_exc()[-4000:]
    rec['seconds'] = round(time.time() - t0, 1)
    rec['warnings'] = dict(collections.Counter(w.category.__name__ for w in caught))
    rec['warning_samples'] = {}
    for w in caught:
        lst = rec['warning_samples'].setdefault(w.category.__name__, [])
        if len(lst) < 3:
            lst.append(str(w.message)[:500])
    meta['tables'][name] = rec
    print(country, name, rec.get('rows'), rec['seconds'], rec.get('error', '')[:300], flush=True)
    json.dump(meta, open(os.path.join(outdir, 'meta.json'), 'w'), indent=1, default=str)

if fa is not None:
    try:
        kgf = food_kg_factors(fa)
        keep = [col for col in ('kg_per_unit', 'KgFactorSource', 'kg_size_pooled', 'baseline_refused_spread')
                if col in kgf.columns]
        save('kgf', kgf[keep])
        meta['kgf'] = {'rows': len(kgf), 'sources': jsonable(kgf.attrs.get('kg_factor_sources'))}
    except Exception as exc:
        meta['kgf'] = {'error': f'{type(exc).__name__}: {exc}'[:2000]}
json.dump(meta, open(os.path.join(outdir, 'meta.json'), 'w'), indent=1, default=str)
print('DONE', country, flush=True)
