"""Cold-build every table of one country from one checkout into a private root.

usage: python build_country.py COUNTRY OUTDIR [--tables t1 t2 ...]
Writes OUTDIR/<table>.parquet (index reset, so dtypes survive) and OUTDIR/meta.json
(per table: rows, index names, columns, dtypes, seconds, warnings by class, attrs
summaries, error).  Asserts the import is the checkout in PYTHONPATH and that the
data root is private.
"""
import json, os, sys, time, traceback, warnings, collections
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
from lsms_library import paths
root = str(paths.data_root())
for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache', os.path.expanduser('~/.local/share/lsms_library')):
    assert not os.path.realpath(root).startswith(os.path.realpath(bad)), root
import pandas as pd
import lsms_library as ll
from lsms_library import diagnostics

country, outdir = sys.argv[1], sys.argv[2]
only = sys.argv[sys.argv.index('--tables') + 1:] if '--tables' in sys.argv else None
os.makedirs(outdir, exist_ok=True)
c = ll.Country(country)
scheme = list(c.data_scheme)
DERIVED_CALLS = {
    'food_expenditures': {}, 'food_prices': {}, 'food_quantities': {},
    'household_characteristics': {},
}
tables = [t for t in scheme if t not in ('panel_ids', 'updated_ids')]
for t in DERIVED_CALLS:
    if t not in tables:
        tables.append(t)
if only:
    tables = [t for t in tables if t in only]
meta = {'country': country, 'checkout': CHECKOUT, 'data_root': root,
        'waves': list(c.waves), 'scheme': scheme, 'tables': {}}

def summarise_attrs(df):
    out = {}
    for k, v in (df.attrs or {}).items():
        if k in ('kg_factor_sources', 'kg_factor_baseline_gate', 'kg_factor_size_pool',
                 'kg_factor_declared_none', 'kg_factor_wave_spread', 'labels_unmatched',
                 'price_rows_dropped'):
            try:
                out[k] = json.loads(json.dumps(v, default=str))
            except Exception:
                out[k] = repr(v)[:2000]
        else:
            out[k] = type(v).__name__
    return out

for t in tables:
    rec = {}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(c, t)()
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f'{t} returned {type(df).__name__}')
            rec.update(rows=len(df), index=list(df.index.names), columns=list(df.columns),
                       dtypes={k: str(v) for k, v in df.reset_index().dtypes.items()},
                       dup_index=int(df.index.duplicated().sum()), attrs=summarise_attrs(df))
            flat = df.reset_index()
            for col in flat.columns:
                if flat[col].dtype == object:
                    flat[col] = flat[col].astype('string')
            flat.to_parquet(os.path.join(outdir, f'{t}.parquet'))
        except Exception as exc:
            rec['error'] = f'{type(exc).__name__}: {exc}'[:4000]
            rec['traceback'] = traceback.format_exc()[-4000:]
    rec['seconds'] = round(time.time() - t0, 1)
    counts = collections.Counter(type(w.message).__name__ if isinstance(w.message, Warning) else w.category.__name__ for w in caught)
    rec['warnings'] = dict(counts)
    rec['warning_samples'] = {}
    for w in caught:
        name = w.category.__name__
        rec['warning_samples'].setdefault(name, [])
        if len(rec['warning_samples'][name]) < 4:
            rec['warning_samples'][name].append(str(w.message)[:600])
    meta['tables'][t] = rec
    print(f'{country} {t}: {rec.get("rows")} rows {rec["seconds"]}s {rec.get("error", "")[:200]}', flush=True)
    with open(os.path.join(outdir, 'meta.json'), 'w') as fh:
        json.dump(meta, fh, indent=1, default=str)
# Derived food tables one wave at a time: attrs then describe the served wave only
# (GH #1487), so the rung partition and kg totals can be read per older wave.
meta['per_wave'] = {}
for t in ('food_quantities', 'food_prices'):
    for w in c.waves:
        key = f'{t}|{w}'
        rec = {}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                df = getattr(c, t)(waves=[w])
                rec['rows'] = len(df)
                rec['attrs'] = summarise_attrs(df)
                if t == 'food_quantities' and 'u' in (df.index.names or []):
                    u = df.index.get_level_values('u')
                    q = pd.to_numeric(df.iloc[:, 0], errors='coerce')
                    rec['kg_total'] = float(q[u == 'kg'].sum())
                    rec['kg_rows'] = int((u == 'kg').sum())
                    rec['column'] = df.columns[0]
            except Exception as exc:
                rec['error'] = f'{type(exc).__name__}: {exc}'[:2000]
        rec['warnings'] = dict(collections.Counter(w_.category.__name__ for w_ in caught))
        meta['per_wave'][key] = rec
        print(country, key, rec.get('rows'), rec.get('kg_total'), rec.get('error', '')[:200], flush=True)
        with open(os.path.join(outdir, 'meta.json'), 'w') as fh:
            json.dump(meta, fh, indent=1, default=str)
print('DONE', country, flush=True)
