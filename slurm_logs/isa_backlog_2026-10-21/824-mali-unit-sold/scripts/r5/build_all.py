# Copied from the red-team review job (mali-sales-review/scripts/build_all.py, 2026-10-23).
"""Cold-build every Mali table (crop_production first); save each frame
(reset_index) + warnings + audit reports + attrs['derivations'].
Usage: python build_all.py <outdir> [table ...]  (default: every table)."""
import sys, os, time, warnings, json
import pandas as pd
import lsms_library as ll
out = sys.argv[1]; os.makedirs(out, exist_ok=True)
c = ll.Country('Mali')
scheme = list(c.data_scheme)
tables = sys.argv[2:] or (['crop_production'] + sorted(set(scheme) - {'panel_ids', 'updated_ids', 'crop_production'}))
print('scheme', scheme, flush=True)
print('tables', tables, flush=True)
summary = {}
for t in tables:
    t0 = time.time()
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            df = getattr(c, t)()
    except Exception as e:
        print('FAIL', t, type(e).__name__, str(e)[:300], flush=True)
        summary[t] = {'error': f'{type(e).__name__}: {str(e)[:300]}'}
        continue
    msgs = [f'{x.category.__name__}: {str(x.message)}' for x in w]
    with open(f'{out}/{t}.warnings.txt', 'w') as f:
        f.write('\n=====\n'.join(msgs))
    if not isinstance(df, pd.DataFrame):
        print('NONFRAME', t, type(df), flush=True); continue
    try:
        with open(f'{out}/{t}.attrs.json', 'w') as f:
            json.dump(df.attrs, f, default=str, indent=1, sort_keys=True)
    except Exception as e:
        print('ATTRS-FAIL', t, e, flush=True)
    flat = df.reset_index()
    for col in flat.columns:
        if flat[col].dtype == object:
            try: flat[col] = flat[col].astype('string')
            except Exception: flat[col] = flat[col].astype(str)
    try:
        flat.to_parquet(f'{out}/{t}.parquet')
    except Exception as e:
        print('PARQUET-FAIL', t, e, flush=True)
    summary[t] = {'shape': list(df.shape), 'index': [str(x) for x in df.index.names],
                  'columns': [str(x) for x in df.columns], 'secs': round(time.time() - t0, 1),
                  'warning_classes': sorted({x.category.__name__ for x in w})}
    print(t, df.shape, list(df.index.names), f'{time.time()-t0:.0f}s', flush=True)
reps = {}
for mod, fn in [('lsms_library.null_index_audit', 'null_index_reports'),
                ('lsms_library.quantity_audit', 'quantity_reports'),
                ('lsms_library.null_read_audit', 'null_read_reports'),
                ('lsms_library.country', 'grain_reports')]:
    try:
        import importlib
        f = getattr(importlib.import_module(mod), fn)
        reps[fn] = [str(r) for r in f(country='Mali')]
    except Exception as e:
        reps[fn] = f'ERR {e}'
json.dump(reps, open(f'{out}/reports.json', 'w'), indent=1, default=str)
json.dump(summary, open(f'{out}/summary.json', 'w'), indent=1, default=str)
print('DONE', flush=True)
