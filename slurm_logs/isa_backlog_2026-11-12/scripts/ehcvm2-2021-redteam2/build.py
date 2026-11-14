"""Cold-build every table of the given countries in a private root; save frames (job-local).

usage: build.py CLONE SIDE OUTDIR COUNTRY[,COUNTRY...]
Also saves fa_agg.parquet = Country._aggregate_wave_data(None, 'food_acquired'),
the frame the derived food tables are computed from.
"""
import json, os, sys, time, traceback, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env

clone, side, outdir, countries = sys.argv[1:5]
head = assert_env(clone)
import pandas as pd
import lsms_library as ll

EXTRA = [  # (name, method, kwargs)
    ('food_quantities__units', 'food_quantities', {'units': 'units'}),
    ('food_prices__unitvalue', 'food_prices', {'units': 'unitvalue'}),
]
SKIP = {'panel_ids', 'updated_ids'}


def save(df, path):
    df = df.copy()
    attrs = dict(df.attrs)
    df.attrs = {}
    df.to_parquet(path)
    return attrs


for country in countries.split(','):
    c = ll.Country(country)
    d = os.path.join(outdir, side, country)
    os.makedirs(d, exist_ok=True)
    scheme = [t for t in c.data_scheme if t not in SKIP]
    calls = [(t, t, {}) for t in scheme] + [x for x in EXTRA if x[1] in scheme]
    meta = {'head': head, 'country': country, 'waves': list(c.waves), 'scheme': scheme, 'tables': {}}
    for name, method, kw in calls:
        t0 = time.time()
        rec = {'method': method, 'kwargs': kw}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                df = getattr(ll.Country(country), method)(**kw)
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(f'not a DataFrame: {type(df)}')
                attrs = save(df, os.path.join(d, f'{name}.parquet'))
                rec.update(ok=True, rows=len(df), index=list(df.index.names),
                           columns=list(df.columns),
                           dtypes={k: str(v) for k, v in df.dtypes.items()},
                           attrs_keys=sorted(attrs),
                           attrs=json.loads(json.dumps(
                               {k: attrs[k] for k in attrs
                                if k in ('derivations', 'kg_factor_sources', 'kg_factor_baseline_gate',
                                         'kg_factor_size_pool', 'kg_factor_declared_none')},
                               default=str)))
            except Exception as exc:
                rec.update(ok=False, error=f'{type(exc).__name__}: {exc}'[:2000],
                           tb=traceback.format_exc()[-3000:])
        rec['warnings'] = sorted({f'{w.category.__name__}: {str(w.message)[:300]}' for w in caught})[:60]
        rec['seconds'] = round(time.time() - t0, 1)
        meta['tables'][name] = rec
        print(f'{side} {country} {name}: ok={rec["ok"]} rows={rec.get("rows")} '
              f'{rec["seconds"]}s {rec.get("error", "")[:300]}', flush=True)
        with open(os.path.join(d, '_meta.json'), 'w') as fh:
            json.dump(meta, fh, indent=1, default=str)
    if 'food_acquired' in scheme:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                fa = ll.Country(country)._aggregate_wave_data(None, 'food_acquired')
            save(fa, os.path.join(d, 'fa_agg.parquet'))
            print(f'{side} {country} fa_agg rows={len(fa)}', flush=True)
        except Exception as exc:
            print(f'{side} {country} fa_agg FAILED {type(exc).__name__}: {exc}', flush=True)
print('BUILD DONE', side, countries, flush=True)
