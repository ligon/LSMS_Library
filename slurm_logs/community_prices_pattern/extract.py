import sys, os, warnings, time, json, traceback
sys.path.insert(0, os.environ['WT'])
import lsms_library
assert 'wt-gsps-inputs' in lsms_library.__file__, lsms_library.__file__
from lsms_library.paths import countries_root, data_root
assert str(countries_root()).startswith(os.environ['WT']), countries_root()
assert str(data_root()).startswith(os.environ['SCRATCH']), data_root()
import pandas as pd, numpy as np
from lsms_library.country import Country

OUT = os.path.join(os.environ['SCRATCH'], 'extracts')
COUNTRIES = ['Malawi','Nigeria','Ethiopia','EthiopiaRHS','Mali','Niger','Tanzania','GhanaLSS']
only = sys.argv[1:] or COUNTRIES

meta = {}
for c in only:
    m = {}
    C = Country(c)
    for tbl in ('community_prices','sample','food_acquired'):
        t0 = time.time()
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')
                df = getattr(C, tbl)()
            wl = sorted({f"{x.category.__name__}" for x in w})
        except Exception as e:
            m[tbl] = {'error': f"{type(e).__name__}: {e}"[:400]}
            traceback.print_exc()
            continue
        el = time.time()-t0
        m[tbl] = {'shape': list(df.shape), 'index': list(df.index.names),
                  'columns': list(df.columns), 'secs': round(el,1),
                  'warn_classes': wl}
        df.to_parquet(os.path.join(OUT, f"{c}__{tbl}.parquet"))
        print(f"{c:14s} {tbl:18s} {str(df.shape):16s} idx={df.index.names} {el:.1f}s", flush=True)
    meta[c] = m
    with open(os.path.join(OUT,f'meta_{c}.json'),'w') as f: json.dump(m,f,indent=1)
print("DONE")
