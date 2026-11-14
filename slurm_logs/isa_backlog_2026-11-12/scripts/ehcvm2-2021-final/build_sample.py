"""Cold-build `sample` for some countries from one checkout into a private root.

usage: python build_sample.py OUTDIR COUNTRY [COUNTRY ...]
Writes OUTDIR/<C>.parquet (index reset) and OUTDIR/<C>.json (dtypes, attrs summary,
warnings by class).  Asserts the import identity and that the data root is private.
"""
import collections, json, os, sys, time, warnings
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
from lsms_library import paths
root = os.path.realpath(str(paths.data_root()))
for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache', os.path.expanduser('~/.local/share/lsms_library')):
    assert not root.startswith(os.path.realpath(bad)), root
import pandas as pd
import lsms_library as ll

outdir = sys.argv[1]
os.makedirs(outdir, exist_ok=True)
for country in sys.argv[2:]:
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = ll.Country(country).sample()
    flat = df.reset_index()
    for col in flat.columns:
        if flat[col].dtype == object:
            flat[col] = flat[col].astype('string')
    flat.to_parquet(os.path.join(outdir, f'{country}.parquet'))
    derivs = df.attrs.get('derivations', {})
    meta = {
        'country': country, 'checkout': CHECKOUT, 'data_root': root,
        'seconds': round(time.time() - t0, 1), 'rows': len(df),
        'index': list(df.index.names), 'columns': list(df.columns),
        'dtypes': {k: str(v) for k, v in df.reset_index().dtypes.items()},
        'attrs_keys': sorted(df.attrs),
        'derivations': json.loads(json.dumps(derivs, default=str)),
        'warnings': dict(collections.Counter(w.category.__name__ for w in caught)),
        'warning_samples': [str(w.message)[:400] for w in caught
                            if w.category.__name__ not in ('FutureWarning', 'DeprecationWarning')][:12],
    }
    if os.environ.get('CF') == '1':
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cf = ll.Country(country).cluster_features().reset_index()
        for col in cf.columns:
            if cf[col].dtype == object:
                cf[col] = cf[col].astype('string')
        cf.to_parquet(os.path.join(outdir, f'{country}_cluster_features.parquet'))
    json.dump(meta, open(os.path.join(outdir, f'{country}.json'), 'w'), indent=1, default=str)
    print(country, len(df), meta['seconds'], 's', flush=True)
print('DONE', flush=True)
