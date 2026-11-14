"""Cold-build GhanaLSS food_acquired, food_quantities(), food_prices(), nutrition.

usage: python build_ghana.py OUTDIR
Writes OUTDIR/<table>.parquet (index reset), OUTDIR/<table>.attrs.json and
OUTDIR/meta.json.  Asserts import identity and a private data root.
"""
import collections, json, os, re, sys, time, traceback, warnings
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
from lsms_library import paths
root = os.path.realpath(str(paths.data_root()))
for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache', os.path.expanduser('~/.local/share/lsms_library')):
    assert not root.startswith(os.path.realpath(bad)), root
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T

outdir = sys.argv[1]
os.makedirs(outdir, exist_ok=True)
c = ll.Country('GhanaLSS')
meta = {'checkout': CHECKOUT, 'data_root': root, 'waves': list(c.waves),
        'size_words': sorted(T._UNIT_SIZE_WORDS), 'tables': {}}


def jsonable(v):
    try:
        return json.loads(json.dumps(v, default=str))
    except Exception:
        return repr(v)[:4000]


for t in ('food_acquired', 'food_quantities', 'food_prices', 'nutrition'):
    rec = {}
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(c, t)()
            rec.update(rows=len(df), index=list(df.index.names), columns=list(df.columns),
                       dtypes={k: str(v) for k, v in df.reset_index().dtypes.items()})
            flat = df.reset_index()
            for col in flat.columns:
                if flat[col].dtype == object:
                    flat[col] = flat[col].astype('string')
            flat.to_parquet(os.path.join(outdir, f'{t}.parquet'))
            json.dump({k: jsonable(v) for k, v in df.attrs.items()},
                      open(os.path.join(outdir, f'{t}.attrs.json'), 'w'), indent=1, default=str)
            if 'u' in (df.index.names or []):
                u = pd.Series(df.index.get_level_values('u')).astype('string')
                rec['u_distinct'] = int(u.nunique())
                sized = u[u.str.contains(r'\((?:Très|Tres) grand\)', regex=True, na=False)]
                rec['u_tres_grand_rows'] = int(len(sized))
                rec['u_tres_grand_labels'] = sorted(set(sized))
                rec['u_with_any_size_suffix'] = int(u.map(lambda x: T._unit_size_base(x) is not None
                                                          if isinstance(x, str) else False).sum())
                rec['u_containing_grand'] = sorted(set(u[u.str.contains('grand', case=False, na=False)]))[:40]
        except Exception as exc:
            rec['error'] = f'{type(exc).__name__}: {exc}'[:4000]
            rec['traceback'] = traceback.format_exc()[-4000:]
    rec['seconds'] = round(time.time() - t0, 1)
    rec['warnings'] = dict(collections.Counter(w.category.__name__ for w in caught))
    meta['tables'][t] = rec
    print(t, rec.get('rows'), rec['seconds'], rec.get('error', '')[:200], flush=True)
    json.dump(meta, open(os.path.join(outdir, 'meta.json'), 'w'), indent=1, default=str)
# Where nutrition's make subprocess wrote: the private root, never the tree.
meta['private_root_parquets'] = sorted(str(p.relative_to(root)) for p in
                                       __import__('pathlib').Path(root).glob('GhanaLSS/**/nutrition*.parquet'))
json.dump(meta, open(os.path.join(outdir, 'meta.json'), 'w'), indent=1, default=str)
print('DONE', flush=True)
