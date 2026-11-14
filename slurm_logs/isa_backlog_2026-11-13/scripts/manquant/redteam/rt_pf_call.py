"""Call Country('Senegal').plot_features() and record the outcome.  usage: rt_pf_call.py <out.pkl> <out.json>"""
import json, os, sys, time, traceback, warnings
from collections import Counter
import lsms_library as ll
from lsms_library import paths
r = os.path.realpath(str(paths.data_root())); assert r.startswith('/local/job'), r
rec = {'backend': os.environ.get('LSMS_BUILD_BACKEND'), 'root': r, 'pkg': ll.__file__}
t0 = time.time()
with warnings.catch_warnings(record=True) as wl:
    warnings.simplefilter('always')
    try:
        df = ll.Country('Senegal').plot_features()
        df2 = df.copy(); df2.attrs = {}; df2.to_pickle(sys.argv[1])
        rec['rows'] = len(df); rec['by_t'] = {str(k): int(v) for k, v in df.index.get_level_values('t').value_counts().items()}
    except BaseException as e:
        rec['error'] = f'{type(e).__name__}: {str(e)[:600]}'
        rec['mro'] = [c.__name__ for c in type(e).__mro__]
        rec['tb_tail'] = traceback.format_exc()[-1500:]
rec['sec'] = round(time.time() - t0, 1)
rec['warn'] = Counter(f'{w.category.__name__}: {str(w.message)[:220]}' for w in wl).most_common(15)
json.dump(rec, open(sys.argv[2], 'w'), indent=1, default=str)
print(json.dumps({k: rec.get(k) for k in ('backend', 'rows', 'by_t', 'error', 'sec')}, default=str))
