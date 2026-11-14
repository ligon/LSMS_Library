"""Call Country('Senegal').plot_features() and report rows by wave, or the exception."""
import json, sys, traceback, warnings, os
from collections import Counter
import lsms_library as ll
from lsms_library import paths
r = os.path.realpath(str(paths.data_root()))
assert r.startswith('/local/job') or '/isa-sm-20261113/private/' in r, r
rec = {'data_root': r, 'backend': os.environ.get('LSMS_BUILD_BACKEND', 'default'), 'file': ll.__file__}
with warnings.catch_warnings(record=True) as wl:
    warnings.simplefilter('always')
    try:
        df = ll.Country('Senegal').plot_features()
        rec['rows_by_wave'] = {str(k): int(v) for k, v in df.index.get_level_values('t').value_counts().items()}
    except BaseException as e:
        rec['error'] = f'{type(e).__name__}: {str(e)[:1500]}'
        rec['tb_tail'] = traceback.format_exc()[-2500:]
rec['warnings'] = Counter(f'{w.category.__name__}: {str(w.message)[:300]}' for w in wl).most_common(15)
print(json.dumps(rec, indent=1, default=str))
