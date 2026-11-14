"""What a country serves when ONE source file cannot be fetched.

Usage: python scf_outage.py <label> <country> <file-pattern> <spec> [<spec> ...]
  spec: table or table@wave1,wave2
Patches local_tools.get_dataframe to raise FileNotFoundError for paths containing
<file-pattern> only, in a FRESH private root.  Aggregates -> results/outage_<label>.json.
"""
import collections
import json
import os
import sys
import traceback
import warnings

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scf_common import RESULTS, guard  # noqa: E402

label, country, pattern, specs = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
_, dr = guard()
from lsms_library import local_tools as lt  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
import lsms_library as ll  # noqa: E402

_orig = lt.get_dataframe
hits = collections.Counter()


def patched(path, *a, **k):
    if pattern in str(path):
        hits['refused'] += 1
        raise FileNotFoundError(f'simulated unavailable: {os.path.basename(str(path))}')
    return _orig(path, *a, **k)


lt.get_dataframe = patched
out = {'label': label, 'country': country, 'pattern': pattern, 'calls': {}}
for spec in specs:
    table, _, waves = spec.partition('@')
    kw = {'waves': waves.split(',')} if waves else {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(ll.Country(country), table)(**kw)
            t = df.index.get_level_values('t').astype(str)
            r = {'ok': True, 'rows': int(len(df)), 'rows_by_t': {w: int((t == w).sum()) for w in sorted(set(t))}}
        except BaseException as e:  # noqa: BLE001 - report it
            r = {'ok': False, 'error_class': type(e).__name__,
                 'mro': [cl.__name__ for cl in type(e).__mro__][:5],
                 'mentions_file': pattern in str(e) or os.path.basename(pattern) in str(e)}
    r['warnings'] = dict(collections.Counter(type(w.message).__name__ for w in caught))
    out['calls'][spec] = r
    print(spec, json.dumps(r), flush=True)
out['refused_reads'] = int(hits['refused'])
# Is the file's blob in this root's L1?  Find its sidecar md5, then look for the blob.
found = []
for side in countries_root().glob(f'{country}/*/Data/*{pattern}*.dvc'):
    try:
        md5 = yaml.safe_load(open(side))['outs'][0]['md5']
    except Exception:  # noqa: BLE001
        continue
    blobs = [p for p in __import__('pathlib').Path(dr).rglob(md5[2:]) if p.parent.name == md5[:2]]
    found.append({'sidecar': side.name, 'blob_in_private_L1': bool(blobs)})
out['blob_check'] = found
json.dump(out, open(os.path.join(RESULTS, f'outage_{label}.json'), 'w'), indent=1)
print('done outage', label, flush=True)
