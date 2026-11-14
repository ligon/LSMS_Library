"""Hash census of one clone: every country/table L2-country hash, every wave input
hash, every build_transforms_fingerprint.  Config/sidecar reads only.

usage: hashes.py <clone> <label> <out_json>
"""
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, dump  # noqa: E402

clone, label, out_json = sys.argv[1:4]
head = assert_env(clone)
import lsms_library as ll  # noqa: E402
from lsms_library._build_registry import build_transforms_fingerprint  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

out = {'head': head, 'label': label, 'country': {}, 'wave': {}, 'btf': {}, 'errors': {}}
tables_seen = set()
for cdir in sorted(countries_root().iterdir()):
    if not (cdir / '_' / 'data_scheme.yml').exists():
        continue
    name = cdir.name
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c = ll.Country(name)
            waves = list(c.waves)
            tables = [t for t in c.data_scheme if t not in ('panel_ids', 'updated_ids')]
    except Exception as e:  # noqa: BLE001
        out['errors'][name] = f'{type(e).__name__}: {str(e)[:200]}'
        continue
    for t in tables:
        tables_seen.add(t)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                out['country'][f'{name}/{t}'] = c._table_cache_hash(t, waves)
        except Exception as e:  # noqa: BLE001
            out['country'][f'{name}/{t}'] = f'ERR {type(e).__name__}'
        for w in waves:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    out['wave'][f'{name}/{w}/{t}'] = c[w]._input_hash(t)
            except Exception as e:  # noqa: BLE001
                out['wave'][f'{name}/{w}/{t}'] = f'ERR {type(e).__name__}'
for t in sorted(tables_seen) + [None]:
    out['btf'][str(t)] = build_transforms_fingerprint(t)
dump(out, out_json)
print('HASHES DONE', label, len(out['country']), len(out['wave']), len(out['btf']), flush=True)
