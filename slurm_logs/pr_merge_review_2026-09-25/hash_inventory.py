import json
import sys
from pathlib import Path
import lsms_library as ll
from lsms_library._build_registry import build_transforms_fingerprint

assert Path(ll.__file__).resolve().parent == Path.cwd() / 'lsms_library', ll.__file__
result = {'module': ll.__file__, 'tables': {}, 'fingerprints': {}, 'errors': {}}
names = set()
for name in ll.countries():
    try:
        country = ll.Country(name, preload_panel_ids=False)
        result['tables'][name] = {}
        for table in country.resources['Data Scheme']:
            names.add(table)
            result['tables'][name][table] = country._table_cache_hash(table, country.waves)
        print(name, len(result['tables'][name]), flush=True)
    except Exception as exc:
        result['errors'][name] = f'{type(exc).__name__}: {exc}'
for name in sorted(names):
    result['fingerprints'][name] = build_transforms_fingerprint(name)
Path(sys.argv[1]).write_text(json.dumps(result, indent=2))
print('COMPLETE', sum(len(t) for t in result['tables'].values()), result['errors'], flush=True)
