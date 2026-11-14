"""Cache-hash census for one checkout: every country, every declared table.

Usage: python hash_census.py <out.json>

Adapted from isa-sm-20261112 sold-kg-final (only the private-root
assertion differs).  Records build_transforms_fingerprint(None) and per table,
framework_imports_fingerprint(country _/*.py) per country,
Country._table_cache_hash(table, waves) per (country, table) and
Wave._input_hash(table) per (country, wave, table).  Config / sidecar reads
only; no data is built.
"""
import json
import os
import subprocess
import sys
import warnings

warnings.simplefilter('ignore')
import lsms_library  # noqa: E402

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import paths  # noqa: E402
_r = os.path.realpath(str(paths.data_root())); assert '/isa-sm-20261113/private/' in _r or _r.startswith('/local/job'), _r; assert '/cache/lsms_library' not in _r and '.local/share' not in _r, _r
from lsms_library.country import Country  # noqa: E402
from lsms_library._build_registry import (build_transforms_fingerprint,  # noqa: E402
                                          framework_imports_fingerprint)

out = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
       'btf': {'<None>': build_transforms_fingerprint(None)},
       'fwimp': {}, 'table': {}, 'wave': {}, 'errors': {}}
root = paths.countries_root()
assert os.path.realpath(str(root)).startswith(os.path.realpath(top) + os.sep), root
countries = sorted(p.parent.parent.name for p in root.glob('*/_/data_scheme.yml'))
for name in countries:
    try:
        c = Country(name)
        waves = list(c.waves)
        tables = list(c.data_scheme)
    except Exception as e:
        out['errors'][name] = f'{type(e).__name__}: {e}'
        continue
    cdir = root / name / '_'
    pys = tuple(sorted(str(p) for p in cdir.glob('*.py'))) if cdir.is_dir() else ()
    out['fwimp'][name] = framework_imports_fingerprint(pys)
    for t in tables:
        if t not in out['btf']:
            try:
                out['btf'][t] = build_transforms_fingerprint(t)
            except Exception as e:
                out['btf'][t] = f'ERR {type(e).__name__}'
        try:
            out['table'][f'{name}/{t}'] = c._table_cache_hash(t, waves)
        except Exception as e:
            out['table'][f'{name}/{t}'] = f'ERR {type(e).__name__}'
        for w in waves:
            try:
                out['wave'][f'{name}/{w}/{t}'] = c[w]._input_hash(t)
            except Exception as e:
                out['wave'][f'{name}/{w}/{t}'] = f'ERR {type(e).__name__}'
with open(sys.argv[1], 'w') as fh:
    json.dump(out, fh, indent=1, sort_keys=True)
print('head', out['head'], 'countries', len(countries), 'tables', len(out['table']),
      'waves', len(out['wave']), 'errors', len(out['errors']))
